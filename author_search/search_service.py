"""Author Search Service — Component 6.

Search is backed by an in-memory name index: in GS mode every profiled Google
Scholar author plus unprofiled people from Wikidata and co-author boxes (~290K
names); in S2 mode ~360K prominent Semantic Scholar authors.

Three search modes:
1. Typeahead (typeahead=True): in-memory index, top 10 results.
2. Full search (default): in-memory index, top 50 results.
3. Extended search (scholar=True): index plus a live lookup — Google Scholar's
   "User profiles" block via SerpApi in GS mode, the S2 API in S2 mode.

When no name contains every query token, the index falls back to approximate
matching (misspellings, dropped middle initials); those results carry
``"fuzzy": True``.

The index is compiled once per load (see _CompiledIndex) so a query only
inspects the handful of names that contain its longest token instead of
scanning every name.
"""

import bisect
import gzip
import heapq
import importlib.util
import json
import logging
import re
import threading
import time
import unicodedata
from array import array
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

from author_search.bigquery_client import BigQuerySearchClient
from author_search.cache import SearchCache
from author_search.config import Config

logger = logging.getLogger(__name__)

# In-memory author name index. _author_index is the source of truth; the lookup
# structures built from it live in _compiled (see _get_compiled).
_author_index = []
_compiled = None
_index_lock = threading.Lock()
_index_loaded_at = 0
_index_version = None  # "version" of the Firestore index we loaded (None: unknown)
_INDEX_TTL_SECONDS = 6 * 3600  # Check Firestore for a newer index every 6 hours
_RELOAD_RETRY_SECONDS = 600    # After a failed background reload, retry this much later

# Firestore collection for the persisted index
_INDEX_COLLECTION = Config.INDEX_COLLECTION
# The index is saved as gzip-compressed JSON cut into parts ("pack_<version>_<n>");
# the meta document names the current version and its number of parts.
_META_DOC = "meta"
_PACK_FORMAT = "gzip-json"
_PACK_PART_BYTES = 900_000  # Firestore documents are capped at 1 MiB
_LOAD_WORKERS = 16

# Ranking tier by index source: profiled authors (any other source) 2, then people
# we have not crawled yet (the results page crawls them): Wikidata 1, co-authors 0.
_SOURCE_TIER = {"wikidata": 1, "coauthor": 0}

# Result limits
_TYPEAHEAD_LIMIT = 10
_FULL_SEARCH_LIMIT = 50
_FUZZY_LIMIT = 5
_RESULT_CACHE_SIZE = 2048

# A GS profile URL (".../citations?user=PA9La6oAAAAJ&hl=en") or a bare 12-char ID.
_GS_URL_RE = re.compile(r"[?&]user=([A-Za-z0-9_-]{12})(?:[&#]|$)")
_GS_ID_RE = re.compile(r"^[A-Za-z0-9_-]{12}$")


def parse_gs_profile_id(text):
    """Return the GS user ID in a pasted profile URL or bare ID, else None.

    A bare ID must contain a digit, "_"/"-", or an uppercase letter after the
    first character, so a capitalised 12-letter name ("Christianson") is
    still treated as a name search.
    """
    text = text.strip()
    m = _GS_URL_RE.search(text)
    if m:
        return m.group(1)
    if _GS_ID_RE.match(text) and any(c.isdigit() or c.isupper() or c in "_-" for c in text[1:]):
        return text
    return None


def _gs_id_result(gs_id):
    """Search result for a pasted GS profile: the indexed author if known, else a stub
    (the results page fetches unknown profiles on demand)."""
    index = _get_compiled()
    if index is not None:
        entry = index.entry_by_id(gs_id)
        if entry is not None:
            return dict(entry, source="gs_id")
    return {"scholar_id": gs_id, "name": f"Google Scholar profile {gs_id}", "affiliation": "",
            "citedby": None, "hindex": None, "source": "gs_id"}


def _load_legacy_chunks(cache):
    """Read an index saved before 2026-11-16: documents chunk_0, chunk_1, ... of 2,000 entries each.

    Chunks are requested a batch at a time, in parallel, until one is missing.
    """
    def read(i):
        data = cache.get_index_chunk(_INDEX_COLLECTION, f"chunk_{i}")
        return data if isinstance(data, list) and data else None

    authors = []
    with ThreadPoolExecutor(max_workers=_LOAD_WORKERS, thread_name_prefix="author-index-load") as pool:
        start = 0
        while True:
            complete = True
            for data in pool.map(read, range(start, start + _LOAD_WORKERS)):
                if data is None:
                    complete = False
                    break
                authors.extend(data)
            if not complete:
                break
            start += _LOAD_WORKERS
    return authors


def _read_index_meta(cache):
    """The packed index's meta document, or None if the saved index is in the legacy format."""
    meta = cache.get_index_chunk(_INDEX_COLLECTION, _META_DOC)
    if (isinstance(meta, dict) and meta.get("format") == _PACK_FORMAT
            and isinstance(meta.get("parts"), int) and meta["parts"] > 0 and meta.get("version")):
        return meta
    return None


def _part_id(version, n):
    return f"pack_{version}_{n}"


def _load_packed(cache, meta):
    """Read the packed index `meta` describes. Returns [] if a part is missing or damaged."""
    def read(n):
        return cache.get_index_chunk(_INDEX_COLLECTION, _part_id(meta["version"], n))

    with ThreadPoolExecutor(max_workers=_LOAD_WORKERS, thread_name_prefix="author-index-load") as pool:
        parts = list(pool.map(read, range(meta["parts"])))
    if not all(isinstance(part, bytes) and part for part in parts):
        logger.error("Author index %s: a part is missing", meta["version"])
        return []
    try:
        authors = json.loads(gzip.decompress(b"".join(parts)))
    except (OSError, EOFError, ValueError):
        logger.exception("Author index %s: could not unpack", meta["version"])
        return []
    return authors if isinstance(authors, list) else []


def _load_index(cache):
    """(authors, version) of the index saved in Firestore; ([], None) if there is none."""
    meta = _read_index_meta(cache)
    if meta:
        authors = _load_packed(cache, meta)
        if authors:
            return authors, meta["version"]
    authors = _load_legacy_chunks(cache)
    for a in authors:
        a.pop("name_lower", None)  # derived from the name; the compiled index keeps its own copy
    return authors, None


def _load_index_from_firestore(cache):
    """Load the author name index from Firestore."""
    return _load_index(cache)[0]


def _delete_docs(cache, doc_id_for, start=0):
    """Delete doc_id_for(start), doc_id_for(start + 1), ... until one is missing or a delete fails."""
    n = start
    while cache.get_index_chunk(_INDEX_COLLECTION, doc_id_for(n)):
        if cache.delete_index_chunk(_INDEX_COLLECTION, doc_id_for(n)) is not True:
            break
        n += 1


def _save_index_to_firestore(cache, authors):
    """Save the author name index to Firestore; returns its version.

    The list is stored as gzip-compressed JSON cut into parts below Firestore's
    1 MiB document limit (~10 documents for 290K authors), which a reader
    fetches and unpacks in about a second. The parts are written under a new
    version, then the meta document is switched to it, so a reader sees either
    the old index or the new one, never a mix. The previous version's parts and
    any legacy chunk_N documents are deleted afterwards.
    """
    entries = [{k: v for k, v in a.items() if k != "name_lower" and v is not None} for a in authors]
    payload = gzip.compress(
        json.dumps(entries, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8"))
    parts = [payload[i:i + _PACK_PART_BYTES] for i in range(0, len(payload), _PACK_PART_BYTES)]
    version = f"{int(time.time() * 1000)}"
    previous = _read_index_meta(cache)

    for n, part in enumerate(parts):
        cache.set_index_chunk(_INDEX_COLLECTION, _part_id(version, n), part)
    meta = {"format": _PACK_FORMAT, "version": version, "parts": len(parts), "count": len(authors)}
    if not _load_packed(cache, meta):
        # A part did not get written: leave the previous index in place.
        logger.error("Author index %s could not be read back; keeping the previous one", version)
        return None
    cache.set_index_chunk(_INDEX_COLLECTION, _META_DOC, meta)

    if previous and previous["version"] != version:
        _delete_docs(cache, lambda n: _part_id(previous["version"], n))
    _delete_docs(cache, lambda n: f"chunk_{n}")
    logger.info("Saved %d authors in %d parts (%.1f MB), version %s",
                len(authors), len(parts), len(payload) / 1e6, version)
    return version


def _set_index(authors, version=None, precompile=True, fuzzy=False):
    """Make `authors` the live index (each global is swapped in one assignment).

    fuzzy=True also builds the approximate-matching vocabulary before the swap;
    use it when an older index keeps serving searches in the meantime.
    """
    global _author_index, _compiled, _index_loaded_at, _index_version
    compiled = None
    if precompile:
        compiled = _CompiledIndex(authors)
        if fuzzy:
            compiled.prepare_fuzzy()
    _compiled = compiled
    _author_index = authors
    _index_version = version
    _index_loaded_at = time.time()


def refresh_author_index(bq=None, cache=None):
    """Rebuild the in-memory author name index from BigQuery.

    Fetches the authors, saves them to Firestore for persistence across
    restarts, and loads them into memory for instant search.
    Returns the number of authors indexed.
    """
    bq = bq or BigQuerySearchClient()
    cache = cache or SearchCache()

    logger.info("Refreshing author name index from BigQuery...")
    authors = bq.get_all_author_names()
    if not authors:
        logger.warning("No authors returned from BigQuery for name index")
        return 0

    version = _save_index_to_firestore(cache, authors)

    # Compiled on the first search: the crawler job calls this only to publish the index.
    _set_index(authors, version=version, precompile=False)
    if version is None:
        # Searchable in this process, but not published for the other instances.
        return 0

    logger.info("Author name index refreshed: %d authors", len(authors))
    return len(authors)


_bootstrap_triggered = False
_reload_running = False
_reload_lock = threading.Lock()


def _ensure_index_loaded(cache, bq=None):
    """Load the index from Firestore if it is not in memory, or refresh it if stale.

    A missing index is loaded here, with concurrent callers waiting on the lock.
    A stale one keeps serving searches while a background thread checks Firestore
    for a newer version. If no index exists in Firestore (fresh deploy), a
    background rebuild from BigQuery is triggered so search recovers automatically.
    """
    global _bootstrap_triggered

    if _author_index:
        if (time.time() - _index_loaded_at) >= _INDEX_TTL_SECONDS:
            _reload_index_async(cache)
        return

    with _index_lock:
        if _author_index:
            return

        authors, version = _load_index(cache)
        if authors:
            _set_index(authors, version=version)
            _bootstrap_triggered = False
            logger.info("Loaded %d authors from Firestore index", len(authors))
        elif not _bootstrap_triggered:
            _bootstrap_triggered = True
            logger.warning("No author index in Firestore; triggering background rebuild")
            _bootstrap_index_async(bq, cache)


def _reload_index_async(cache):
    """Refresh a stale in-memory index in a background thread (one at a time).

    Skips the download when the saved index's version is the one already loaded.
    On failure the current index stays and the check is retried later.
    """
    global _reload_running
    with _reload_lock:
        if _reload_running:
            return
        _reload_running = True

    def _reload():
        global _reload_running, _index_loaded_at
        try:
            meta = _read_index_meta(cache)
            if meta and meta["version"] == _index_version:
                _index_loaded_at = time.time()
                return
            authors, version = _load_index(cache)
            if authors:
                _set_index(authors, version=version, fuzzy=True)
                logger.info("Reloaded %d authors from Firestore index", len(authors))
            else:
                logger.warning("Author index reload returned nothing; keeping the current index")
                _index_loaded_at = time.time() - _INDEX_TTL_SECONDS + _RELOAD_RETRY_SECONDS
        except Exception:
            logger.exception("Author index reload failed; keeping the current index")
            _index_loaded_at = time.time() - _INDEX_TTL_SECONDS + _RELOAD_RETRY_SECONDS
        finally:
            _reload_running = False

    threading.Thread(target=_reload, daemon=True, name="author-index-reload").start()


def warm_index_async(cache, bq=None):
    """Start loading the index in the background (called once at app start-up).

    Without this the first search on a new instance waits for the whole download.
    """
    def _warm():
        try:
            _ensure_index_loaded(cache, bq=bq)
            # Searches are being served by now; get approximate matching ready too.
            index = _get_compiled()
            if index is not None:
                index.prepare_fuzzy()
        except Exception:
            logger.exception("Author index warm-up failed")

    threading.Thread(target=_warm, daemon=True, name="author-index-warm").start()


def _bootstrap_index_async(bq, cache):
    """Rebuild the author index in a background thread.

    Called once when the index is missing from Firestore (fresh deploy).
    Resets the _bootstrap_triggered guard on failure so retries can occur.
    """
    def _rebuild():
        global _bootstrap_triggered
        try:
            count = refresh_author_index(bq=bq, cache=cache)
            if count > 0:
                logger.info("Background index bootstrap complete: %d authors", count)
            else:
                logger.warning("Background index bootstrap returned 0 authors, resetting guard")
                _bootstrap_triggered = False
        except Exception:
            logger.exception("Background index bootstrap failed, resetting guard")
            _bootstrap_triggered = False

    t = threading.Thread(target=_rebuild, daemon=True, name="author-index-bootstrap")
    t.start()


def _is_initial(word):
    """Check if a word is a name initial (e.g., "t.", "t", "j")."""
    return len(word) <= 2 and (len(word) == 1 or word[1] == '.')


# Common name suffixes that should not count as boundary words.
_NAME_SUFFIXES = frozenset({
    "jr", "jr.", "sr", "sr.", "ii", "ii.", "iii", "iii.", "iv", "iv.", "v", "v.",
    "phd", "ph.d.", "md", "m.d.", "esq", "esq.",
})


def _token_matches_name(token, token_idx, name, name_words):
    """Check if a query token matches an author name.

    Returns a tuple (matches, is_substring) where:
    - matches: whether the token matches at all
    - is_substring: True if matched via direct substring (strong match),
      False if matched only via initial expansion (weak match)

    Initial expansion (e.g., "tyler" matching "t.") is restricted to the
    same position in the token list and name word list, so "Tyler Cowen"
    matches "T. Cowen" but not "Steven T. Cowen".
    """
    # Direct substring match (most common case)
    if token in name:
        return True, True

    # Position-aligned initial-to-full-name matching:
    if token_idx < len(name_words):
        word = name_words[token_idx]
        first_char = token[0]
        # Name has an initial at this position, query token starts with same letter
        if _is_initial(word) and word[0] == first_char:
            return True, False
        # Query token is an initial, name word at this position starts with same letter
        if _is_initial(token) and word[0] == first_char:
            return True, False

    return False, False


def _entry_matches(tokens, name):
    """Whether every query token matches `name` (both lowercased and accent-folded).

    Supports matching full names against initials (e.g., "Tyler" matches "T."
    in names), but requires at least one token to match via direct substring
    to avoid false positives (e.g., "Tyler Cowen" should not match
    "T. C. Smith").
    """
    name_words = name.split()
    any_substring = False
    any_initial = False
    for token_idx, token in enumerate(tokens):
        matched, is_substring = _token_matches_name(token, token_idx, name, name_words)
        if not matched:
            return False
        if is_substring:
            any_substring = True
        else:
            any_initial = True
    if not any_substring:
        return False
    # When initial expansion was used, verify the first and last
    # non-initial name words are covered by a query token (via
    # substring or position-aligned initial).  Middle names are
    # allowed to be unmatched so "J. Smith" finds "John Adam Smith",
    # but the surname must match to prevent "Tyler Cowen" from
    # matching "Tyler C. Sloan".
    if any_initial:
        non_initials = [(i, w.rstrip(".,;:")) for i, w in enumerate(name_words)
                        if not _is_initial(w)
                        and w.rstrip(".,;:") not in _NAME_SUFFIXES]
        if non_initials:
            # First non-initial is a boundary only when it's the
            # actual first word (position 0).  If the name starts
            # with an initial (e.g., "T. Adam Cowen"), the first
            # non-initial is a middle name and can be skipped.
            boundary_words = set()
            if non_initials[0][0] == 0:
                boundary_words.add(non_initials[0])
            boundary_words.add(non_initials[-1])
            for w_idx, w in boundary_words:
                # Covered by whole-word match from a non-initial token?
                # Whole-word prevents partial hits like "li" covering
                # "williams".  Initial tokens are excluded because
                # single letters match too many words.
                if any(t == w for t in tokens if not _is_initial(t)):
                    continue
                # Covered by a position-aligned initial query token?
                if (w_idx < len(tokens)
                        and _is_initial(tokens[w_idx])
                        and tokens[w_idx][0] == w[0]):
                    continue
                return False
    return True


# Letters NFKD leaves alone but people type without the stroke or ligature.
_FOLD_EXTRA = str.maketrans({"ø": "o", "ł": "l", "đ": "d", "ð": "d", "ı": "i", "ħ": "h",
                             "ß": "ss", "æ": "ae", "œ": "oe", "þ": "th"})
_WORD_STRIP = ".,;:\"'()[]"


def _fold(text):
    """Lowercase and strip accents, so "Gulsun Tunay" finds "Gülsün Tünay"."""
    text = text.lower()
    if text.isascii():
        return text
    text = unicodedata.normalize("NFKD", text)
    return "".join(c for c in text if not unicodedata.combining(c)).translate(_FOLD_EXTRA)


def _max_edits(token):
    """Typos tolerated in a query word: none up to 3 letters, one up to 5, two beyond."""
    n = len(token)
    return 0 if n < 4 else 1 if n < 6 else 2


def _rank_key(author):
    # Profiled authors first (by citations, even when they have none), then the
    # unprofiled ones: better-known on Wikidata (more Wikipedia editions) first,
    # then by how many profiled authors list them as a co-author.
    return (_SOURCE_TIER.get(author.get("source"), 2), author.get("citedby") or 0,
            author.get("sitelinks") or 0, author.get("coauthor_of") or 0)


class _CompiledIndex:
    """Lookup structures over one version of the author list.

    - `blob`: every folded name joined by newlines, so `str.find` locates the
      names containing a token without a Python-level loop over all of them;
      `starts[i]` is where entry i's name begins.
    - `initials_at[(position, letter)]`: entries whose name has an initial at
      that word position ("T. Cowen" is under (0, "t")), the only names a full
      query word can match without appearing in them.
    - `rank[i]`: position of entry i in the result order (0 = shown first).
    - a word vocabulary for approximate matching, built on first use.
    """

    def __init__(self, entries):
        self.entries = entries
        names = [" ".join(_fold(a.get("name_lower") or (a.get("name") or "").lower()).split())
                 for a in entries]
        starts = array("l")
        pos = 0
        initials_at = {}
        for i, name in enumerate(names):
            starts.append(pos)
            pos += len(name) + 1
            for p, word in enumerate(name.split()):
                if _is_initial(word):
                    initials_at.setdefault((p, word[0]), []).append(i)
        starts.append(pos)
        self.blob = "\n".join(names)
        self.starts = starts
        self.initials_at = initials_at
        self._by_id = None
        # sorted() is stable, also with reverse=True: ties keep index order.
        order = sorted(range(len(entries)), key=lambda i: _rank_key(entries[i]), reverse=True)
        rank = array("l", [0]) * len(entries)
        for position, i in enumerate(order):
            rank[i] = position
        self.rank = rank
        self._vocab = None
        self._vocab_words = None
        self._truncated = OrderedDict()
        self._results = OrderedDict()
        self._lock = threading.Lock()

    def name(self, i):
        return self.blob[self.starts[i]:self.starts[i + 1] - 1]

    def entry_by_id(self, scholar_id):
        """The entry with this ID, or None. The lookup table is built on first use."""
        if self._by_id is None:
            self._by_id = {a.get("scholar_id"): a for a in self.entries if a.get("scholar_id")}
        return self._by_id.get(scholar_id)

    def containing(self, token):
        """Entry numbers, ascending, of the names that contain `token`."""
        blob, starts = self.blob, self.starts
        out = []
        pos = blob.find(token)
        while pos != -1:
            i = bisect.bisect_right(starts, pos) - 1
            out.append(i)
            pos = blob.find(token, starts[i + 1])
        return out

    def exact(self, tokens):
        """Entry numbers whose name matches every token (see _entry_matches)."""
        anchors = [(len(t), idx) for idx, t in enumerate(tokens) if not _is_initial(t)]
        if not anchors:
            # Only initials ("j. k."): no selective token, check every name.
            candidates = range(len(self.entries))
            return [i for i in candidates if _entry_matches(tokens, self.name(i))]
        # A full word matches a name either as a substring or against an initial at
        # the same word position, so these two sets hold every possible match.
        _, anchor_idx = max(anchors)
        anchor = tokens[anchor_idx]
        direct = self.containing(anchor)
        others = [t for idx, t in enumerate(tokens) if idx != anchor_idx]
        matches = []
        for i in direct:
            name = self.name(i)
            # Every token a substring: a match with no initial expansion to verify.
            if all(t in name for t in others) or _entry_matches(tokens, name):
                matches.append(i)
        via_initial = self.initials_at.get((anchor_idx, anchor[0]))
        if via_initial:
            seen = set(direct)
            matches.extend(i for i in via_initial
                           if i not in seen and _entry_matches(tokens, self.name(i)))
        return matches

    def prepare_fuzzy(self):
        """Build the word vocabulary and load rapidfuzz ahead of the first approximate search."""
        if importlib.util.find_spec("rapidfuzz") is not None:
            self._build_vocab()

    def _build_vocab(self):
        with self._lock:
            if self._vocab is not None:
                return
            vocab = {}
            for i in range(len(self.entries)):
                for word in self.name(i).split():
                    word = word.strip(_WORD_STRIP)
                    parts = [word] + (word.split("-") if "-" in word else [])
                    for part in parts:
                        if len(part) >= 3:
                            ids = vocab.get(part)
                            if ids is None:
                                vocab[part] = [i]
                            elif ids[-1] != i:
                                ids.append(i)
            self._vocab_words = list(vocab)
            self._vocab = vocab

    def _truncated_words(self, length):
        """The vocabulary cut to `length` letters, to compare a half-typed word against."""
        with self._lock:
            words = self._truncated.get(length)
            if words is None:
                words = [w[:length] for w in self._vocab_words]
                self._truncated[length] = words
                while len(self._truncated) > 4:
                    self._truncated.popitem(last=False)
            else:
                self._truncated.move_to_end(length)
            return words

    def fuzzy(self, tokens):
        """Approximate matches as [(edits, entry number)], best first.

        Initials in the query are ignored ("Robert F. Engle" finds "Robert Engle").
        Every other query word must be in the name, or within _max_edits of one of
        its words. The last word may also be a misspelled beginning of a name word,
        since typeahead sends half-typed names.
        """
        try:
            from rapidfuzz import process
            from rapidfuzz.distance import OSA
        except ImportError:
            logger.warning("rapidfuzz is not installed; approximate name matching is off")
            return []
        words = [t.strip(_WORD_STRIP) for t in tokens if not _is_initial(t)]
        words = [w for w in words if w]
        if not words or max(len(w) for w in words) < 3:
            return []
        if self._vocab is None:
            self._build_vocab()
        vocab, vocab_words = self._vocab, self._vocab_words

        totals = None
        for n, word in enumerate(words):
            edits = dict.fromkeys(self.containing(word), 0)
            k = _max_edits(word)
            if k:
                choice_lists = [vocab_words]
                if n == len(words) - 1:
                    choice_lists.append(self._truncated_words(len(word)))
                for choices in choice_lists:
                    for _, distance, j in process.extract(word, choices, scorer=OSA.distance,
                                                          score_cutoff=k, limit=None):
                        for i in vocab[vocab_words[j]]:
                            if distance < edits.get(i, 99):
                                edits[i] = distance
            if totals is None:
                totals = edits
            else:
                totals = {i: d + edits[i] for i, d in totals.items() if i in edits}
            if not totals:
                return []
        rank = self.rank
        return heapq.nsmallest(_FUZZY_LIMIT, ((d, i) for i, d in totals.items()),
                               key=lambda pair: (pair[0], rank[pair[1]]))

    def search(self, query, limit):
        tokens = [t for t in (t.strip(",;") for t in _fold(query).split()) if t]
        if not tokens:
            return []
        key = (" ".join(tokens), limit)
        with self._lock:
            cached = self._results.get(key)
            if cached is not None:
                self._results.move_to_end(key)
        if cached is None:
            rank = self.rank
            matches = heapq.nsmallest(limit, self.exact(tokens), key=rank.__getitem__)
            cached = [(i, False) for i in matches]
            if not cached:
                cached = [(i, True) for _, i in self.fuzzy(tokens)[:limit]]
            with self._lock:
                self._results[key] = cached
                while len(self._results) > _RESULT_CACHE_SIZE:
                    self._results.popitem(last=False)

        results = []
        for i, fuzzy in cached:
            a = self.entries[i]
            result = {
                "scholar_id": a.get("scholar_id", ""),
                "name": a.get("name", ""),
                "affiliation": a.get("affiliation", ""),
                "email_domain": "",
                "citedby": a.get("citedby", 0),
                "hindex": a.get("hindex", 0),
                "source": a.get("source") or "index",
            }
            if fuzzy:
                result["fuzzy"] = True
            results.append(result)
        return results


def _get_compiled():
    """The compiled form of the current _author_index (built here if a caller set the list directly)."""
    global _compiled
    authors = _author_index
    if not authors:
        return None
    compiled = _compiled
    if compiled is None or compiled.entries is not authors:
        with _index_lock:
            compiled = _compiled
            if compiled is None or compiled.entries is not authors:
                compiled = _CompiledIndex(authors)
                _compiled = compiled
    return compiled


def _search_in_memory(query, limit=_TYPEAHEAD_LIMIT):
    """Search the in-memory index.

    Returns the authors whose name matches every query token (see
    _entry_matches), best-ranked first (see _rank_key). If there are none,
    returns approximate matches instead, each marked ``"fuzzy": True``.
    Returns None if the index is not loaded.
    """
    index = _get_compiled()
    if index is None:
        return None
    return index.search(query, limit)


# A query the live lookup is worth a paid search for: a few words with letters, not a URL.
_NAME_QUERY_RE = re.compile(r"^[^\W\d_][\w .,'’-]{2,79}$")


def _looks_like_name(text):
    return bool(_NAME_QUERY_RE.match(text)) and len(text.split()) <= 6


class AuthorSearchService:
    def __init__(self, bq_client=None, cache=None):
        self.bq = bq_client or BigQuerySearchClient()
        self.cache = cache or SearchCache()

    def warm(self):
        """Start loading the name index in the background (call once at start-up)."""
        warm_index_async(self.cache, bq=self.bq)

    def search(self, author_name, typeahead=False, scholar=False, allow_live=True):
        """Search for authors by name.

        Modes:
            typeahead=True:  In-memory index only, top 10.
            Default:         In-memory index, top 50.
            scholar=True:    In-memory index plus a live lookup: Google Scholar
                             profiles (GS mode) or the S2 API (S2 mode). Each
                             lookup is a paid request, so callers pass
                             allow_live=False for traffic that looks automated.

        Index results that only approximately match (misspellings) carry
        "fuzzy": True; they are returned only when nothing matches exactly.

        Returns a list of author dicts with keys:
            scholar_id, name, affiliation, email_domain, citedby, hindex, source
        """
        if not author_name or len(author_name.strip()) < 2:
            return []

        name = author_name.strip()

        # Ensure the in-memory index is loaded (pass bq for bootstrap)
        _ensure_index_loaded(self.cache, bq=self.bq)

        if Config.IS_GS:
            gs_id = parse_gs_profile_id(name)
            if gs_id:
                return [_gs_id_result(gs_id)]

        if typeahead:
            return _search_in_memory(name, limit=_TYPEAHEAD_LIMIT) or []

        if Config.IS_GS:
            return self._search_gs(name, live=scholar and allow_live)

        # Use separate cache keys for local vs scholar searches
        cache_key = f"s2:{name}" if scholar else name

        cached_results = self.cache.get_search_results(cache_key)
        if cached_results is not None:
            logger.info("Search '%s': cache hit (%d results, scholar=%s)", name, len(cached_results), scholar)
            return cached_results

        results = _search_in_memory(name, limit=_FULL_SEARCH_LIMIT)

        # Don't cache when the index isn't loaded yet
        if results is None:
            logger.warning("Search '%s': index not loaded, returning empty", name)
            return []

        # Supplement with S2 API when we got few local results.
        # Always fall back when 0 results; also fall back with scholar=True
        # when < 5 results, giving users broader coverage on explicit request.
        if len(results) == 0 or (scholar and len(results) < 5):
            if Config.S2_SEARCH_FALLBACK:
                results = self._supplement_with_s2(name, results)

        logger.info("Search '%s': %d results (scholar=%s)", name, len(results), scholar)
        # Don't cache empty results — a transient S2 API failure should
        # not suppress retries for the full cache TTL (24h).
        if results:
            self.cache.set_search_results(cache_key, results)
        return results

    def _search_gs(self, name, live):
        """Full search in GS mode: the index, plus live Google Scholar profiles when asked.

        Index results are not cached in Firestore: computing them takes a few
        milliseconds, less than the cache read.
        """
        results = _search_in_memory(name, limit=_FULL_SEARCH_LIMIT)
        if results is None:
            logger.warning("Search '%s': index not loaded, returning empty", name)
            return []
        if live and Config.GS_LIVE_SEARCH and _looks_like_name(name):
            results = self._supplement_with_gs(name, results)
        logger.info("Search '%s': %d results (gs index, live=%s)", name, len(results), live)
        return results

    def _supplement_with_gs(self, name, local_results):
        """Look the name up on Google Scholar and put the profiles found ahead of the index results.

        Lookups are cached (also when Google has no profile under the name) and
        capped per day, since each one is a paid SerpApi search.
        """
        from author_search import gs_search_client

        cache_key = f"gs:{name}"
        found = self.cache.get(cache_key, ttl_hours=Config.GS_LIVE_CACHE_HOURS)
        if found is None:
            if not self.cache.reserve_live_search(Config.GS_LIVE_SEARCH_DAILY_LIMIT):
                logger.warning("Live Google Scholar search for '%s' skipped: daily limit reached", name)
                return local_results
            found = gs_search_client.search_profiles(name)
            if found is None:  # the lookup failed; do not cache
                return local_results
            self.cache.set(cache_key, found)

        index = _get_compiled()
        results, seen = [], set()
        for author in found:
            sid = author.get("scholar_id")
            if not sid or sid in seen:
                continue
            seen.add(sid)
            known = index.entry_by_id(sid) if index is not None else None
            if known and known.get("source") not in _SOURCE_TIER:
                # Already profiled: show our own numbers.
                author = dict(author, name=known.get("name") or author.get("name"),
                              citedby=known.get("citedby"), hindex=known.get("hindex"))
            results.append(dict(author, source="gs_live"))
        results.extend(r for r in local_results if r.get("scholar_id") not in seen)
        return results

    def _supplement_with_s2(self, name, local_results):
        """Query S2 API and merge with local results."""
        from author_search import s2_client

        # Check S2 cache first
        cached = self.cache.get(name)
        if cached is not None:
            s2_results = cached
            source_tag = "s2_api_cached"
        else:
            s2_results = s2_client.search_authors(name)
            if s2_results:
                self.cache.set(name, s2_results)
            source_tag = "s2_api"

        if not s2_results:
            return local_results

        seen_ids = {r.get("scholar_id") for r in local_results if r.get("scholar_id")}
        results = list(local_results)

        for author in s2_results:
            sid = author.get("scholar_id")
            if sid and sid not in seen_ids:
                seen_ids.add(sid)
                author["source"] = source_tag
                results.append(author)

        return results
