"""Author Search Service — Component 6.

Primary search is backed by an in-memory index loaded from the
daily-materialized ranked_author_current_table in BigQuery. The index
covers ~360K prominent S2 authors (hindex >= 20, citedby > 5000). For
less-known researchers, the Semantic Scholar API provides fallback
coverage of the full 102M author universe.

Three search modes:
1. Typeahead (typeahead=True): In-memory index, top 10 results (instant).
2. Full search (default): In-memory index, top 50 results.
3. Extended search (scholar=True): In-memory + S2 API fallback.
   Results cached in Firestore so repeat queries are even faster.
"""

import logging
import threading
import time

from author_search.bigquery_client import BigQuerySearchClient
from author_search.cache import SearchCache

logger = logging.getLogger(__name__)

# In-memory author name index for instant substring matching
_author_index = []
_index_lock = threading.Lock()
_index_loaded_at = 0
_INDEX_TTL_SECONDS = 6 * 3600  # Refresh from Firestore every 6 hours

# Firestore collection for the persisted index (chunked)
_INDEX_COLLECTION = "v3_author_name_index"
_CHUNK_SIZE = 4000

# Result limits
_TYPEAHEAD_LIMIT = 10
_FULL_SEARCH_LIMIT = 50


def _load_index_from_firestore(cache):
    """Load the author name index from Firestore chunks."""
    authors = []
    chunk_num = 0
    while True:
        doc_id = f"chunk_{chunk_num}"
        data = cache.get_index_chunk(_INDEX_COLLECTION, doc_id)
        if not data:
            break
        authors.extend(data)
        chunk_num += 1
    return authors


def _save_index_to_firestore(cache, authors):
    """Save the author name index to Firestore in chunks."""
    for i in range(0, len(authors), _CHUNK_SIZE):
        chunk = authors[i:i + _CHUNK_SIZE]
        doc_id = f"chunk_{i // _CHUNK_SIZE}"
        cache.set_index_chunk(_INDEX_COLLECTION, doc_id, chunk)
    logger.info("Saved %d authors in %d chunks", len(authors),
                (len(authors) + _CHUNK_SIZE - 1) // _CHUNK_SIZE)


def refresh_author_index(bq=None, cache=None):
    """Rebuild the in-memory author name index from BigQuery.

    Fetches all active S2 authors, saves to Firestore for persistence
    across restarts, and loads into memory for instant search.
    Returns the number of authors indexed.
    """
    global _author_index, _index_loaded_at
    bq = bq or BigQuerySearchClient()
    cache = cache or SearchCache()

    logger.info("Refreshing author name index from BigQuery...")
    authors = bq.get_all_author_names()
    if not authors:
        logger.warning("No authors returned from BigQuery for name index")
        return 0

    for a in authors:
        a["name_lower"] = (a.get("name") or "").lower()

    _save_index_to_firestore(cache, authors)

    with _index_lock:
        _author_index = authors
        _index_loaded_at = time.time()

    logger.info("Author name index refreshed: %d authors", len(authors))
    return len(authors)


_bootstrap_triggered = False


def _ensure_index_loaded(cache, bq=None):
    """Lazily load the index from Firestore if not yet in memory or stale.

    If no index exists in Firestore (fresh deploy), triggers a background
    rebuild from BigQuery so search recovers automatically.
    """
    global _author_index, _index_loaded_at, _bootstrap_triggered

    if _author_index and (time.time() - _index_loaded_at) < _INDEX_TTL_SECONDS:
        return

    with _index_lock:
        if _author_index and (time.time() - _index_loaded_at) < _INDEX_TTL_SECONDS:
            return

        authors = _load_index_from_firestore(cache)
        if authors:
            for a in authors:
                if "name_lower" not in a:
                    a["name_lower"] = (a.get("name") or "").lower()
            _author_index = authors
            _index_loaded_at = time.time()
            _bootstrap_triggered = False
            logger.info("Loaded %d authors from Firestore index", len(authors))
        elif not _bootstrap_triggered:
            _bootstrap_triggered = True
            logger.warning("No author index in Firestore; triggering background rebuild")
            _bootstrap_index_async(bq, cache)


def _bootstrap_index_async(bq, cache):
    """Rebuild the author index in a background thread.

    Called once when the index is missing from Firestore (fresh deploy).
    Resets the _bootstrap_triggered guard on failure so retries can occur.
    """
    global _bootstrap_triggered
    import threading

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


def _search_in_memory(query, limit=_TYPEAHEAD_LIMIT):
    """Search the in-memory index by substring matching.

    All query tokens must appear in the author name. Supports matching
    full names against initials (e.g., "Tyler" matches "T." in names),
    but requires at least one token to match via direct substring to
    avoid false positives (e.g., "Tyler Cowen" should not match
    "T. C. Smith").
    Returns matching authors sorted by citation count (descending),
    or None if the index is not loaded.
    """
    if not _author_index:
        return None

    query_lower = query.lower().strip()
    tokens = query_lower.split()

    matches = []
    for author in _author_index:
        name = author.get("name_lower", "")
        name_words = name.split()
        all_match = True
        any_substring = False
        any_initial = False
        for token_idx, token in enumerate(tokens):
            matched, is_substring = _token_matches_name(token, token_idx, name, name_words)
            if not matched:
                all_match = False
                break
            if is_substring:
                any_substring = True
            else:
                any_initial = True
        if not (all_match and any_substring):
            continue
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
                uncovered = False
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
                    uncovered = True
                    break
                if uncovered:
                    continue
        matches.append(author)

    matches.sort(key=lambda a: a.get("citedby") or 0, reverse=True)

    results = []
    for a in matches[:limit]:
        results.append({
            "scholar_id": a.get("scholar_id", ""),
            "name": a.get("name", ""),
            "affiliation": a.get("affiliation", ""),
            "email_domain": "",
            "citedby": a.get("citedby", 0),
            "hindex": a.get("hindex", 0),
            "source": "index",
        })
    return results


class AuthorSearchService:
    def __init__(self, bq_client=None, cache=None):
        self.bq = bq_client or BigQuerySearchClient()
        self.cache = cache or SearchCache()

    def search(self, author_name, typeahead=False, scholar=False):
        """Search for authors by name.

        Modes:
            typeahead=True:  In-memory index only, top 10 (instant).
            Default:         In-memory index, top 50, cached in Firestore.
            scholar=True:    In-memory index + S2 API fallback for broader
                             coverage. Use when "Search beyond" is clicked.

        The in-memory index covers ~3M active authors (hindex >= 10,
        citedby > 500). For less-known researchers, scholar=True queries
        the Semantic Scholar API (102M authors).

        Returns a list of author dicts with keys:
            scholar_id, name, affiliation, email_domain, citedby, hindex, source
        """
        if not author_name or len(author_name.strip()) < 2:
            return []

        name = author_name.strip()

        # Ensure the in-memory index is loaded (pass bq for bootstrap)
        _ensure_index_loaded(self.cache, bq=self.bq)

        if typeahead:
            return _search_in_memory(name, limit=_TYPEAHEAD_LIMIT) or []

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
            results = self._supplement_with_s2(name, results)

        logger.info("Search '%s': %d results (scholar=%s)", name, len(results), scholar)
        # Don't cache empty results — a transient S2 API failure should
        # not suppress retries for the full cache TTL (24h).
        if results:
            self.cache.set_search_results(cache_key, results)
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
