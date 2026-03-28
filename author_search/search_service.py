"""Author Search Service — Component 6.

Three search modes:
1. Typeahead (typeahead=True): In-memory index only — instant, no I/O.
2. Local search (default): In-memory index + BigQuery stats views.
   Results cached in Firestore.
3. Extended search (scholar=True): Also searches the full S2 authors
   universe (102M authors) in BigQuery, merges with local results.

Results from all sources are deduplicated by scholar_id and merged.
Local results are cached so repeat queries skip BigQuery entirely.
"""

import logging
import threading
import time

from author_search.bigquery_client import BigQuerySearchClient
from author_search.cache import SearchCache
from author_search.config import Config

logger = logging.getLogger(__name__)

# In-memory author name index for instant substring matching
_author_index = []
_index_lock = threading.Lock()
_index_loaded_at = 0
_INDEX_TTL_SECONDS = 6 * 3600  # Refresh from Firestore every 6 hours

# Firestore collection for the persisted index (chunked)
_INDEX_COLLECTION = "v3_author_name_index"
_CHUNK_SIZE = 4000


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

    Fetches all author names, saves to Firestore for persistence across
    restarts, and loads into memory for instant search.
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


def _ensure_index_loaded(cache):
    """Lazily load the index from Firestore if not yet in memory or stale."""
    global _author_index, _index_loaded_at

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
            logger.info("Loaded %d authors from Firestore index", len(authors))
        else:
            logger.warning("No author index in Firestore; call refresh_author_index first")


def _search_in_memory(query, limit=10):
    """Search the in-memory index by substring matching.

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
        if all(token in name for token in tokens):
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
            "hindex": 0,
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
            typeahead=True:  In-memory index only (instant).
            scholar=True:    Extended search across full S2 universe.
            Default:         Local search — index + BigQuery stats views.

        Returns a list of author dicts with keys:
            scholar_id, name, affiliation, email_domain, citedby, hindex, source
        """
        if not author_name or len(author_name.strip()) < 2:
            return []

        name = author_name.strip()

        # Always try the in-memory index first (instant)
        _ensure_index_loaded(self.cache)
        index_results = _search_in_memory(name)
        if typeahead:
            return index_results or []

        # Local search: index + BigQuery stats views
        local_results = self._search_local(name, index_results)

        if not scholar:
            return local_results

        # Extended search: also query the full S2 authors universe
        return self._search_s2_universe(name, local_results)

    def _search_local(self, name, index_results):
        """Search local sources: in-memory index + BigQuery stats views."""
        # Check local results cache first
        cached_results = self.cache.get_search_results(name)
        if cached_results is not None:
            logger.info("Search '%s': local cache hit (%d results)", name, len(cached_results))
            return cached_results

        seen_ids = set()
        results = []

        # Merge any in-memory results first
        if index_results:
            for author in index_results:
                sid = author.get("scholar_id")
                if sid:
                    seen_ids.add(sid)
                    results.append(author)

        # Search authors in BigQuery stats views
        crawled = self.bq.search_crawled_authors(name)
        for author in crawled:
            sid = author.get("scholar_id")
            if sid and sid not in seen_ids:
                seen_ids.add(sid)
                author["source"] = "database"
                results.append(author)

        logger.info("Search '%s': %d local results", name, len(results))
        self.cache.set_search_results(name, results)
        return results

    def _search_s2_universe(self, name, local_results):
        """Search the full S2 authors universe and merge with local results."""
        seen_ids = {r.get("scholar_id") for r in local_results if r.get("scholar_id")}
        results = list(local_results)

        # Check Firestore cache for previous extended search results
        cached = self.cache.get(name)
        if cached is not None:
            for author in cached:
                sid = author.get("scholar_id")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    author["source"] = "s2_universe_cached"
                    results.append(author)
            logger.info("Search '%s': %d results (local + cached S2 universe)", name, len(results))
            return results

        # Query the full S2 authors table in BigQuery
        s2_results = self.bq.search_s2_universe(name, limit=Config.MAX_S2_UNIVERSE_RESULTS)
        if s2_results:
            self.cache.set(name, s2_results)
            for author in s2_results:
                sid = author.get("scholar_id")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    author["source"] = "s2_universe"
                    results.append(author)

        logger.info("Search '%s': %d results (local + S2 universe)", name, len(results))
        return results
