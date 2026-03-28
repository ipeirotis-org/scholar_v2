"""Author Search Service — Component 6.

All author search is backed by an in-memory index loaded from the
daily-materialized ranked_author_current_table in BigQuery. The index
contains all S2 authors with meaningful activity (citationcount > 0,
total_publications >= 3, hindex > 3). No BigQuery queries happen at
search time.

Two search modes:
1. Typeahead (typeahead=True): In-memory index, top 10 results (instant).
2. Full search (default): In-memory index, top 50 results.
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


def _search_in_memory(query, limit=_TYPEAHEAD_LIMIT):
    """Search the in-memory index by substring matching.

    All query tokens must appear in the author name.
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

        All search is backed by the in-memory index of active S2 authors.

        Modes:
            typeahead=True:  Top 10 results (instant, no caching overhead).
            Default:         Top 50 results, cached in Firestore.

        The `scholar` parameter is accepted for API compatibility but no
        longer changes behavior — all searches use the same index.

        Returns a list of author dicts with keys:
            scholar_id, name, affiliation, email_domain, citedby, hindex, source
        """
        if not author_name or len(author_name.strip()) < 2:
            return []

        name = author_name.strip()

        # Ensure the in-memory index is loaded
        _ensure_index_loaded(self.cache)

        if typeahead:
            return _search_in_memory(name, limit=_TYPEAHEAD_LIMIT) or []

        # Full search with caching
        cached_results = self.cache.get_search_results(name)
        if cached_results is not None:
            logger.info("Search '%s': cache hit (%d results)", name, len(cached_results))
            return cached_results

        results = _search_in_memory(name, limit=_FULL_SEARCH_LIMIT) or []
        logger.info("Search '%s': %d results from index", name, len(results))
        self.cache.set_search_results(name, results)
        return results
