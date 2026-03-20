"""Author Search Service — Component 6.

Implements the search strategy described in ARCHITECTURE.md:
0. Search in-memory author name index (instant, no I/O)
1. Check top-level search cache (fast, avoids BigQuery on repeat queries)
2. Search crawled authors in BigQuery (fast, free)
3. Search coauthor network in BigQuery (fast, free)
4. Check Firestore cache for Scholar results
5. Fall back to Google Scholar (slow, rate-limited)

Results from all sources are deduplicated by scholar_id and merged.
The final merged results are cached so repeat queries skip BigQuery entirely.
"""

import logging
import threading
import time

from v3.author_search.bigquery_client import BigQuerySearchClient
from v3.author_search.cache import SearchCache
from v3.author_search.config import Config
from v3.author_search import scholar_client

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

    def search(self, author_name, typeahead=False):
        """Search for authors by name using the tiered strategy.

        If typeahead=True, only searches the in-memory index for speed.

        Returns a list of author dicts with keys:
            scholar_id, name, affiliation, email_domain, citedby, hindex, source
        """
        if not author_name or len(author_name.strip()) < 2:
            return []

        name = author_name.strip()

        # Try the in-memory index first (instant)
        _ensure_index_loaded(self.cache)
        index_results = _search_in_memory(name)
        if index_results is not None and len(index_results) >= Config.LOCAL_RESULTS_THRESHOLD:
            return index_results
        if typeahead:
            return index_results or []

        # Step 0: Check top-level search results cache
        cached_results = self.cache.get_search_results(name)
        if cached_results is not None:
            logger.info("Search '%s': cache hit (%d results)", name, len(cached_results))
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

        # Step 1: Search crawled authors
        crawled = self.bq.search_crawled_authors(name)
        for author in crawled:
            sid = author.get("scholar_id")
            if sid and sid not in seen_ids:
                seen_ids.add(sid)
                author["source"] = "database"
                results.append(author)

        if len(results) >= Config.LOCAL_RESULTS_THRESHOLD:
            logger.info("Search '%s': %d results from crawled authors", name, len(results))
            self.cache.set_search_results(name, results)
            return results

        # Step 2: Search coauthor network
        coauthors = self.bq.search_coauthor_network(name)
        for author in coauthors:
            sid = author.get("scholar_id")
            if sid and sid not in seen_ids:
                seen_ids.add(sid)
                author["source"] = "coauthor_network"
                results.append(author)

        if len(results) >= Config.LOCAL_RESULTS_THRESHOLD:
            logger.info("Search '%s': %d results (crawled + coauthor)", name, len(results))
            self.cache.set_search_results(name, results)
            return results

        # Step 3: Check Firestore cache for Scholar results
        cached = self.cache.get(name)
        if cached is not None:
            for author in cached:
                sid = author.get("scholar_id")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    author["source"] = "scholar_cached"
                    results.append(author)
            logger.info("Search '%s': %d results (local + cached scholar)", name, len(results))
            self.cache.set_search_results(name, results)
            return results

        # Step 4: Fall back to Google Scholar
        scholar_results = scholar_client.search_scholar(name)
        if scholar_results:
            self.cache.set(name, scholar_results)
            for author in scholar_results:
                sid = author.get("scholar_id")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    author["source"] = "scholar"
                    results.append(author)

        logger.info("Search '%s': %d total results (all sources)", name, len(results))
        self.cache.set_search_results(name, results)
        return results
