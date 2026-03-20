"""In-memory author name index for fast search.

Loads author names from Firestore (populated from BigQuery) and performs
substring matching locally. Falls back to scholarly-based search on miss.
"""

import logging
import threading
import time

from shared.services.firestore_service import FirestoreService
from shared.services.bigquery_service import BigQueryService

logging.basicConfig(level=logging.INFO)

firestore_service = FirestoreService()
bigquery_service = BigQueryService()

# In-memory cache
_author_index = []
_index_lock = threading.Lock()
_index_loaded_at = 0
_INDEX_TTL_SECONDS = 6 * 3600  # Refresh every 6 hours

# Firestore collection for the cached index (split into chunks)
_INDEX_COLLECTION = "author_search_index"
_CHUNK_SIZE = 4000


def _load_index_from_firestore():
    """Load the author name index from Firestore chunks."""
    authors = []
    chunk_num = 0
    while True:
        doc_id = f"chunk_{chunk_num}"
        data, _ = firestore_service.get_firestore_cache(_INDEX_COLLECTION, doc_id)
        if not data:
            break
        authors.extend(data)
        chunk_num += 1
    return authors


def _save_index_to_firestore(authors):
    """Save the author name index to Firestore in chunks."""
    for i in range(0, len(authors), _CHUNK_SIZE):
        chunk = authors[i:i + _CHUNK_SIZE]
        doc_id = f"chunk_{i // _CHUNK_SIZE}"
        firestore_service.set_firestore_cache(_INDEX_COLLECTION, doc_id, chunk)
    logging.info(f"Saved {len(authors)} authors in {(len(authors) + _CHUNK_SIZE - 1) // _CHUNK_SIZE} chunks")


def refresh_author_index():
    """Rebuild the author name index from BigQuery and cache in Firestore + memory."""
    global _author_index, _index_loaded_at
    logging.info("Refreshing author name index from BigQuery...")
    authors = bigquery_service.get_all_author_names()
    if not authors:
        logging.warning("No authors returned from BigQuery for name index")
        return 0

    # Normalize for search: add lowercase name field
    for a in authors:
        a["name_lower"] = (a.get("name") or "").lower()

    _save_index_to_firestore(authors)

    with _index_lock:
        _author_index = authors
        _index_loaded_at = time.time()

    logging.info(f"Author name index refreshed: {len(authors)} authors")
    return len(authors)


def _ensure_index_loaded():
    """Lazily load the index from Firestore if not yet in memory or stale."""
    global _author_index, _index_loaded_at

    if _author_index and (time.time() - _index_loaded_at) < _INDEX_TTL_SECONDS:
        return

    with _index_lock:
        # Double-check after acquiring lock
        if _author_index and (time.time() - _index_loaded_at) < _INDEX_TTL_SECONDS:
            return

        authors = _load_index_from_firestore()
        if authors:
            # Add lowercase name for search if not present
            for a in authors:
                if "name_lower" not in a:
                    a["name_lower"] = (a.get("name") or "").lower()
            _author_index = authors
            _index_loaded_at = time.time()
            logging.info(f"Loaded {len(authors)} authors from Firestore index")
        else:
            logging.warning("No author index in Firestore; run refresh_author_index first")


def search_authors(query, limit=10):
    """Search authors by name using substring matching on the in-memory index.

    Returns a list of matching authors sorted by citation count (descending).
    """
    _ensure_index_loaded()

    if not _author_index:
        return []

    query_lower = query.lower().strip()
    if not query_lower:
        return []

    # Split query into tokens for multi-word matching
    tokens = query_lower.split()

    matches = []
    for author in _author_index:
        name = author.get("name_lower", "")
        if all(token in name for token in tokens):
            matches.append(author)

    # Sort by citedby descending (most cited first)
    matches.sort(key=lambda a: a.get("citedby") or 0, reverse=True)

    # Return top results in the expected format
    results = []
    for a in matches[:limit]:
        results.append({
            "name": a.get("name", ""),
            "affiliation": a.get("affiliation", ""),
            "email": "",
            "citedby": a.get("citedby", 0),
            "scholar_id": a.get("scholar_id", ""),
        })
    return results
