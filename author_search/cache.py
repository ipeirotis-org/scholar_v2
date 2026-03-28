"""Firestore cache for author search results.

Caches search results from local BigQuery and S2 universe queries
so that repeat searches skip BigQuery entirely.
"""

import logging
from datetime import datetime, timedelta, timezone

from google.cloud import firestore

from author_search.config import Config

logger = logging.getLogger(__name__)

# Collection for caching final merged search results (all tiers combined)
SEARCH_RESULTS_COLLECTION = "v3_search_results"


class SearchCache:
    def __init__(self, client=None):
        self.db = client or firestore.Client(project=Config.PROJECT_ID)

    def get(self, query_string):
        """Get cached extended search results if fresh.

        Returns list of author dicts, or None if missing/stale.
        """
        doc_id = self._safe_doc_id(query_string)
        try:
            doc = self.db.collection(Config.CACHE_COLLECTION).document(doc_id).get()
            if not doc.exists:
                return None
            cached = doc.to_dict()
            cached_time = cached.get("timestamp")
            if not cached_time:
                return None
            cutoff = datetime.now(timezone.utc) - timedelta(hours=Config.CACHE_TTL_HOURS)
            if cached_time < cutoff:
                logger.info("Search cache stale for query: %s", query_string)
                return None
            return cached.get("data")
        except Exception:
            logger.exception("Search cache read failed for: %s", query_string)
            return None

    def set(self, query_string, results):
        """Cache extended search results."""
        doc_id = self._safe_doc_id(query_string)
        try:
            self.db.collection(Config.CACHE_COLLECTION).document(doc_id).set({
                "timestamp": datetime.now(timezone.utc),
                "query": query_string,
                "data": results,
            })
        except Exception:
            logger.exception("Search cache write failed for: %s", query_string)

    def get_search_results(self, query_string):
        """Get cached final search results (all tiers combined) if fresh.

        Returns list of author dicts, or None if missing/stale.
        """
        doc_id = self._safe_doc_id(query_string)
        try:
            doc = self.db.collection(SEARCH_RESULTS_COLLECTION).document(doc_id).get()
            if not doc.exists:
                return None
            cached = doc.to_dict()
            cached_time = cached.get("timestamp")
            if not cached_time:
                return None
            cutoff = datetime.now(timezone.utc) - timedelta(hours=Config.CACHE_TTL_HOURS)
            if cached_time < cutoff:
                logger.info("Search results cache stale for query: %s", query_string)
                return None
            return cached.get("data")
        except Exception:
            logger.exception("Search results cache read failed for: %s", query_string)
            return None

    def set_search_results(self, query_string, results):
        """Cache the final merged search results."""
        doc_id = self._safe_doc_id(query_string)
        try:
            self.db.collection(SEARCH_RESULTS_COLLECTION).document(doc_id).set({
                "timestamp": datetime.now(timezone.utc),
                "query": query_string,
                "data": results,
            })
        except Exception:
            logger.exception("Search results cache write failed for: %s", query_string)

    def get_index_chunk(self, collection, doc_id):
        """Get a chunk of the author name index from Firestore."""
        try:
            doc = self.db.collection(collection).document(doc_id).get()
            if not doc.exists:
                return None
            return doc.to_dict().get("data")
        except Exception:
            logger.exception("Index chunk read failed: %s/%s", collection, doc_id)
            return None

    def set_index_chunk(self, collection, doc_id, data):
        """Save a chunk of the author name index to Firestore."""
        try:
            self.db.collection(collection).document(doc_id).set({"data": data})
        except Exception:
            logger.exception("Index chunk write failed: %s/%s", collection, doc_id)

    @staticmethod
    def _safe_doc_id(query_string):
        """Convert a query string to a safe Firestore document ID."""
        # Firestore doc IDs can't contain / or be longer than 1500 bytes
        safe = query_string.strip().lower().replace("/", "_")
        if len(safe.encode("utf-8")) > 1500:
            import hashlib
            safe = hashlib.sha256(safe.encode("utf-8")).hexdigest()
        return safe
