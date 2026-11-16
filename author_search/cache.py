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
# One document per UTC day counting live Google Scholar lookups.
LIVE_SEARCH_USAGE_COLLECTION = "v3_gs_live_search_usage"


class SearchCache:
    def __init__(self, client=None):
        self.db = client or firestore.Client(project=Config.PROJECT_ID)

    def get(self, query_string, ttl_hours=None):
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
            cutoff = datetime.now(timezone.utc) - timedelta(hours=ttl_hours or Config.CACHE_TTL_HOURS)
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

    def reserve_live_search(self, daily_limit):
        """Count one live lookup against today's allowance (UTC day).

        Returns False when the allowance is used up, and also when the counter
        cannot be read: lookups cost money, so an unknown count means no. The
        check and the increment run in one transaction, so requests arriving
        together cannot both take the last lookup.
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        doc = self.db.collection(LIVE_SEARCH_USAGE_COLLECTION).document(today)

        @firestore.transactional
        def _reserve(transaction):
            snapshot = doc.get(transaction=transaction)
            used = (snapshot.to_dict() or {}).get("count", 0) if snapshot.exists else 0
            if used >= daily_limit:
                return False
            transaction.set(doc, {"count": used + 1, "updated": datetime.now(timezone.utc)}, merge=True)
            return True

        try:
            return _reserve(self.db.transaction()) is True
        except Exception:
            logger.exception("Live search counter failed")
            return False

    def get_index_chunk(self, collection, doc_id):
        """Get one document of the author name index (a list, bytes or dict) from Firestore."""
        try:
            doc = self.db.collection(collection).document(doc_id).get()
            if not doc.exists:
                return None
            return doc.to_dict().get("data")
        except Exception:
            logger.exception("Index chunk read failed: %s/%s", collection, doc_id)
            return None

    def set_index_chunk(self, collection, doc_id, data):
        """Save one document of the author name index to Firestore."""
        try:
            self.db.collection(collection).document(doc_id).set({"data": data})
        except Exception:
            logger.exception("Index chunk write failed: %s/%s", collection, doc_id)

    def delete_index_chunk(self, collection, doc_id):
        """Delete one index chunk. Returns True on success."""
        try:
            self.db.collection(collection).document(doc_id).delete()
            return True
        except Exception:
            logger.exception("Index chunk delete failed: %s/%s", collection, doc_id)
            return False

    @staticmethod
    def _safe_doc_id(query_string):
        """Convert a query string to a safe Firestore document ID."""
        # Firestore doc IDs can't contain / or be longer than 1500 bytes
        safe = query_string.strip().lower().replace("/", "_")
        if len(safe.encode("utf-8")) > 1500:
            import hashlib
            safe = hashlib.sha256(safe.encode("utf-8")).hexdigest()
        return safe
