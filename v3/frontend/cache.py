"""Firestore cache layer for BigQuery query results.

Cache invalidation: data is stale if the author's latest raw data timestamp
is newer than the cached entry's timestamp.
"""

import logging
from datetime import datetime, timezone

from google.cloud import firestore

from v3.frontend.config import Config

logger = logging.getLogger(__name__)


class FirestoreCache:
    def __init__(self, client=None):
        self.db = client or firestore.Client(project=Config.PROJECT_ID)

    def get(self, collection, doc_id, valid_after=None):
        """Get cached data if it exists and is not stale.

        Args:
            collection: Firestore collection name.
            doc_id: Document ID.
            valid_after: If provided, cache is only valid if its timestamp
                         is after this datetime.

        Returns:
            Cached data dict/list, or None if missing/stale.
        """
        try:
            doc = self.db.collection(collection).document(doc_id).get()
            if not doc.exists:
                return None
            cached = doc.to_dict()
            cached_time = cached.get("timestamp")
            if valid_after and cached_time and cached_time < valid_after:
                logger.info("Cache stale for %s/%s", collection, doc_id)
                return None
            return cached.get("data")
        except Exception:
            logger.exception("Firestore cache read failed: %s/%s", collection, doc_id)
            return None

    def set(self, collection, doc_id, data):
        """Write data to cache with current timestamp."""
        if not doc_id or not doc_id.strip():
            return False
        try:
            self.db.collection(collection).document(doc_id).set({
                "timestamp": datetime.now(timezone.utc),
                "data": data,
            })
            return True
        except Exception:
            logger.exception("Firestore cache write failed: %s/%s", collection, doc_id)
            return False
