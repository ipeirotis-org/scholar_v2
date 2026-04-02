"""Firestore cache reader for the frontend.

Data population is owned by the Cache Layer (Component 7). The frontend
reads from Firestore only.
"""

import logging
from datetime import datetime, timezone

from google.cloud import firestore

from frontend.config import Config

logger = logging.getLogger(__name__)

class FirestoreCache:
    def __init__(self, client=None):
        self.db = client or firestore.Client(project=Config.PROJECT_ID)

    def get(self, collection, doc_id):
        """Read cached data.

        Returns cached data dict/list, or None if missing.
        """
        try:
            doc = self.db.collection(collection).document(doc_id).get()
            if not doc.exists:
                return None
            cached = doc.to_dict()
            return cached.get("data")
        except Exception:
            logger.exception("Firestore cache read failed: %s/%s", collection, doc_id)
            return None

    def get_timestamp(self, collection, doc_id):
        """Read the cache write timestamp for a document.

        Returns datetime or None if missing.
        """
        try:
            doc = self.db.collection(collection).document(doc_id).get()
            if not doc.exists:
                return None
            cached = doc.to_dict()
            return cached.get("timestamp")
        except Exception:
            logger.exception("Firestore timestamp read failed: %s/%s", collection, doc_id)
            return None

    def delete(self, collection, doc_id):
        """Delete a cached document.

        Returns True if deleted, False on failure.
        """
        try:
            self.db.collection(collection).document(doc_id).delete()
            return True
        except Exception:
            logger.exception("Firestore cache delete failed: %s/%s", collection, doc_id)
            return False

    def set(self, collection, doc_id, data):
        """Write data to cache with current timestamp.

        Used only for frontend-generated data (e.g., recent authors).
        All other cache writes are owned by the Cache Layer.
        """
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

