"""Firestore cache reader for the frontend.

Data population is owned by the Cache Layer (Component 7). The frontend
reads from Firestore only. The exceptions are plot caching and recent-author
tracking — both are maintained by the frontend and written back for reuse.
"""

import logging
from datetime import datetime, timezone

from google.cloud import firestore

from frontend.config import Config

logger = logging.getLogger(__name__)

RECENT_AUTHORS_COLLECTION = "v3_recent_authors"
RECENT_AUTHORS_DOC_ID = "recent"
MAX_RECENT_AUTHORS = 20


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

        Used only for frontend-generated data (e.g., plot caching).
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

    def record_recent_author(self, author_stats):
        """Record an author as recently queried.

        Maintains a list of the most recently queried authors in Firestore,
        ordered by query time (most recent first). Deduplicates by scholar_id.
        """
        entry = {
            "scholar_id": author_stats.get("scholar_id"),
            "name": author_stats.get("name"),
            "affiliation": author_stats.get("affiliation"),
            "hindex": author_stats.get("hindex"),
            "citedby": author_stats.get("citedby"),
            "pip_auc_score": author_stats.get("pip_auc_score"),
            "pip_auc_percentile": author_stats.get("pip_auc_percentile"),
        }

        if not entry["scholar_id"]:
            return

        try:
            current = self.get(RECENT_AUTHORS_COLLECTION, RECENT_AUTHORS_DOC_ID)
            if not isinstance(current, list):
                current = []

            # Remove existing entry for this author (if any) to move to front
            current = [a for a in current if a.get("scholar_id") != entry["scholar_id"]]

            # Prepend and truncate
            current = [entry] + current
            current = current[:MAX_RECENT_AUTHORS]

            self.set(RECENT_AUTHORS_COLLECTION, RECENT_AUTHORS_DOC_ID, current)
        except Exception:
            logger.exception("Failed to record recent author: %s", entry.get("scholar_id"))
