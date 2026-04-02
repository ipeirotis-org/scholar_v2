"""Write-only Firestore client for cache population.

This is the single writer to all Firestore cache collections.
The frontend reads from these collections but never writes.
"""

import logging
from datetime import datetime, timezone

from google.cloud import firestore

from cache_layer.config import Config

logger = logging.getLogger(__name__)


class CacheWriter:
    def __init__(self, client=None):
        self.db = client or firestore.Client(project=Config.PROJECT_ID)

    def write(self, collection, doc_id, data):
        """Write data to a cache collection with current timestamp.

        Returns True on success, False on failure.
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
            logger.exception("Cache write failed: %s/%s", collection, doc_id)
            return False

    def write_batch(self, writes):
        """Write multiple cache entries in a Firestore batch.

        Args:
            writes: list of (collection, doc_id, data) tuples.

        Returns number of successfully committed writes.
        """
        if not writes:
            return 0

        now = datetime.now(timezone.utc)
        batch = self.db.batch()
        committed = 0
        pending = 0

        for collection, doc_id, data in writes:
            if not doc_id or not doc_id.strip():
                continue
            ref = self.db.collection(collection).document(doc_id)
            batch.set(ref, {"timestamp": now, "data": data})
            pending += 1

            # Firestore batches are limited to 500 operations
            if pending == 500:
                try:
                    batch.commit()
                    committed += pending
                except Exception:
                    logger.exception(
                        "Batch commit failed (%d items lost)", pending
                    )
                pending = 0
                batch = self.db.batch()

        if pending > 0:
            try:
                batch.commit()
                committed += pending
            except Exception:
                logger.exception(
                    "Final batch commit failed (%d items lost)", pending
                )

        return committed

    def delete_collection(self, collection, batch_size=500):
        """Delete all documents in a Firestore collection.

        Returns the number of documents deleted.
        """
        deleted = 0
        try:
            collection_ref = self.db.collection(collection)
            while True:
                docs = list(collection_ref.limit(batch_size).stream())
                if not docs:
                    break
                batch = self.db.batch()
                for doc in docs:
                    batch.delete(doc.reference)
                batch.commit()
                deleted += len(docs)
        except Exception:
            logger.exception("Failed to delete collection %s (deleted %d so far)", collection, deleted)
        return deleted
