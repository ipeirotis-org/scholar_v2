"""Firestore cache reader for the frontend.

Data population is owned by the Cache Layer (Component 7). The frontend
reads from Firestore only. The exception is recent-author tracking, which
is maintained by the frontend and written back for reuse.
"""

import logging
import queue
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.cloud import firestore

from frontend.config import Config

logger = logging.getLogger(__name__)

RECENT_AUTHORS_COLLECTION = "v3_recent_authors"
RECENT_AUTHORS_DOC_ID = "recent"
MAX_RECENT_AUTHORS = 20

# Background thread pool for fire-and-forget writes (query logging).
# Bounded work queue: drop log entries when backlogged rather than OOM.
_LOG_QUEUE_MAX = 100
_log_queue: queue.Queue = queue.Queue(maxsize=_LOG_QUEUE_MAX)
_log_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="query-log")


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
        except (GoogleAPICallError, RetryError, ValueError):
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
        except (GoogleAPICallError, RetryError, ValueError):
            logger.exception("Firestore timestamp read failed: %s/%s", collection, doc_id)
            return None

    def delete(self, collection, doc_id):
        """Delete a cached document.

        Returns True if deleted, False on failure.
        """
        try:
            self.db.collection(collection).document(doc_id).delete()
            return True
        except (GoogleAPICallError, RetryError, ValueError):
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
        except (GoogleAPICallError, RetryError, ValueError):
            logger.exception("Firestore cache write failed: %s/%s", collection, doc_id)
            return False

    def log_query(self, query_type, query_text, result_count=None, author_id=None,
                  typeahead=False, scholar=False):
        """Log a search or profile query to Firestore (fire-and-forget).

        The Firestore write runs in a background thread so it never
        blocks the calling request handler.

        Args:
            query_type: 'search' or 'profile_view'
            query_text: The search string or author ID
            result_count: Number of results returned (for searches)
            author_id: Author ID (for profile views)
            typeahead: Whether this was a typeahead search
            scholar: Whether S2 API fallback was used
        """
        entry = {
            "timestamp": datetime.now(timezone.utc),
            "type": query_type,
            "query": query_text,
        }
        if result_count is not None:
            entry["result_count"] = result_count
        if author_id:
            entry["author_id"] = author_id
        if query_type == "search":
            entry["typeahead"] = typeahead
            entry["scholar"] = scholar

        def _write():
            try:
                self.db.collection(Config.CACHE_QUERY_LOG).add(entry)
            except Exception:
                logger.debug("Failed to log query: %s %s", query_type, query_text)
            finally:
                _log_queue.get_nowait()

        try:
            _log_queue.put_nowait(None)
        except queue.Full:
            logger.debug("Query log backlogged, dropping entry")
            return
        _log_pool.submit(_write)

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
            "pip_auc_percentile": author_stats.get("pip_auc_score_percentile"),
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
        except (GoogleAPICallError, RetryError, ValueError):
            logger.exception("Failed to record recent author: %s", entry.get("scholar_id"))
