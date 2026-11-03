"""Cache population orchestration.

Dispatches by request type and coordinates BigQuery reads + Firestore writes.
"""

import logging
import re

from cache_layer.bigquery_client import BigQueryClient
from cache_layer.cache_writer import CacheWriter
from cache_layer.config import Config
from cache_layer.crawl_lock import CrawlLock

logger = logging.getLogger(__name__)

# S2 author IDs are purely numeric strings
_S2_ID_RE = re.compile(r"^\d+$")
# Google Scholar user IDs are 12 characters, e.g. "PA9La6oAAAAJ"
_GS_ID_RE = re.compile(r"^[A-Za-z0-9_-]{12}$")


def _default_gs_crawl(scholar_id):
    """Crawl one GS profile via SerpApi and append it to gs_data.

    Returns True if the profile was stored. Imported lazily so S2 deployments
    don't need the crawler's dependencies.
    """
    from gs_crawler.main import crawl_and_store, searches_left

    left = searches_left()
    if left < Config.GS_ON_DEMAND_MIN_SEARCHES:
        logger.warning("Skipping on-demand crawl of %s: only %d SerpApi searches left", scholar_id, left)
        return False
    summary = crawl_and_store([scholar_id], budget=left - Config.GS_ON_DEMAND_MIN_SEARCHES + 1)
    return summary["crawled"] == 1


class CacheService:
    def __init__(self, bq=None, writer=None, gs_crawl=None, crawl_lock=None):
        self.bq = bq or BigQueryClient()
        self.writer = writer or CacheWriter()
        self.gs_crawl = gs_crawl or _default_gs_crawl
        self._crawl_lock = crawl_lock

    def dispatch(self, request_type, payload):
        """Dispatch a cache population request by type.

        Returns a dict with status and details.
        """
        handlers = {
            "populate_author_profile": self._populate_author_profile,
            "populate_publication_detail": self._populate_publication_detail,
            "invalidate_author": self._invalidate_author,
            "warm_author": self._populate_author_profile,
            "purge_legacy_cache": self._purge_legacy_cache,
            "flush_cache": self._flush_cache,
        }

        handler = handlers.get(request_type)
        if not handler:
            return {"status": "error", "message": f"Unknown request type: {request_type}"}

        return handler(payload)

    def _populate_author_profile(self, payload):
        """Query BQ for all author data and write to Firestore cache.

        Populates: author_stats, author_pub_stats, author_temporal, author_freshness.
        """
        scholar_id = payload.get("scholar_id")
        if not scholar_id:
            return {"status": "error", "message": "Missing scholar_id"}

        exists, last_updated = self.bq.get_author_freshness(scholar_id)
        if not exists and Config.IS_GS and _GS_ID_RE.match(scholar_id):
            # Unknown GS author: crawl the profile now (~3 SerpApi searches),
            # unless another task is already crawling it.
            acquired, token = self._acquire_crawl_lock(scholar_id)
            if not acquired:
                return {"status": "in_progress", "scholar_id": scholar_id}
            try:
                crawled = self.gs_crawl(scholar_id)
            except Exception:
                logger.exception("On-demand GS crawl failed for %s", scholar_id)
                crawled = False
            finally:
                if token:
                    self._release_crawl_lock(scholar_id, token)
            if crawled:
                exists, last_updated = self.bq.get_author_freshness(scholar_id)
        if not exists:
            return {"status": "not_found", "scholar_id": scholar_id}

        # Query all data from the BigQuery views (S2 or GS, per Config.DATA_SOURCE)
        author_stats = self.bq.get_author_stats(scholar_id)
        pub_stats = self.bq.get_author_pub_stats(scholar_id)
        temporal_stats = self.bq.get_author_temporal_stats(scholar_id)

        # Write data to cache first, then freshness — so the frontend
        # never sees a "fresh" marker without the corresponding data.
        writes = []
        if author_stats:
            writes.append((Config.CACHE_AUTHOR_STATS, scholar_id, author_stats))
        if pub_stats:
            writes.append((Config.CACHE_AUTHOR_PUB_STATS, scholar_id, pub_stats))
        if temporal_stats:
            writes.append((Config.CACHE_AUTHOR_TEMPORAL, scholar_id, temporal_stats))

        written = self.writer.write_batch(writes)

        # Determine status based on write success
        if writes and written == 0:
            # Had data to write but all writes failed — don't write freshness
            status = "error"
        else:
            # Write freshness: either data was written successfully, or BQ
            # returned no data (author exists but has no stats yet).  In both
            # cases, record freshness so the frontend doesn't re-enqueue.
            self.writer.write(Config.CACHE_AUTHOR_FRESHNESS, scholar_id, {
                "exists": True,
                "last_updated": last_updated,
            })
            if writes and written < len(writes):
                status = "partial_failure"
            else:
                status = "ok"

        return {
            "status": status,
            "scholar_id": scholar_id,
            "cached": {
                "author_stats": author_stats is not None,
                "pub_stats": bool(pub_stats),
                "temporal_stats": bool(temporal_stats),
            },
            "writes": written,
        }

    @property
    def crawl_lock(self):
        if self._crawl_lock is None:
            self._crawl_lock = CrawlLock(self.writer.db, Config.GS_CRAWL_LOCKS, Config.GS_CRAWL_LOCK_MINUTES)
        return self._crawl_lock

    def _acquire_crawl_lock(self, scholar_id):
        """Return (may_crawl, token).

        (True, token): this task owns the lock. (False, None): another live
        crawl owns it. (True, None): Firestore failed, so crawl unlocked (a rare
        duplicate costs a few searches; a missing profile costs the user) and
        release nothing.
        """
        try:
            token = self.crawl_lock.acquire(scholar_id)
        except Exception:
            logger.exception("Crawl lock unavailable for %s; crawling without it", scholar_id)
            return True, None
        if token is None:
            logger.info("GS crawl of %s already in progress; skipping", scholar_id)
            return False, None
        return True, token

    def _release_crawl_lock(self, scholar_id, token):
        try:
            self.crawl_lock.release(scholar_id, token)
        except Exception:
            logger.exception("Failed to release crawl lock for %s", scholar_id)

    def _populate_publication_detail(self, payload):
        """Query BQ for publication temporal data and write to cache."""
        author_pub_id = payload.get("author_pub_id")
        if not author_pub_id:
            return {"status": "error", "message": "Missing author_pub_id"}

        pub_stats = self.bq.get_publication_stats(author_pub_id)
        if not pub_stats:
            return {"status": "not_found", "author_pub_id": author_pub_id}

        success = self.writer.write(Config.CACHE_PUB_STATS, author_pub_id, pub_stats)

        return {
            "status": "ok" if success else "error",
            "author_pub_id": author_pub_id,
            "records": len(pub_stats),
        }

    def _invalidate_author(self, payload):
        """Re-populate all caches for an author whose data has changed."""
        scholar_id = payload.get("scholar_id")
        if not scholar_id:
            return {"status": "error", "message": "Missing scholar_id"}

        # Just delegate to populate — it always overwrites
        return self._populate_author_profile(payload)

    def _flush_cache(self, payload):
        """Delete all documents from all Firestore cache collections.

        This is a destructive operation. The cache repopulates on-demand
        as users query the frontend. Recent authors will repopulate
        organically.
        """
        collections = [
            Config.CACHE_AUTHOR_STATS,
            Config.CACHE_AUTHOR_PUB_STATS,
            Config.CACHE_AUTHOR_TEMPORAL,
            Config.CACHE_AUTHOR_FRESHNESS,
            Config.CACHE_PUB_STATS,
            Config.CACHE_RECENT_AUTHORS,
        ]

        total_deleted = 0
        failed_collections = []

        for collection_name in collections:
            try:
                deleted = self.writer.delete_collection(collection_name)
                total_deleted += deleted
                logger.info("Flushed %d entries from %s", deleted, collection_name)
            except Exception:
                logger.exception("Failed to flush %s", collection_name)
                failed_collections.append(collection_name)

        status = "ok" if not failed_collections else "partial_failure"
        result = {
            "status": status,
            "total_deleted": total_deleted,
        }
        if failed_collections:
            result["failed_collections"] = failed_collections
        return result

    def _purge_legacy_cache(self, payload):
        """Delete Firestore cache entries with non-S2 (legacy Google Scholar) keys.

        S2 author IDs are purely numeric. Any cache entry whose document ID
        contains letters, hyphens, or underscores is a legacy Google Scholar
        entry that should be purged.

        Scans all author-keyed cache collections and deletes non-numeric entries.
        Refused in GS mode, where every valid key is non-numeric.
        """
        if Config.IS_GS:
            return {"status": "error", "message": "purge_legacy_cache is disabled when DATA_SOURCE=gs"}
        collections = [
            Config.CACHE_AUTHOR_STATS,
            Config.CACHE_AUTHOR_PUB_STATS,
            Config.CACHE_AUTHOR_TEMPORAL,
            Config.CACHE_AUTHOR_FRESHNESS,
        ]

        db = self.writer.db
        total_deleted = 0
        total_scanned = 0
        failed_collections = []

        for collection_name in collections:
            deleted = 0
            scanned = 0
            try:
                collection_ref = db.collection(collection_name)
                # Stream all documents (only ID, no data needed)
                for doc in collection_ref.select([]).stream():
                    scanned += 1
                    doc_id = doc.id
                    if not _S2_ID_RE.match(doc_id):
                        doc.reference.delete()
                        deleted += 1
                logger.info(
                    "Purged %d/%d legacy entries from %s",
                    deleted, scanned, collection_name,
                )
            except Exception:
                logger.exception("Failed to purge legacy cache from %s", collection_name)
                failed_collections.append(collection_name)

            total_deleted += deleted
            total_scanned += scanned

        status = "ok" if not failed_collections else "partial_failure"
        result = {
            "status": status,
            "total_scanned": total_scanned,
            "total_deleted": total_deleted,
        }
        if failed_collections:
            result["failed_collections"] = failed_collections
        return result
