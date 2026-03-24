"""Cache population orchestration.

Dispatches by request type and coordinates BigQuery reads + Firestore writes.
"""

import json
import logging

from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2

from cache_layer.bigquery_client import BigQueryClient
from cache_layer.cache_writer import CacheWriter
from cache_layer.config import Config

logger = logging.getLogger(__name__)

_tasks_client = None


def _get_tasks_client():
    global _tasks_client
    if _tasks_client is None:
        _tasks_client = tasks_v2.CloudTasksClient()
    return _tasks_client


class CacheService:
    def __init__(self, bq=None, writer=None):
        self.bq = bq or BigQueryClient()
        self.writer = writer or CacheWriter()

    def dispatch(self, request_type, payload):
        """Dispatch a cache population request by type.

        Returns a dict with status and details.
        """
        handlers = {
            "populate_author_profile": self._populate_author_profile,
            "populate_publication_detail": self._populate_publication_detail,
            "populate_recent_authors": self._populate_recent_authors,
            "invalidate_author": self._invalidate_author,
            "warm_author": self._populate_author_profile,
            "rebuild_all": self._rebuild_all,
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
        if not exists:
            return {"status": "not_found", "scholar_id": scholar_id}

        # Write freshness first
        self.writer.write(Config.CACHE_AUTHOR_FRESHNESS, scholar_id, {
            "exists": True,
            "last_updated": last_updated,
        })

        # Query all data
        author_stats = self.bq.get_author_stats(scholar_id)
        pub_stats = self.bq.get_author_pub_stats(scholar_id)

        # Check if pub_latest_table is stale (raw data newer than materialized).
        # This covers both empty pub_stats (new author, never materialized) and
        # non-empty but stale pub_stats (new raw pubs ingested since last
        # materialization). If the freshness check returns None (query error),
        # skip refresh to avoid unnecessary DML driven by read errors.
        materialized_fresh = None
        if author_stats is not None and self.bq.author_has_raw_pubs(scholar_id):
            materialized_fresh = self.bq.author_pubs_freshly_materialized(scholar_id)
        if materialized_fresh is False:
            rows = self.bq.refresh_author_pubs(scholar_id)
            if rows > 0:
                logger.info("Retrying after refresh for %s", scholar_id)
                pub_stats = self.bq.get_author_pub_stats(scholar_id)
                # Re-fetch author stats since PiP scores depend on publication data;
                # keep original stats as fallback if the retry fails transiently.
                refreshed_stats = self.bq.get_author_stats(scholar_id)
                if refreshed_stats is not None:
                    author_stats = refreshed_stats

        temporal_stats = self.bq.get_author_temporal_stats(scholar_id)

        # Write to cache
        writes = []
        if author_stats:
            writes.append((Config.CACHE_AUTHOR_STATS, scholar_id, author_stats))
        if pub_stats:
            writes.append((Config.CACHE_AUTHOR_PUB_STATS, scholar_id, pub_stats))
        if temporal_stats:
            writes.append((Config.CACHE_AUTHOR_TEMPORAL, scholar_id, temporal_stats))

        written = self.writer.write_batch(writes)

        return {
            "status": "ok",
            "scholar_id": scholar_id,
            "cached": {
                "author_stats": author_stats is not None,
                "pub_stats": bool(pub_stats),
                "temporal_stats": bool(temporal_stats),
            },
            "writes": written,
        }

    def _populate_publication_detail(self, payload):
        """Query BQ for publication temporal data and write to cache."""
        author_pub_id = payload.get("author_pub_id")
        if not author_pub_id:
            return {"status": "error", "message": "Missing author_pub_id"}

        pub_stats = self.bq.get_publication_stats(author_pub_id)
        if not pub_stats:
            return {"status": "not_found", "author_pub_id": author_pub_id}

        self.writer.write(Config.CACHE_PUB_STATS, author_pub_id, pub_stats)

        return {
            "status": "ok",
            "author_pub_id": author_pub_id,
            "records": len(pub_stats),
        }

    def _populate_recent_authors(self, payload):
        """Query BQ for recently analyzed authors and write to cache."""
        limit = payload.get("limit", 20)
        recent = self.bq.get_recently_analyzed_authors(limit=limit)

        self.writer.write(Config.CACHE_RECENT_AUTHORS, "recent", recent)

        return {
            "status": "ok",
            "authors_cached": len(recent) if recent else 0,
        }

    def _invalidate_author(self, payload):
        """Re-populate all caches for an author whose data has changed."""
        scholar_id = payload.get("scholar_id")
        if not scholar_id:
            return {"status": "error", "message": "Missing scholar_id"}

        # Just delegate to populate — it always overwrites
        return self._populate_author_profile(payload)

    def _rebuild_all(self, payload):
        """Enqueue populate_author_profile for every known author to the batch queue.

        This fans out individual tasks so the batch queue controls concurrency.
        """
        author_ids = self.bq.get_all_author_ids()
        if not author_ids:
            return {"status": "error", "message": "No authors found in BigQuery"}

        cache_layer_url = Config.CACHE_LAYER_URL
        if not cache_layer_url:
            return {"status": "error", "message": "CACHE_LAYER_URL not configured"}

        client = _get_tasks_client()
        queue_path = Config.queue_path(Config.QUEUE_NAME_BATCH)
        target_url = f"{cache_layer_url.rstrip('/')}/tasks/batch"

        enqueued = 0
        errors = 0
        for scholar_id in author_ids:
            task_body = json.dumps({
                "type": "populate_author_profile",
                "scholar_id": scholar_id,
            }).encode()

            task = {
                "http_request": {
                    "http_method": tasks_v2.HttpMethod.POST,
                    "url": target_url,
                    "headers": {"Content-Type": "application/json"},
                    "body": task_body,
                },
            }

            try:
                client.create_task(parent=queue_path, task=task)
                enqueued += 1
            except AlreadyExists:
                enqueued += 1  # Already queued, still counts
            except Exception:
                logger.exception("Failed to enqueue rebuild task for %s", scholar_id)
                errors += 1

        # Also refresh recent authors
        recent_task_body = json.dumps({
            "type": "populate_recent_authors",
        }).encode()
        try:
            client.create_task(parent=queue_path, task={
                "http_request": {
                    "http_method": tasks_v2.HttpMethod.POST,
                    "url": target_url,
                    "headers": {"Content-Type": "application/json"},
                    "body": recent_task_body,
                },
            })
        except Exception:
            logger.exception("Failed to enqueue recent authors rebuild task")

        return {
            "status": "ok",
            "total_authors": len(author_ids),
            "enqueued": enqueued,
            "errors": errors,
        }
