"""Health dashboard service — queries BigQuery and Cloud Tasks for system metrics."""

import logging
from datetime import datetime, timezone

from google.cloud import bigquery, tasks_v2

from frontend.config import Config

logger = logging.getLogger(__name__)


class HealthService:
    """Gathers system health metrics from BigQuery and Cloud Tasks."""

    def __init__(self, bq_client=None, tasks_client=None):
        self._bq = bq_client
        self._tasks = tasks_client

    @property
    def bq(self):
        if self._bq is None:
            self._bq = bigquery.Client(project=Config.PROJECT_ID)
        return self._bq

    @property
    def tasks_client(self):
        if self._tasks is None:
            self._tasks = tasks_v2.CloudTasksClient()
        return self._tasks

    # ------------------------------------------------------------------
    # BigQuery metrics
    # ------------------------------------------------------------------

    def get_author_stats(self):
        """Return author counts: total, stale (>90d), errored, and fetch-date buckets."""
        query = f"""
        SELECT
            COUNT(*) AS total_authors,
            COUNTIF(timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY))
                AS stale_authors,
            COUNTIF(JSON_EXTRACT_SCALAR(data, '$.error') IS NOT NULL)
                AS error_authors,
            COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
                AS fetched_1d,
            COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
                AS fetched_7d,
            COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY))
                AS fetched_30d,
            COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY))
                AS fetched_90d,
            MIN(timestamp) AS oldest_fetch,
            MAX(timestamp) AS newest_fetch
        FROM {Config.bq_raw('author_latest')}
        """
        try:
            rows = list(self.bq.query(query).result())
            if rows:
                row = rows[0]
                return {
                    "total_authors": row.total_authors,
                    "stale_authors": row.stale_authors,
                    "error_authors": row.error_authors,
                    "fetched_1d": row.fetched_1d,
                    "fetched_7d": row.fetched_7d,
                    "fetched_30d": row.fetched_30d,
                    "fetched_90d": row.fetched_90d,
                    "oldest_fetch": row.oldest_fetch,
                    "newest_fetch": row.newest_fetch,
                }
        except Exception:
            logger.exception("Failed to query author stats")
        return None

    def get_publication_stats(self):
        """Return publication counts and freshness."""
        query = f"""
        SELECT
            COUNT(*) AS total_publications,
            MIN(timestamp) AS oldest_fetch,
            MAX(timestamp) AS newest_fetch
        FROM {Config.bq_raw('pub_latest')}
        """
        try:
            rows = list(self.bq.query(query).result())
            if rows:
                row = rows[0]
                return {
                    "total_publications": row.total_publications,
                    "oldest_fetch": row.oldest_fetch,
                    "newest_fetch": row.newest_fetch,
                }
        except Exception:
            logger.exception("Failed to query publication stats")
        return None

    def get_fetch_date_histogram(self):
        """Return weekly fetch counts for the last 6 months."""
        query = f"""
        SELECT
            DATE_TRUNC(DATE(timestamp), WEEK) AS week_start,
            COUNT(*) AS author_count
        FROM {Config.bq_raw('author_latest')}
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
        GROUP BY week_start
        ORDER BY week_start
        """
        try:
            rows = list(self.bq.query(query).result())
            return [
                {"week": row.week_start.isoformat(), "count": row.author_count}
                for row in rows
            ]
        except Exception:
            logger.exception("Failed to query fetch date histogram")
        return []

    def get_fetch_age_distribution(self):
        """Return distribution of author data age in buckets."""
        query = f"""
        SELECT
            CASE
                WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) THEN '< 1 day'
                WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) THEN '1-7 days'
                WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) THEN '7-30 days'
                WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY) THEN '30-90 days'
                WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY) THEN '90-180 days'
                WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY) THEN '180-365 days'
                ELSE '> 1 year'
            END AS age_bucket,
            COUNT(*) AS author_count
        FROM {Config.bq_raw('author_latest')}
        GROUP BY age_bucket
        ORDER BY MIN(timestamp) DESC
        """
        try:
            return [
                {"bucket": row.age_bucket, "count": row.author_count}
                for row in self.bq.query(query).result()
            ]
        except Exception:
            logger.exception("Failed to query fetch age distribution")
        return []

    def get_error_authors_sample(self, limit=10):
        """Return a sample of recent errored authors."""
        query = f"""
        SELECT
            CASE
              WHEN ENDS_WITH(document_id, '.json')
              THEN SUBSTR(document_id, 1, LENGTH(document_id) - 5)
              ELSE document_id
            END AS scholar_id,
            JSON_EXTRACT_SCALAR(data, '$.error') AS error,
            timestamp
        FROM {Config.bq_raw('author_latest')}
        WHERE JSON_EXTRACT_SCALAR(data, '$.error') IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT {int(limit)}
        """
        try:
            return [
                {
                    "scholar_id": row.scholar_id,
                    "error": row.error,
                    "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                }
                for row in self.bq.query(query).result()
            ]
        except Exception:
            logger.exception("Failed to query error authors")
        return []

    # ------------------------------------------------------------------
    # Cloud Tasks metrics
    # ------------------------------------------------------------------

    def get_queue_stats(self):
        """Return task counts for each Cloud Tasks queue."""
        queue_names = [
            "process-authors",
            "process-pubs",
            "cache-priority",
            "cache-batch",
        ]
        location = Config.QUEUE_LOCATION
        project = Config.PROJECT_ID
        stats = {}

        for name in queue_names:
            queue_path = (
                f"projects/{project}/locations/{location}/queues/{name}"
            )
            try:
                queue = self.tasks_client.get_queue(name=queue_path)
                # queue.stats provides approximate task counts
                task_count = None
                if queue.stats:
                    task_count = queue.stats.tasks_count
                stats[name] = {
                    "state": queue.state.name if hasattr(queue.state, "name") else str(queue.state),
                    "task_count": task_count,
                    "rate_limits": {
                        "max_dispatches_per_second": queue.rate_limits.max_dispatches_per_second,
                        "max_concurrent_dispatches": queue.rate_limits.max_concurrent_dispatches,
                    } if queue.rate_limits else None,
                }
            except Exception as e:
                stats[name] = {"state": "ERROR", "task_count": None, "rate_limits": None, "error": str(e)}

        return stats

    # ------------------------------------------------------------------
    # Aggregated dashboard
    # ------------------------------------------------------------------

    def get_dashboard_data(self):
        """Gather all health metrics into a single dict."""
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "authors": self.get_author_stats(),
            "publications": self.get_publication_stats(),
            "fetch_histogram": self.get_fetch_date_histogram(),
            "age_distribution": self.get_fetch_age_distribution(),
            "error_authors": self.get_error_authors_sample(),
            "queues": self.get_queue_stats(),
        }
