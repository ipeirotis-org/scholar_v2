"""Health dashboard service — queries BigQuery, Cloud Tasks, and Cloud Monitoring for system metrics."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery, monitoring_v3, tasks_v2
from google.protobuf.duration_pb2 import Duration

# google.cloud.logging must be imported via importlib to avoid namespace
# collisions with the stdlib ``logging`` module in some environments.
import importlib as _il
_gcl = _il.import_module("google.cloud.logging")
LoggingClient = _gcl.Client

from frontend.config import AVAILABLE_FUNCTION_REGIONS, CLOUD_FUNCTION_NAMES, Config

logger = logging.getLogger(__name__)


class HealthService:
    """Gathers system health metrics from BigQuery and Cloud Tasks."""

    def __init__(self, bq_client=None, tasks_client=None,
                 monitoring_client=None, logging_client=None):
        self._bq = bq_client
        self._tasks = tasks_client
        self._monitoring = monitoring_client
        self._logging = logging_client

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

    @property
    def monitoring_client(self):
        if self._monitoring is None:
            self._monitoring = monitoring_v3.MetricServiceClient()
        return self._monitoring

    @property
    def logging_client(self):
        if self._logging is None:
            self._logging = LoggingClient(project=Config.PROJECT_ID)
        return self._logging

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

    def get_recent_fetched_authors(self, limit=10):
        """Return the most recently crawled/fetched authors."""
        query = f"""
        SELECT
            CASE
              WHEN ENDS_WITH(document_id, '.json')
              THEN SUBSTR(document_id, 1, LENGTH(document_id) - 5)
              ELSE document_id
            END AS scholar_id,
            JSON_EXTRACT_SCALAR(data, '$.name') AS name,
            JSON_EXTRACT_SCALAR(data, '$.affiliation') AS affiliation,
            timestamp
        FROM {Config.bq_raw('author_latest')}
        WHERE JSON_EXTRACT_SCALAR(data, '$.error') IS NULL
        ORDER BY timestamp DESC
        LIMIT {int(limit)}
        """
        try:
            return [
                {
                    "scholar_id": row.scholar_id,
                    "name": row.name,
                    "affiliation": row.affiliation,
                    "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                }
                for row in self.bq.query(query).result()
            ]
        except Exception:
            logger.exception("Failed to query recent fetched authors")
        return []

    def get_recent_analyzed_authors(self, limit=10):
        """Return the most recently analyzed authors with PiP-AUC scores."""
        query = f"""
        SELECT S.scholar_id, S.name, S.affiliation,
               S.hindex, S.citedby,
               ROUND(MAX(P.pip_auc_score), 4) AS pip_auc_score,
               ROUND(MAX(P.pip_auc_score_percentile), 4) AS pip_auc_percentile,
               S.last_updated
        FROM {Config.bq_view('ranked_author_current_table')} S
        LEFT JOIN {Config.bq_view('ranked_author_pip_scores_current_table')} P
          ON P.scholar_id = S.scholar_id
        GROUP BY S.scholar_id, S.name, S.affiliation, S.hindex, S.citedby, S.last_updated
        ORDER BY S.last_updated DESC
        LIMIT {int(limit)}
        """
        try:
            return [
                {
                    "scholar_id": row.scholar_id,
                    "name": row.name,
                    "affiliation": row.affiliation,
                    "hindex": row.hindex,
                    "citedby": row.citedby,
                    "pip_auc_score": float(row.pip_auc_score) if row.pip_auc_score is not None else None,
                    "pip_auc_percentile": float(row.pip_auc_percentile) if row.pip_auc_percentile is not None else None,
                    "last_updated": row.last_updated.isoformat() if row.last_updated else None,
                }
                for row in self.bq.query(query).result()
            ]
        except Exception:
            logger.exception("Failed to query recent analyzed authors")
        return []

    # ------------------------------------------------------------------
    # Cloud Tasks metrics
    # ------------------------------------------------------------------

    def get_queue_stats(self):
        """Return task counts for each Cloud Tasks queue."""
        queue_names = [
            "process-authors",
            "process-authors-priority",
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
                # Try to read queue.stats (may not exist in all client library versions)
                task_count = None
                try:
                    if hasattr(queue, "stats") and queue.stats:
                        task_count = queue.stats.tasks_count
                except Exception:
                    pass
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
    # Cloud Monitoring metrics (Cloud Functions execution)
    # ------------------------------------------------------------------

    _TIME_WINDOWS = {"1h": 1, "3h": 3, "24h": 24}

    def get_function_execution_stats(self):
        """Return execution counts per function, status, and region over 1h/3h/24h.

        Uses the built-in Cloud Functions metric
        ``cloudfunctions.googleapis.com/function/execution_count``.
        Returns ``{"totals": {func: {window: {ok,error,timeout,total}}},
                  "by_region": {region: {func: {ok,error,timeout,total}}}}``.
        """
        func_filter = " OR ".join(
            f'resource.labels.function_name = "{fn}"' for fn in CLOUD_FUNCTION_NAMES
        )
        metric_filter = (
            'metric.type = "cloudfunctions.googleapis.com/function/execution_count"'
            f" AND ({func_filter})"
        )
        project_name = f"projects/{Config.PROJECT_ID}"
        now = datetime.now(timezone.utc)

        # Per-window totals (aggregate across regions)
        totals = {fn: {} for fn in CLOUD_FUNCTION_NAMES}
        # Per-region detail for the 24h window
        by_region = {}

        try:
            for window_label, hours in self._TIME_WINDOWS.items():
                interval = monitoring_v3.TimeInterval(
                    start_time=now - timedelta(hours=hours),
                    end_time=now,
                )
                # Include region in grouping so we can build per-region data
                aggregation = monitoring_v3.Aggregation(
                    alignment_period=Duration(seconds=hours * 3600),
                    per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
                    cross_series_reducer=monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
                    group_by_fields=[
                        "resource.labels.function_name",
                        "metric.labels.status",
                        "resource.labels.region",
                    ],
                )
                request = monitoring_v3.ListTimeSeriesRequest(
                    name=project_name,
                    filter=metric_filter,
                    interval=interval,
                    aggregation=aggregation,
                    view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                )

                # Accumulate counts
                window_counts = {fn: defaultdict(int) for fn in CLOUD_FUNCTION_NAMES}
                region_counts = defaultdict(lambda: {fn: defaultdict(int) for fn in CLOUD_FUNCTION_NAMES})

                for ts in self.monitoring_client.list_time_series(request=request):
                    fn_name = ts.resource.labels.get("function_name", "")
                    status = ts.metric.labels.get("status", "unknown")
                    region = ts.resource.labels.get("region", "unknown")
                    value = sum(p.value.int64_value for p in ts.points)

                    if fn_name in window_counts:
                        window_counts[fn_name][status] += value
                    if window_label == "24h" and fn_name in CLOUD_FUNCTION_NAMES:
                        region_counts[region][fn_name][status] += value

                for fn_name in CLOUD_FUNCTION_NAMES:
                    counts = window_counts[fn_name]
                    ok = counts.get("ok", 0)
                    error = counts.get("error", 0)
                    timeout = counts.get("timeout", 0)
                    totals[fn_name][window_label] = {
                        "ok": ok,
                        "error": error,
                        "timeout": timeout,
                        "total": ok + error + timeout,
                    }

                if window_label == "24h":
                    for region in AVAILABLE_FUNCTION_REGIONS:
                        by_region[region] = {}
                        for fn_name in CLOUD_FUNCTION_NAMES:
                            rc = region_counts[region][fn_name]
                            ok = rc.get("ok", 0)
                            error = rc.get("error", 0)
                            timeout = rc.get("timeout", 0)
                            by_region[region][fn_name] = {
                                "ok": ok,
                                "error": error,
                                "timeout": timeout,
                                "total": ok + error + timeout,
                            }

            return {"totals": totals, "by_region": by_region}

        except Exception:
            logger.exception("Failed to query function execution stats")
        return None

    def get_function_error_breakdown(self):
        """Return HTTP status code breakdown for failed function requests over 1h/3h/24h.

        Queries Cloud Logging for non-200 responses, grouped by function name
        and HTTP status code (429 = rate-limited, 500 = permanent, 400 = bad input).
        Returns ``{func: {window: {status_code: count}}}``.
        """
        result = {fn: {} for fn in CLOUD_FUNCTION_NAMES}
        now = datetime.now(timezone.utc)

        try:
            for window_label, hours in self._TIME_WINDOWS.items():
                cutoff = now - timedelta(hours=hours)
                cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")

                for fn_name in CLOUD_FUNCTION_NAMES:
                    log_filter = (
                        f'resource.type="cloud_function"'
                        f' AND resource.labels.function_name="{fn_name}"'
                        f' AND httpRequest.status!=200'
                        f' AND httpRequest.status!=0'
                        f' AND timestamp>="{cutoff_str}"'
                    )
                    status_counts = defaultdict(int)
                    for entry in self.logging_client.list_entries(
                        filter_=log_filter,
                        page_size=1000,
                    ):
                        http_req = entry.http_request
                        if http_req and hasattr(http_req, "status"):
                            status_counts[http_req.status] += 1
                        elif hasattr(entry, "payload") and isinstance(entry.payload, dict):
                            # Fallback: check structured payload
                            code = entry.payload.get("httpRequest", {}).get("status")
                            if code:
                                status_counts[int(code)] += 1

                    result[fn_name][window_label] = dict(status_counts) if status_counts else {}

            return result

        except Exception:
            logger.exception("Failed to query function error breakdown")
        return None

    # ------------------------------------------------------------------
    # Aggregated dashboard
    # ------------------------------------------------------------------

    def get_dashboard_data(self):
        """Gather all health metrics into a single dict."""
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "authors": self.get_author_stats(),
            "publications": self.get_publication_stats(),
            "recent_fetched_authors": self.get_recent_fetched_authors(),
            "recent_analyzed_authors": self.get_recent_analyzed_authors(),
            "fetch_histogram": self.get_fetch_date_histogram(),
            "age_distribution": self.get_fetch_age_distribution(),
            "error_authors": self.get_error_authors_sample(),
            "queues": self.get_queue_stats(),
            "function_executions": self.get_function_execution_stats(),
            "function_errors": self.get_function_error_breakdown(),
        }
