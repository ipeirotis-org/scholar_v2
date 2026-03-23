"""Query Cloud Monitoring for per-region error rates, compute weights, persist to Firestore."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from region_health.config import (
    AVAILABLE_FUNCTION_REGIONS,
    CLOUD_FUNCTION_NAMES,
    DEFAULT_WEIGHT,
    ERROR_RATE_MULTIPLIER,
    HEALTH_COLLECTION,
    HEALTH_WINDOW_HOURS,
    MIN_WEIGHT,
)

logger = logging.getLogger(__name__)


def compute_weight(ok, error, timeout):
    """Compute a routing weight from execution counts.

    Returns a float in [MIN_WEIGHT, 1.0].
    """
    total = ok + error + timeout
    if total == 0:
        return DEFAULT_WEIGHT
    error_rate = (error + timeout) / total
    return max(MIN_WEIGHT, 1.0 - error_rate * ERROR_RATE_MULTIPLIER)


def _query_region_stats(monitoring_client, project_id):
    """Query Cloud Monitoring for per-region execution counts over the health window.

    Returns ``{region: {"ok": int, "error": int, "timeout": int}}``.
    """
    from google.cloud import monitoring_v3
    from google.protobuf.duration_pb2 import Duration

    func_filter = " OR ".join(
        f'resource.labels.function_name = "{fn}"' for fn in CLOUD_FUNCTION_NAMES
    )
    metric_filter = (
        'metric.type = "cloudfunctions.googleapis.com/function/execution_count"'
        f" AND ({func_filter})"
    )
    project_name = f"projects/{project_id}"
    now = datetime.now(timezone.utc)
    interval = monitoring_v3.TimeInterval(
        start_time=now - timedelta(hours=HEALTH_WINDOW_HOURS),
        end_time=now,
    )
    aggregation = monitoring_v3.Aggregation(
        alignment_period=Duration(seconds=HEALTH_WINDOW_HOURS * 3600),
        per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
        cross_series_reducer=monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
        group_by_fields=[
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

    region_counts = defaultdict(lambda: defaultdict(int))
    for ts in monitoring_client.list_time_series(request=request):
        status = ts.metric.labels.get("status", "unknown")
        region = ts.resource.labels.get("region", "unknown")
        value = sum(p.value.int64_value for p in ts.points)
        region_counts[region][status] += value

    return region_counts


def update_scores(monitoring_client=None, firestore_client=None, project_id="scholar-version2"):
    """Fetch monitoring data, compute weights, and write to Firestore.

    Can be called periodically by a background thread or triggered
    from the health dashboard.
    """
    if monitoring_client is None:
        from google.cloud import monitoring_v3
        monitoring_client = monitoring_v3.MetricServiceClient()
    if firestore_client is None:
        from google.cloud import firestore
        firestore_client = firestore.Client(project=project_id)

    try:
        region_stats = _query_region_stats(monitoring_client, project_id)
    except Exception:
        logger.exception("Failed to query Cloud Monitoring for region health")
        return

    now = datetime.now(timezone.utc)
    batch = firestore_client.batch()
    collection = firestore_client.collection(HEALTH_COLLECTION)

    for region in AVAILABLE_FUNCTION_REGIONS:
        counts = region_stats.get(region, {})
        ok = counts.get("ok", 0)
        error = counts.get("error", 0)
        timeout = counts.get("timeout", 0)
        total = ok + error + timeout
        error_rate = (error + timeout) / total if total > 0 else 0.0
        weight = compute_weight(ok, error, timeout)

        doc_ref = collection.document(region)
        batch.set(doc_ref, {
            "region": region,
            "ok": ok,
            "error": error,
            "timeout": timeout,
            "total": total,
            "error_rate": round(error_rate, 4),
            "weight": round(weight, 4),
            "window_hours": HEALTH_WINDOW_HOURS,
            "updated_at": now,
        })

    try:
        batch.commit()
        logger.info("Updated region health scores for %d regions", len(AVAILABLE_FUNCTION_REGIONS))
    except Exception:
        logger.exception("Failed to write region health scores to Firestore")
