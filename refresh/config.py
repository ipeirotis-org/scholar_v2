"""Refresh & Expand configuration with environment variable overrides."""

import os

from region_health.config import AVAILABLE_FUNCTION_REGIONS  # noqa: F401
from region_health.router import get_rotating_region  # noqa: F401


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    BUCKET_NAME = os.environ.get("GCS_BUCKET", "scholar_data_share")

    # BigQuery
    BQ_DATASET = os.environ.get("BQ_DATASET", "scholar_raw_data")
    BQ_STATS_DATASET = os.environ.get("BQ_STATS_DATASET", "statistics")

    # Cloud Tasks
    QUEUE_LOCATION = os.environ.get("QUEUE_LOCATION", "northamerica-northeast1")
    QUEUE_NAME_AUTHORS = os.environ.get("QUEUE_NAME_AUTHORS", "process-authors")
    QUEUE_NAME_CACHE_BATCH = os.environ.get("QUEUE_NAME_CACHE_BATCH", "cache-batch")

    # Cache Layer service
    CACHE_LAYER_URL = os.environ.get("CACHE_LAYER_URL", "")
    CLOUD_TASKS_SA_EMAIL = os.environ.get(
        "CLOUD_TASKS_SA_EMAIL",
        "875626982900-compute@developer.gserviceaccount.com",
    )

    # Optional env var override pins to a specific region (e.g. for testing)
    _FUNCTION_LOCATION_OVERRIDE = os.environ.get("FUNCTION_LOCATION", "")

    # Refresh policies
    STALE_THRESHOLD_DAYS = int(os.environ.get("STALE_THRESHOLD_DAYS", "90"))
    ERROR_COOLDOWN_HOURS = int(os.environ.get("ERROR_COOLDOWN_HOURS", "24"))
    COAUTHOR_BATCH_SIZE = int(os.environ.get("COAUTHOR_BATCH_SIZE", "1"))
    STALE_BATCH_SIZE = int(os.environ.get("STALE_BATCH_SIZE", "10"))
    ERROR_BATCH_SIZE = int(os.environ.get("ERROR_BATCH_SIZE", "5"))
    COAUTHOR_OVERSAMPLE_FACTOR = int(os.environ.get("COAUTHOR_OVERSAMPLE_FACTOR", "10"))

    @classmethod
    def queue_path(cls, queue_name=None):
        """Construct the full Cloud Tasks queue path."""
        queue_name = queue_name or cls.QUEUE_NAME_AUTHORS
        return (
            f"projects/{cls.PROJECT_ID}"
            f"/locations/{cls.QUEUE_LOCATION}"
            f"/queues/{queue_name}"
        )

    @classmethod
    def function_url(cls, function_name):
        """Construct the Cloud Function URL, dynamically selecting the healthiest region.

        Uses the FUNCTION_LOCATION env var if set, otherwise selects the
        best region based on health scores (updated per call, not at import time).
        """
        location = cls._FUNCTION_LOCATION_OVERRIDE
        if not location:
            from region_health.router import select_best_region
            location = select_best_region()
        return (
            f"https://{location}-{cls.PROJECT_ID}"
            f".cloudfunctions.net/{function_name}"
        )

    @classmethod
    def bq_raw(cls, table):
        """Fully qualified BigQuery raw data table/view reference."""
        return f"`{cls.PROJECT_ID}.{cls.BQ_DATASET}.{table}`"

    @classmethod
    def bq_view(cls, view):
        """Fully qualified BigQuery statistics view reference."""
        return f"`{cls.PROJECT_ID}.{cls.BQ_STATS_DATASET}.{view}`"
