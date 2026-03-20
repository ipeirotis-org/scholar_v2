"""Refresh & Expand configuration with environment variable overrides."""

import os
from datetime import datetime, timezone


AVAILABLE_FUNCTION_REGIONS = [
    "us-central1",
    "us-east1",
    "us-east4",
    "us-east5",
    "us-west1",
    "us-west2",
    "us-west3",
    "us-west4",
    "us-south1",
]


def get_rotating_region(regions=None):
    """Select a region based on the current UTC day.

    Rotates daily: (hours_since_epoch // 24) % len(regions).
    """
    regions = regions or AVAILABLE_FUNCTION_REGIONS
    now_utc = datetime.now(timezone.utc)
    total_hours = int(now_utc.timestamp() // 3600)
    return regions[(total_hours // 24) % len(regions)]


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    BUCKET_NAME = os.environ.get("GCS_BUCKET", "scholar_data_share")

    # BigQuery
    BQ_DATASET = os.environ.get("BQ_DATASET", "scholar_raw_data")
    BQ_STATS_DATASET = os.environ.get("BQ_STATS_DATASET", "statistics")

    # Cloud Tasks
    QUEUE_LOCATION = os.environ.get("QUEUE_LOCATION", "northamerica-northeast1")
    QUEUE_NAME_AUTHORS = os.environ.get("QUEUE_NAME_AUTHORS", "process-authors")

    # Region rotation for function URLs
    FUNCTION_LOCATION = os.environ.get("FUNCTION_LOCATION") or get_rotating_region()

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
        """Construct the Cloud Function URL for the current region."""
        return (
            f"https://{cls.FUNCTION_LOCATION}-{cls.PROJECT_ID}"
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
