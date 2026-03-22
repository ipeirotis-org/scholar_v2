"""Crawler configuration with environment variable overrides."""

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

    QUEUE_LOCATION = os.environ.get("QUEUE_LOCATION", "northamerica-northeast1")
    QUEUE_NAME_AUTHORS = os.environ.get("QUEUE_NAME_AUTHORS", "process-authors")
    QUEUE_NAME_PUBS = os.environ.get("QUEUE_NAME_PUBS", "process-pubs")

    FUNCTION_LOCATION = os.environ.get("FUNCTION_LOCATION") or get_rotating_region()

    SCHOLARLY_TIMEOUT = int(os.environ.get("SCHOLARLY_TIMEOUT", "300"))
    PUB_ENQUEUE_DELAY = float(os.environ.get("PUB_ENQUEUE_DELAY", "0.1"))

    # Batch load function (for triggering immediate ingestion on priority crawls)
    BATCH_LOAD_FUNCTION = os.environ.get("BATCH_LOAD_FUNCTION", "v3_batch_load_gcs_to_bq")

    @classmethod
    def queue_path(cls, queue_name):
        return f"projects/{cls.PROJECT_ID}/locations/{cls.QUEUE_LOCATION}/queues/{queue_name}"

    @classmethod
    def function_url(cls, function_name):
        return f"https://{cls.FUNCTION_LOCATION}-{cls.PROJECT_ID}.cloudfunctions.net/{function_name}"

    @classmethod
    def gcs_date_prefix(cls):
        now = datetime.now(timezone.utc)
        return now.strftime("%Y/%m/%d")
