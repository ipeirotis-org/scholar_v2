"""Crawler configuration with environment variable overrides."""

import os
from datetime import datetime, timezone

from region_health.config import AVAILABLE_FUNCTION_REGIONS  # noqa: F401
from region_health.router import get_rotating_region  # noqa: F401


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    BUCKET_NAME = os.environ.get("GCS_BUCKET", "scholar_data_share")

    QUEUE_LOCATION = os.environ.get("QUEUE_LOCATION", "northamerica-northeast1")
    QUEUE_NAME_AUTHORS = os.environ.get("QUEUE_NAME_AUTHORS", "process-authors")
    QUEUE_NAME_PUBS = os.environ.get("QUEUE_NAME_PUBS", "process-pubs")

    # Optional env var override pins to a specific region (e.g. for testing)
    _FUNCTION_LOCATION_OVERRIDE = os.environ.get("FUNCTION_LOCATION", "")

    SCHOLARLY_TIMEOUT = int(os.environ.get("SCHOLARLY_TIMEOUT", "300"))
    PUB_ENQUEUE_DELAY = float(os.environ.get("PUB_ENQUEUE_DELAY", "0.1"))

    # Batch load function (for triggering immediate ingestion on priority crawls)
    BATCH_LOAD_FUNCTION = os.environ.get("BATCH_LOAD_FUNCTION", "v3_batch_load_gcs_to_bq")

    @classmethod
    def queue_path(cls, queue_name):
        return f"projects/{cls.PROJECT_ID}/locations/{cls.QUEUE_LOCATION}/queues/{queue_name}"

    @classmethod
    def function_url(cls, function_name):
        """Construct the Cloud Function URL, dynamically selecting a region.

        Uses the FUNCTION_LOCATION env var if set, otherwise selects a
        region via health-weighted random selection (per call, not at import time).
        """
        location = cls._FUNCTION_LOCATION_OVERRIDE
        if not location:
            from region_health.router import select_region
            location = select_region()
        return (
            f"https://{location}-{cls.PROJECT_ID}"
            f".cloudfunctions.net/{function_name}"
        )

    @classmethod
    def gcs_date_prefix(cls):
        now = datetime.now(timezone.utc)
        return now.strftime("%Y/%m/%d")
