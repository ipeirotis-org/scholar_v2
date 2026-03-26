"""Crawler configuration with environment variable overrides."""

import os
from datetime import datetime, timezone

from region_health.config import AVAILABLE_FUNCTION_REGIONS  # noqa: F401


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    BUCKET_NAME = os.environ.get("GCS_BUCKET", "scholar_data_share")

    QUEUE_LOCATION = os.environ.get("QUEUE_LOCATION", "northamerica-northeast1")
    QUEUE_NAME_AUTHORS = os.environ.get("QUEUE_NAME_AUTHORS", "process-authors")
    QUEUE_NAME_PUBS = os.environ.get("QUEUE_NAME_PUBS", "process-pubs")
    QUEUE_NAME_PUBS_PRIORITY = os.environ.get("QUEUE_NAME_PUBS_PRIORITY", "process-pub-priority")

    CLOUD_TASKS_SA_EMAIL = os.environ.get(
        "CLOUD_TASKS_SA_EMAIL",
        "875626982900-compute@developer.gserviceaccount.com",
    )

    # Optional env var override pins to a specific region (e.g. for testing)
    _FUNCTION_LOCATION_OVERRIDE = os.environ.get("FUNCTION_LOCATION", "")

    SCHOLARLY_TIMEOUT = int(os.environ.get("SCHOLARLY_TIMEOUT", "300"))
    SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")
    PUB_ENQUEUE_DELAY = float(os.environ.get("PUB_ENQUEUE_DELAY", "0.1"))

    # Batch load function (for triggering immediate ingestion on priority crawls)
    BATCH_LOAD_FUNCTION = os.environ.get("BATCH_LOAD_FUNCTION", "v3_batch_load_gcs_to_bq")
    # Batch load is deployed only to us-central1
    BATCH_LOAD_LOCATION = os.environ.get("BATCH_LOAD_LOCATION", "us-central1")

    @classmethod
    def queue_path(cls, queue_name):
        return f"projects/{cls.PROJECT_ID}/locations/{cls.QUEUE_LOCATION}/queues/{queue_name}"

    @classmethod
    def function_url(cls, function_name):
        """Construct the Cloud Function URL using the function's own region.

        Uses the FUNCTION_LOCATION env var (set per-region at deploy time)
        so publication tasks target the same region as the author fetch.
        Falls back to health-weighted random selection for local dev.
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
    def batch_load_url(cls):
        """URL for the batch_load function (deployed only to us-central1)."""
        return (
            f"https://{cls.BATCH_LOAD_LOCATION}-{cls.PROJECT_ID}"
            f".cloudfunctions.net/{cls.BATCH_LOAD_FUNCTION}"
        )

    @classmethod
    def gcs_date_prefix(cls):
        now = datetime.now(timezone.utc)
        return now.strftime("%Y/%m/%d")

