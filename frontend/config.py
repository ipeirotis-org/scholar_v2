"""Frontend configuration with environment variable overrides."""

import os
import secrets

from region_health.config import AVAILABLE_FUNCTION_REGIONS  # noqa: F401
from region_health.config import CLOUD_FUNCTION_NAMES  # noqa: F401


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    BUCKET_NAME = os.environ.get("GCS_BUCKET", "scholar_data_share")
    BQ_DATASET = os.environ.get("BQ_DATASET", "scholar_raw_data")
    BQ_STATS_DATASET = os.environ.get("BQ_STATS_DATASET", "statistics")

    # Flask
    SECRET_KEY = os.environ.get("SECRET_KEY") or secrets.token_hex(32)
    GA_TRACKING_ID = os.environ.get("GA_TRACKING_ID", "G-P3R50RQS24")

    # Firestore cache collections
    CACHE_AUTHOR_PUB_STATS = "v3_author_pub_stats"
    CACHE_AUTHOR_STATS = "v3_author_stats"
    CACHE_PUB_STATS = "v3_pub_stats"
    CACHE_AUTHOR_TEMPORAL = "v3_author_temporal"
    CACHE_AUTHOR_FRESHNESS = "v3_author_freshness"

    # Refresh Cloud Functions base URL
    REFRESH_FUNCTIONS_BASE = os.environ.get(
        "REFRESH_FUNCTIONS_BASE",
        "https://us-central1-scholar-version2.cloudfunctions.net",
    )

    # Cloud Tasks
    QUEUE_LOCATION = os.environ.get("QUEUE_LOCATION", "northamerica-northeast1")
    QUEUE_NAME_CACHE_PRIORITY = os.environ.get("QUEUE_NAME_CACHE_PRIORITY", "cache-priority")
    QUEUE_NAME_CRAWL_PRIORITY = os.environ.get("QUEUE_NAME_CRAWL_PRIORITY", "process-authors-priority")

    # Crawler function URL (for enqueueing crawl tasks)
    # This is the base/fallback URL; get_rotating_crawl_url() rotates regions.
    CRAWL_FUNCTION_URL = os.environ.get(
        "CRAWL_FUNCTION_URL",
        "https://us-central1-scholar-version2.cloudfunctions.net/v3_fetch_author",
    )
    CRAWL_FUNCTION_NAME = os.environ.get("CRAWL_FUNCTION_NAME", "v3_fetch_author")

    # Cache Layer service (Component 7)
    CACHE_LAYER_URL = os.environ.get("CACHE_LAYER_URL", "")
    CLOUD_TASKS_SA_EMAIL = os.environ.get(
        "CLOUD_TASKS_SA_EMAIL",
        "875626982900-compute@developer.gserviceaccount.com",
    )

    # Author search function (Component 6 — stub until built)
    SEARCH_FUNCTION_URL = os.environ.get("SEARCH_FUNCTION_URL", "")

    # Stale data threshold in days
    STALE_DAYS = int(os.environ.get("STALE_DAYS", "90"))

    # CSV export
    ALL_AUTHORS_CSV_BLOB = "all_authors_stats.csv"

    @classmethod
    def get_rotating_crawl_url(cls):
        """Return a crawl function URL using health-weighted region selection.

        Distributes priority crawl tasks across all regions, favoring
        regions with lower error rates to avoid rate-limited regions.
        """
        if not cls.CRAWL_FUNCTION_URL:
            return ""
        from region_health.router import select_region
        region = select_region()
        return (
            f"https://{region}-{cls.PROJECT_ID}"
            f".cloudfunctions.net/{cls.CRAWL_FUNCTION_NAME}"
        )

    @classmethod
    def bq_view(cls, view_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_STATS_DATASET}.{view_name}`"

    @classmethod
    def bq_raw(cls, table_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_DATASET}.{table_name}`"
