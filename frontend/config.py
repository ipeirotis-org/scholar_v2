"""Frontend configuration with environment variable overrides."""

import os
import secrets


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

    # Refresh & Expand service (Component 5)
    REFRESH_SERVICE_URL = os.environ.get("REFRESH_SERVICE_URL", "")

    # Cloud Tasks (legacy — Component 5 now owns task enqueueing)
    QUEUE_LOCATION = os.environ.get("QUEUE_LOCATION", "northamerica-northeast1")
    QUEUE_NAME_AUTHORS = os.environ.get("QUEUE_NAME_AUTHORS", "process-authors")

    # Author search function (Component 6 — stub until built)
    SEARCH_FUNCTION_URL = os.environ.get("SEARCH_FUNCTION_URL", "")

    # Stale data threshold in days
    STALE_DAYS = int(os.environ.get("STALE_DAYS", "90"))

    # CSV export
    ALL_AUTHORS_CSV_BLOB = "all_authors_stats.csv"

    @classmethod
    def bq_view(cls, view_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_STATS_DATASET}.{view_name}`"

    @classmethod
    def bq_raw(cls, table_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_DATASET}.{table_name}`"
