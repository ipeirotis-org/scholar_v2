"""Frontend configuration with environment variable overrides."""

import os
import secrets

from shared.bq_helpers import bq_raw as _bq_raw, bq_view as _bq_view


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

    # Cloud Tasks
    QUEUE_LOCATION = os.environ.get("QUEUE_LOCATION", "northamerica-northeast1")
    QUEUE_NAME_CACHE_PRIORITY = os.environ.get("QUEUE_NAME_CACHE_PRIORITY", "cache-priority")

    # Cache Layer service
    CACHE_LAYER_URL = os.environ.get("CACHE_LAYER_URL", "")
    CLOUD_TASKS_SA_EMAIL = os.environ.get(
        "CLOUD_TASKS_SA_EMAIL",
        "875626982900-compute@developer.gserviceaccount.com",
    )

    # Author search function
    SEARCH_FUNCTION_URL = os.environ.get("SEARCH_FUNCTION_URL", "")

    # CSV export
    ALL_AUTHORS_CSV_BLOB = "all_authors_stats.csv"

    @classmethod
    def bq_view(cls, view_name):
        return _bq_view(cls.PROJECT_ID, cls.BQ_STATS_DATASET, view_name)

    @classmethod
    def bq_raw(cls, table_name):
        return _bq_raw(cls.PROJECT_ID, cls.BQ_DATASET, table_name)
