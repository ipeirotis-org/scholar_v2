"""Author Search Service configuration."""

import os


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    BQ_DATASET = os.environ.get("BQ_DATASET", "scholar_raw_data")
    BQ_STATS_DATASET = os.environ.get("BQ_STATS_DATASET", "statistics")

    # Firestore cache collection for search results
    CACHE_COLLECTION = "v3_author_search"

    # Search tuning
    LOCAL_RESULTS_THRESHOLD = 5  # Skip Scholar fallback if local returns >= this many
    MAX_SCHOLAR_RESULTS = 10
    CACHE_TTL_HOURS = 24  # Scholar search cache freshness

    @classmethod
    def bq_view(cls, view_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_STATS_DATASET}.{view_name}`"

    @classmethod
    def bq_raw(cls, table_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_DATASET}.{table_name}`"
