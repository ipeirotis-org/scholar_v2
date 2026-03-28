"""Author Search Service configuration."""

import os


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    BQ_DATASET = os.environ.get("BQ_DATASET", "scholar_raw_data")
    BQ_STATS_DATASET = os.environ.get("BQ_STATS_DATASET", "statistics")
    BQ_S2_DATASET = os.environ.get("BQ_S2_DATASET", "s2_data")

    # Firestore cache collection for search results
    CACHE_COLLECTION = "v3_author_search"

    # Search tuning
    MAX_S2_UNIVERSE_RESULTS = 20
    CACHE_TTL_HOURS = 24  # Search cache freshness

    @classmethod
    def bq_view(cls, view_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_STATS_DATASET}.{view_name}`"

    @classmethod
    def bq_raw(cls, table_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_DATASET}.{table_name}`"

    @classmethod
    def bq_s2(cls, table_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_S2_DATASET}.{table_name}`"
