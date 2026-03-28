"""Author Search Service configuration."""

import os


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    BQ_DATASET = os.environ.get("BQ_DATASET", "scholar_raw_data")
    BQ_STATS_DATASET = os.environ.get("BQ_STATS_DATASET", "statistics")

    # Firestore cache collection for search results
    CACHE_COLLECTION = "v3_author_search"

    # Search tuning
    MAX_S2_RESULTS = 10
    CACHE_TTL_HOURS = 24  # Search cache freshness

    # Semantic Scholar API
    S2_API_KEY_SECRET = os.environ.get(
        "S2_API_KEY_SECRET",
        "projects/875626982900/secrets/s2-api-key/versions/latest",
    )
    S2_TIMEOUT_SECONDS = int(os.environ.get("S2_TIMEOUT_SECONDS", "10"))

    @classmethod
    def bq_view(cls, view_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_STATS_DATASET}.{view_name}`"

    @classmethod
    def bq_raw(cls, table_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_DATASET}.{table_name}`"
