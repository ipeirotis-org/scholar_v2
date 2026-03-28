"""Author Search Service configuration."""

import os


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    BQ_STATS_DATASET = os.environ.get("BQ_STATS_DATASET", "statistics")

    # Firestore cache collection for search results
    CACHE_COLLECTION = "v3_author_search"

    # Search cache freshness
    CACHE_TTL_HOURS = 24

    # Semantic Scholar API (fallback for authors not in the in-memory index)
    S2_API_KEY_SECRET = os.environ.get(
        "S2_API_KEY_SECRET",
        "projects/875626982900/secrets/s2-api-key/versions/latest",
    )
    S2_TIMEOUT_SECONDS = int(os.environ.get("S2_TIMEOUT_SECONDS", "10"))

    @classmethod
    def bq_view(cls, view_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_STATS_DATASET}.{view_name}`"
