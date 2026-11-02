"""Author Search Service configuration."""

import os

from shared.bq_helpers import bq_view as _bq_view


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    # "s2" or "gs" (Google Scholar profiles). In GS mode the index covers every
    # profiled author and there is no external name-search fallback: GS author
    # search requires sign-in, so users paste a profile URL/ID instead.
    DATA_SOURCE = os.environ.get("DATA_SOURCE", "s2").lower()
    IS_GS = DATA_SOURCE == "gs"
    BQ_STATS_DATASET = os.environ.get("BQ_STATS_DATASET", "gs_statistics" if IS_GS else "statistics")

    # Firestore cache collection for search results
    # Separate collections per data source so switching DATA_SOURCE never
    # serves the other source's IDs.
    CACHE_COLLECTION = "v3_gs_author_search" if IS_GS else "v3_author_search"
    INDEX_COLLECTION = "v3_gs_author_name_index" if IS_GS else "v3_author_name_index"

    # Search cache freshness
    CACHE_TTL_HOURS = 24

    # Semantic Scholar API (fallback for authors not in the in-memory index)
    S2_API_KEY_SECRET = os.environ.get(
        "S2_API_KEY_SECRET",
        "projects/875626982900/secrets/s2-api-key/versions/latest",
    )
    S2_TIMEOUT_SECONDS = int(os.environ.get("S2_TIMEOUT_SECONDS", "10"))
    # Live S2 API name-search fallback. Off by default since 2026-11-02 (no new
    # Semantic Scholar fetches); set S2_SEARCH_FALLBACK=true to re-enable.
    S2_SEARCH_FALLBACK = os.environ.get("S2_SEARCH_FALLBACK", "false").lower() in ("true", "1", "yes")

    @classmethod
    def bq_view(cls, view_name):
        return _bq_view(cls.PROJECT_ID, cls.BQ_STATS_DATASET, view_name)
