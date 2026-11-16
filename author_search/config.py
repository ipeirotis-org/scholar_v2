"""Author Search Service configuration."""

import os

from shared.bq_helpers import bq_view as _bq_view


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    # "s2" or "gs" (Google Scholar profiles). In GS mode the index covers every
    # profiled author; names it does not have can be looked up live (GS_LIVE_SEARCH
    # below) or users paste a profile URL/ID.
    DATA_SOURCE = os.environ.get("DATA_SOURCE", "s2").lower()
    IS_GS = DATA_SOURCE == "gs"
    BQ_STATS_DATASET = os.environ.get("BQ_STATS_DATASET", "gs_statistics" if IS_GS else "statistics")
    # GS mode also indexes unprofiled researchers whose GS ID is on Wikidata or who
    # are listed as co-authors on a profiled author's page.
    BQ_GS_DATASET = os.environ.get("BQ_GS_DATASET", "gs_data")
    WIKIDATA_AUTHORS_TABLE = "wikidata_authors"
    AUTHORS_LATEST_VIEW = "authors_latest"  # co-author lists of profiled authors
    RETIRED_PROFILES_VIEW = "retired_profiles"  # profiles Google Scholar no longer serves

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

    # Live Google Scholar name lookup (GS mode, "scholar=true" searches): the "User
    # profiles" block of a regular Scholar search, through SerpApi. One paid search
    # per lookup, out of the same monthly plan as the crawler (which keeps a reserve
    # for on-demand requests), so lookups are cached and capped per day.
    GS_LIVE_SEARCH = os.environ.get("GS_LIVE_SEARCH", "true").lower() in ("true", "1", "yes")
    GS_LIVE_SEARCH_DAILY_LIMIT = int(os.environ.get("GS_LIVE_SEARCH_DAILY_LIMIT", "25"))
    GS_LIVE_CACHE_HOURS = int(os.environ.get("GS_LIVE_CACHE_HOURS", str(7 * 24)))
    GS_LIVE_TIMEOUT_SECONDS = int(os.environ.get("GS_LIVE_TIMEOUT_SECONDS", "20"))
    SERPAPI_KEY_SECRET = os.environ.get(
        "SERPAPI_KEY_SECRET",
        "projects/875626982900/secrets/serpapi-key/versions/latest",
    )

    @classmethod
    def bq_view(cls, view_name):
        return _bq_view(cls.PROJECT_ID, cls.BQ_STATS_DATASET, view_name)

    @classmethod
    def bq_gs_table(cls, table_name):
        return _bq_view(cls.PROJECT_ID, cls.BQ_GS_DATASET, table_name)
