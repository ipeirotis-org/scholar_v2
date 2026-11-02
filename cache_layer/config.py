"""Cache Layer configuration with environment variable overrides."""

import os

from shared.bq_helpers import bq_raw as _bq_raw, bq_view as _bq_view


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")

    # Data source: "s2" (Semantic Scholar bulk data, statistics dataset) or
    # "gs" (Google Scholar profiles, gs_statistics dataset; profiles only, so
    # temporal metrics are unavailable). See docs/google-scholar-data-sources.md.
    DATA_SOURCE = os.environ.get("DATA_SOURCE", "s2").lower()
    IS_GS = DATA_SOURCE == "gs"

    # BigQuery
    BQ_DATASET = os.environ.get("BQ_DATASET", "scholar_raw_data")
    BQ_STATS_DATASET = os.environ.get("BQ_STATS_DATASET", "gs_statistics" if IS_GS else "statistics")
    BQ_GS_DATASET = os.environ.get("BQ_GS_DATASET", "gs_data")

    # GS mode: crawl unknown authors on demand via SerpApi (gs_crawler), but stop
    # once the account is down to this many searches.
    GS_ON_DEMAND_MIN_SEARCHES = int(os.environ.get("GS_ON_DEMAND_MIN_SEARCHES", "25"))

    # Cloud Tasks
    QUEUE_LOCATION = os.environ.get("QUEUE_LOCATION", "northamerica-northeast1")
    QUEUE_NAME_PRIORITY = os.environ.get("QUEUE_NAME_CACHE_PRIORITY", "cache-priority")
    QUEUE_NAME_BATCH = os.environ.get("QUEUE_NAME_CACHE_BATCH", "cache-batch")

    # Cache Layer's own Cloud Run URL (for self-enqueue in rebuild_all)
    CACHE_LAYER_URL = os.environ.get("CACHE_LAYER_URL", "")

    # Admin authentication — require shared secret for admin endpoints.
    # When set, admin requests must include Authorization: Bearer <token>.
    ADMIN_AUTH_TOKEN = os.environ.get("CACHE_LAYER_ADMIN_TOKEN", "")

    # Use materialized tables instead of live views for all queries.
    # Set to "false" only for fresh environments where _table objects
    # don't exist yet. Production should always use materialized tables
    # to avoid expensive live view computation on every cache-miss query.
    # gs_statistics has no _table counterparts (its views are cheap at ~16K authors).
    USE_MATERIALIZED_TABLES = not IS_GS and os.environ.get(
        "USE_MATERIALIZED_TABLES", "true"
    ).lower() in ("true", "1", "yes")

    # Firestore cache collection names (must match what the frontend reads)
    CACHE_AUTHOR_PUB_STATS = "v3_author_pub_stats"
    CACHE_AUTHOR_STATS = "v3_author_stats"
    CACHE_PUB_STATS = "v3_pub_stats"
    CACHE_AUTHOR_TEMPORAL = "v3_author_temporal"
    CACHE_AUTHOR_FRESHNESS = "v3_author_freshness"
    CACHE_RECENT_AUTHORS = "v3_recent_authors"

    # Views that have materialized _table counterparts.
    # When USE_MATERIALIZED_TABLES is True, queries use the _table version.
    _MATERIALIZED_VIEW_MAP = {
        # pip_inputs is NOT materialized — view works fine for per-author queries
        "ranked_author_current": "ranked_author_current_table",
        "ranked_author_pip_scores_current": "ranked_author_pip_scores_current_table",
        "ranked_publication_citations_temporal": "ranked_publication_citations_temporal_table",
        "ranked_author_metrics_temporal": "ranked_author_metrics_temporal_table",
        "stats_author_current": "stats_author_current_table",
        # These were already _table references before this change:
        "ranked_author_current_table": "ranked_author_current_table",
        "ranked_author_pip_scores_current_table": "ranked_author_pip_scores_current_table",
    }

    @classmethod
    def bq_view(cls, view_name):
        if cls.USE_MATERIALIZED_TABLES and view_name in cls._MATERIALIZED_VIEW_MAP:
            view_name = cls._MATERIALIZED_VIEW_MAP[view_name]
        return _bq_view(cls.PROJECT_ID, cls.BQ_STATS_DATASET, view_name)

    @classmethod
    def bq_gs(cls, table_name):
        return _bq_raw(cls.PROJECT_ID, cls.BQ_GS_DATASET, table_name)

    @classmethod
    def bq_raw(cls, table_name):
        return _bq_raw(cls.PROJECT_ID, cls.BQ_DATASET, table_name)

    @classmethod
    def queue_path(cls, queue_name=None):
        queue_name = queue_name or cls.QUEUE_NAME_PRIORITY
        return (
            f"projects/{cls.PROJECT_ID}"
            f"/locations/{cls.QUEUE_LOCATION}"
            f"/queues/{queue_name}"
        )
