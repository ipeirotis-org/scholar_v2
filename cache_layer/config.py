"""Cache Layer configuration with environment variable overrides."""

import os


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")

    # BigQuery
    BQ_DATASET = os.environ.get("BQ_DATASET", "scholar_raw_data")
    BQ_STATS_DATASET = os.environ.get("BQ_STATS_DATASET", "statistics")

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
    # Set to "false" to fall back to live views during migration.
    USE_MATERIALIZED_TABLES = os.environ.get(
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
        "stats_author_publication_pip_inputs_current": "stats_author_publication_pip_inputs_current_table",
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
        return f"`{cls.PROJECT_ID}.{cls.BQ_STATS_DATASET}.{view_name}`"

    @classmethod
    def bq_raw(cls, table_name):
        return f"`{cls.PROJECT_ID}.{cls.BQ_DATASET}.{table_name}`"

    @classmethod
    def queue_path(cls, queue_name=None):
        queue_name = queue_name or cls.QUEUE_NAME_PRIORITY
        return (
            f"projects/{cls.PROJECT_ID}"
            f"/locations/{cls.QUEUE_LOCATION}"
            f"/queues/{queue_name}"
        )
