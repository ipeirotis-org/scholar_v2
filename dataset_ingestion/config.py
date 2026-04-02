"""Dataset ingestion pipeline configuration with environment variable overrides."""

import os


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    BUCKET_NAME = os.environ.get("GCS_BUCKET", "scholar_data_share")

    # Semantic Scholar API
    S2_API_BASE = "https://api.semanticscholar.org/datasets/v1"
    S2_API_KEY_SECRET = os.environ.get(
        "S2_API_KEY_SECRET",
        "projects/875626982900/secrets/s2-api-key/versions/latest",
    )

    # GCS paths
    S2_DATASETS_PREFIX = "s2_datasets/"

    # BigQuery — separate dataset from existing scholar_raw_data
    BQ_DATASET = os.environ.get("BQ_S2_DATASET", "s2_data")

    # Table names
    PAPERS_TABLE = "papers"
    CITATIONS_TABLE = "citations"
    AUTHORS_TABLE = "authors"
    PAPER_CITATIONS_BY_YEAR_TABLE = "paper_citations_by_year"
    AUTHOR_PAPER_STATS_TABLE = "author_paper_stats"
    AUTHOR_PAPER_BRIDGE_TABLE = "author_paper_bridge"
    QUALIFYING_PAPERS_TABLE = "qualifying_papers"
    RELEASE_LOG_TABLE = "release_log"

    # Minimum total publications for an author to be included in detail tables
    # and for their papers to be included in percentile calculations.
    # Authors with fewer publications are excluded from PiP score computation
    # but their papers still contribute citations to other papers.
    MIN_AUTHOR_PUBLICATIONS = int(
        os.environ.get("MIN_AUTHOR_PUBLICATIONS", "6")
    )

    # Statistics dataset (analytics views and materialized tables)
    BQ_STATS_DATASET = os.environ.get("BQ_STATS_DATASET", "statistics")

    # Datasets to ingest (order matters: papers first for derived tables)
    DATASETS = ["papers", "citations", "authors"]

    # All dataset_name values that must have status='success' for a release
    # to be considered complete for diff-baseline purposes.
    # Note: materialized_tables is NOT included here because it's a
    # post-processing step. Including it would reject all pre-existing
    # releases that were loaded before materialization was introduced,
    # forcing an unnecessary full reload on the first auto-mode run.
    REQUIRED_SUCCESS_MARKERS = ["papers", "citations", "authors", "derived_tables"]

    # Download settings
    DOWNLOAD_WORKERS = int(os.environ.get("DOWNLOAD_WORKERS", "4"))
    DOWNLOAD_CHUNK_SIZE = 64 * 1024 * 1024  # 64 MB chunks for streaming

    @classmethod
    def bq_table(cls, table_name):
        return f"{cls.PROJECT_ID}.{cls.BQ_DATASET}.{table_name}"

    @classmethod
    def bq_table_ref(cls, table_name):
        """Backtick-quoted table reference for use in SQL."""
        return f"`{cls.bq_table(table_name)}`"

    @classmethod
    def gcs_dataset_prefix(cls, release_id, dataset_name):
        return f"{cls.S2_DATASETS_PREFIX}{release_id}/{dataset_name}/"

    @classmethod
    def gcs_uri_pattern(cls, release_id, dataset_name):
        return f"gs://{cls.BUCKET_NAME}/{cls.gcs_dataset_prefix(release_id, dataset_name)}*.gz"

    @classmethod
    def bq_stats_table(cls, table_name):
        return f"{cls.PROJECT_ID}.{cls.BQ_STATS_DATASET}.{table_name}"

    @classmethod
    def bq_stats_table_ref(cls, table_name):
        """Backtick-quoted statistics table reference for use in SQL."""
        return f"`{cls.bq_stats_table(table_name)}`"
