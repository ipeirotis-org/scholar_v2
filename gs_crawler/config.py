"""Google Scholar crawler configuration with environment variable overrides."""

import os


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    BUCKET_NAME = os.environ.get("GCS_BUCKET", "scholar_data_share")

    # SerpApi (Google Scholar Author API)
    SERPAPI_BASE = "https://serpapi.com"
    SERPAPI_KEY_SECRET = os.environ.get(
        "SERPAPI_KEY_SECRET",
        "projects/875626982900/secrets/serpapi-key/versions/latest",
    )
    SERPAPI_TIMEOUT = int(os.environ.get("SERPAPI_TIMEOUT", "60"))
    SERPAPI_MAX_RETRIES = int(os.environ.get("SERPAPI_MAX_RETRIES", "2"))
    PAGE_SIZE = 100  # max articles per google_scholar_author request
    # Hard cap on profile pages per author (GS profiles top out around 3K papers).
    MAX_PAGES_PER_AUTHOR = int(os.environ.get("MAX_PAGES_PER_AUTHOR", "40"))

    # Search budget. The Developer plan has 5,000 searches/month; keep a reserve
    # for on-demand lookups so scheduled refreshes never starve user requests.
    SEARCH_RESERVE = int(os.environ.get("SERPAPI_SEARCH_RESERVE", "1000"))
    MAX_SEARCHES_PER_RUN = int(os.environ.get("MAX_SEARCHES_PER_RUN", "150"))

    # GCS paths for raw vendor responses (kept for reprocessing)
    RAW_PREFIX = os.environ.get("GS_RAW_PREFIX", "gs_raw/serpapi/author/")

    # BigQuery
    BQ_DATASET = os.environ.get("BQ_GS_DATASET", "gs_data")
    AUTHOR_SNAPSHOTS_TABLE = "author_snapshots"
    PUBLICATION_SNAPSHOTS_TABLE = "publication_snapshots"
    AUTHORS_LATEST_VIEW = "authors_latest"
    REFRESH_QUEUE_TABLE = "refresh_queue"

    @classmethod
    def bq_table(cls, table_name):
        return f"{cls.PROJECT_ID}.{cls.BQ_DATASET}.{table_name}"

    @classmethod
    def bq_table_ref(cls, table_name):
        """Backtick-quoted table reference for use in SQL."""
        return f"`{cls.bq_table(table_name)}`"
