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

    # Search budget. Keep a reserve for on-demand lookups so scheduled refreshes
    # never starve user requests. The deploy workflow sets the per-run cap for the
    # SerpApi plan in use (docs/architecture-gs-crawler.md, "Budget").
    SEARCH_RESERVE = int(os.environ.get("SERPAPI_SEARCH_RESERVE", "1000"))
    MAX_SEARCHES_PER_RUN = int(os.environ.get("MAX_SEARCHES_PER_RUN", "150"))
    # A run stops starting new profiles after this long. Snapshots are loaded into
    # BigQuery at the end of a run, so a run killed by the job's task timeout (3600 s)
    # would lose every search it paid for. A search takes ~3.2 s (2026-11).
    RUN_TIME_LIMIT_SECONDS = int(os.environ.get("RUN_TIME_LIMIT_SECONDS", "2700"))
    # The scheduled refresh leaves alone profiles crawled more recently than this.
    # Without it, a budget larger than the backlog would re-crawl week-old profiles.
    REFRESH_MIN_AGE_DAYS = int(os.environ.get("REFRESH_MIN_AGE_DAYS", "30"))
    # Legacy-seeded profiles where at least this share of works has no title (the seed
    # lacked titles for some authors, e.g. Kaiming He) are refreshed before stale ones.
    UNTITLED_MIN_SHARE = float(os.environ.get("UNTITLED_MIN_SHARE", "0.1"))
    # Share of each refresh run spent on authors we have never crawled, taken from
    # Wikidata in order of Wikipedia pageviews; unused share goes to refreshes.
    NEW_AUTHOR_SHARE = float(os.environ.get("NEW_AUTHOR_SHARE", "0.25"))
    # A failed crawl (deleted profile, stale Wikidata ID) is not retried for this long.
    FAILURE_COOLDOWN_DAYS = int(os.environ.get("FAILURE_COOLDOWN_DAYS", "30"))

    # GCS paths for raw vendor responses (kept for reprocessing)
    RAW_PREFIX = os.environ.get("GS_RAW_PREFIX", "gs_raw/serpapi/author/")

    # BigQuery
    BQ_DATASET = os.environ.get("BQ_GS_DATASET", "gs_data")
    AUTHOR_SNAPSHOTS_TABLE = "author_snapshots"
    PUBLICATION_SNAPSHOTS_TABLE = "publication_snapshots"
    AUTHORS_LATEST_VIEW = "authors_latest"
    PUBLICATIONS_LATEST_VIEW = "publications_latest"
    REFRESH_QUEUE_TABLE = "refresh_queue"
    CRAWL_FAILURES_TABLE = "crawl_failures"
    WIKIDATA_AUTHORS_TABLE = "wikidata_authors"  # GS IDs from Wikidata P1960, for name search
    BQ_STATS_DATASET = os.environ.get("BQ_GS_STATS_DATASET", "gs_statistics")

    @classmethod
    def bq_table(cls, table_name):
        return f"{cls.PROJECT_ID}.{cls.BQ_DATASET}.{table_name}"

    @classmethod
    def bq_table_ref(cls, table_name):
        """Backtick-quoted table reference for use in SQL."""
        return f"`{cls.bq_table(table_name)}`"

    @classmethod
    def stats_table_ref(cls, table_name):
        """Backtick-quoted gs_statistics view or table reference for use in SQL."""
        return f"`{cls.PROJECT_ID}.{cls.BQ_STATS_DATASET}.{table_name}`"
