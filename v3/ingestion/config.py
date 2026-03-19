"""Ingestion pipeline configuration with environment variable overrides."""

import os


class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
    BUCKET_NAME = os.environ.get("GCS_BUCKET", "scholar_data_share")
    TEMP_BUCKET_NAME = os.environ.get("GCS_TEMP_BUCKET", BUCKET_NAME)

    BQ_DATASET = os.environ.get("BQ_DATASET", "scholar_raw_data")
    BQ_AUTHOR_TABLE = os.environ.get("BQ_AUTHOR_TABLE", "author")
    BQ_PUB_TABLE = os.environ.get("BQ_PUB_TABLE", "pub")

    SOURCE_AUTHORS_PREFIX = "authors_json/"
    ARCHIVE_AUTHORS_PREFIX = "authors_archive/"
    SOURCE_PUBS_PREFIX = "publications_json/"
    ARCHIVE_PUBS_PREFIX = "publications_archive/"
    DEAD_LETTER_PREFIX = "dead_letter/"
    TEMP_PREFIX = "bq_load_temp/"

    # Max files in a single NDJSON batch (controls peak memory)
    BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))
    # Max total files to process per entity type per invocation
    MAX_FILES_PER_RUN = int(os.environ.get("MAX_FILES_PER_RUN", "500"))

    @classmethod
    def bq_table_id(cls, table_name):
        return f"{cls.PROJECT_ID}.{cls.BQ_DATASET}.{table_name}"
