"""Persist crawl output: raw SerpApi pages to GCS, normalized rows to BigQuery."""

import json
import logging

from google.cloud import bigquery, storage

from gs_crawler.config import Config

logger = logging.getLogger(__name__)

_bq_client = None
_gcs_client = None

COAUTHOR_FIELDS = [
    bigquery.SchemaField("scholar_id", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("affiliation", "STRING"),
]

AUTHOR_SNAPSHOTS_SCHEMA = [
    bigquery.SchemaField("scholar_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("affiliation", "STRING"),
    bigquery.SchemaField("email_domain", "STRING"),
    bigquery.SchemaField("interests", "STRING", mode="REPEATED"),
    bigquery.SchemaField("citedby", "INTEGER"),
    bigquery.SchemaField("hindex", "INTEGER"),
    bigquery.SchemaField("i10index", "INTEGER"),
    bigquery.SchemaField("citedby_since", "INTEGER"),
    bigquery.SchemaField("hindex_since", "INTEGER"),
    bigquery.SchemaField("i10index_since", "INTEGER"),
    bigquery.SchemaField("since_year", "INTEGER"),
    bigquery.SchemaField("cites_per_year", "STRING"),
    bigquery.SchemaField("coauthors", "RECORD", mode="REPEATED", fields=COAUTHOR_FIELDS),
    bigquery.SchemaField("num_publications", "INTEGER"),
]

PUBLICATION_SNAPSHOTS_SCHEMA = [
    bigquery.SchemaField("scholar_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("author_pub_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("authors", "STRING"),
    bigquery.SchemaField("venue", "STRING"),
    bigquery.SchemaField("pub_year", "INTEGER"),
    bigquery.SchemaField("num_citations", "INTEGER"),
]


def _bq():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=Config.PROJECT_ID)
    return _bq_client


def _gcs():
    global _gcs_client
    if _gcs_client is None:
        _gcs_client = storage.Client(project=Config.PROJECT_ID)
    return _gcs_client


def raw_blob_path(scholar_id, fetched_at, start):
    """gs_raw/serpapi/author/YYYY/MM/DD/{scholar_id}/{fetched_at}_{start}.json"""
    day = fetched_at[:10].replace("-", "/")
    stamp = fetched_at.replace(":", "").replace("-", "")
    return f"{Config.RAW_PREFIX}{day}/{scholar_id}/{stamp}_{start:05d}.json"


def write_raw_pages(scholar_id, fetched_at, pages):
    bucket = _gcs().bucket(Config.BUCKET_NAME)
    for i, page in enumerate(pages):
        blob = bucket.blob(raw_blob_path(scholar_id, fetched_at, i * Config.PAGE_SIZE))
        # search_parameters carries no key; search_metadata URLs do not either.
        blob.upload_from_string(json.dumps(page), content_type="application/json")


def _load(rows, table_name, schema):
    if not rows:
        return 0
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = _bq().load_table_from_json(rows, Config.bq_table(table_name), job_config=job_config)
    job.result()
    return len(rows)


def load_snapshots(author_rows, publication_rows):
    """Append one batch of snapshots. Authors are written last so a reader never
    sees an author snapshot whose publications are missing."""
    n_pubs = _load(publication_rows, Config.PUBLICATION_SNAPSHOTS_TABLE, PUBLICATION_SNAPSHOTS_SCHEMA)
    n_auth = _load(author_rows, Config.AUTHOR_SNAPSHOTS_TABLE, AUTHOR_SNAPSHOTS_SCHEMA)
    logger.info("Loaded %d author and %d publication snapshot rows", n_auth, n_pubs)
    return n_auth, n_pubs
