"""Load S2 dataset files from GCS into BigQuery.

Handles schema definition, load job configuration, and derived table
materialization.
"""

import logging

from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery

from dataset_ingestion.config import Config

logger = logging.getLogger(__name__)

_bq_client = None


def _get_bq_client():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=Config.PROJECT_ID)
    return _bq_client


# ── BigQuery schemas matching the actual S2 JSONL record format ──────────

PAPERS_SCHEMA = [
    bigquery.SchemaField("corpusid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("externalids", "JSON"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("authors", "JSON"),  # [{authorId, name}]
    bigquery.SchemaField("venue", "STRING"),
    bigquery.SchemaField("publicationvenueid", "STRING"),
    bigquery.SchemaField("year", "INTEGER"),
    bigquery.SchemaField("referencecount", "INTEGER"),
    bigquery.SchemaField("citationcount", "INTEGER"),
    bigquery.SchemaField("influentialcitationcount", "INTEGER"),
    bigquery.SchemaField("isopenaccess", "BOOLEAN"),
    bigquery.SchemaField("s2fieldsofstudy", "JSON"),
    bigquery.SchemaField("publicationtypes", "JSON"),
    bigquery.SchemaField("publicationdate", "STRING"),
    bigquery.SchemaField("journal", "JSON"),
]

CITATIONS_SCHEMA = [
    bigquery.SchemaField("citationid", "INTEGER"),
    bigquery.SchemaField("citingcorpusid", "INTEGER"),
    bigquery.SchemaField("citedcorpusid", "INTEGER"),
    bigquery.SchemaField("isinfluential", "BOOLEAN"),
    bigquery.SchemaField("contexts", "JSON"),
    bigquery.SchemaField("intents", "JSON"),
]

AUTHORS_SCHEMA = [
    bigquery.SchemaField("authorid", "STRING"),
    bigquery.SchemaField("externalids", "JSON"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("aliases", "JSON"),
    bigquery.SchemaField("affiliations", "JSON"),
    bigquery.SchemaField("homepage", "STRING"),
    bigquery.SchemaField("papercount", "INTEGER"),
    bigquery.SchemaField("citationcount", "INTEGER"),
    bigquery.SchemaField("hindex", "INTEGER"),
]

DATASET_SCHEMAS = {
    "papers": PAPERS_SCHEMA,
    "citations": CITATIONS_SCHEMA,
    "authors": AUTHORS_SCHEMA,
}

# Clustering for base tables. Enables efficient per-entity lookups
# (e.g. WHERE corpusid = X or WHERE authorid = X) without full table scans.
DATASET_CLUSTERING = {
    "papers": ["corpusid"],
    "authors": ["authorid"],
}


def ensure_dataset_exists():
    """Create the s2_data BigQuery dataset if it doesn't exist."""
    client = _get_bq_client()
    dataset_ref = bigquery.DatasetReference(Config.PROJECT_ID, Config.BQ_DATASET)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)
    logger.info("BigQuery dataset %s ready", Config.BQ_DATASET)


def load_dataset(release_id, dataset_name, write_disposition="WRITE_TRUNCATE"):
    """Load a dataset from GCS into BigQuery.

    Args:
        release_id: S2 release ID (used to find GCS files).
        dataset_name: "papers", "citations", or "authors".
        write_disposition: WRITE_TRUNCATE for full load, WRITE_APPEND for incremental.

    Returns:
        Number of rows loaded.
    """
    schema = DATASET_SCHEMAS.get(dataset_name)
    if schema is None:
        raise ValueError(f"Unknown dataset: {dataset_name}")

    table_id = Config.bq_table(dataset_name)
    source_uri = Config.gcs_uri_pattern(release_id, dataset_name)

    logger.info("Loading %s from %s into %s", dataset_name, source_uri, table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=write_disposition,
        max_bad_records=100,  # tolerate some bad records
        ignore_unknown_values=True,  # S2 may add fields
    )

    # Cluster base tables for efficient per-entity lookups.
    # Without clustering, every per-author query scans the entire table
    # (e.g. 4.2 GB for authors, 22.7 GB for papers).
    clustering = DATASET_CLUSTERING.get(dataset_name)
    if clustering:
        job_config.clustering_fields = clustering

    client = _get_bq_client()
    load_job = client.load_table_from_uri(source_uri, table_id, job_config=job_config)

    logger.info("BigQuery load job %s started for %s", load_job.job_id, dataset_name)
    load_job.result()  # Wait for completion

    table = client.get_table(table_id)
    logger.info("Loaded %s: %d rows", dataset_name, table.num_rows)
    return table.num_rows


def build_paper_citations_by_year():
    """Materialize the paper_citations_by_year derived table.

    Joins citations with papers to get the year of each citing paper,
    then groups by (cited paper, citing year).
    """
    table_ref = Config.bq_table_ref(Config.PAPER_CITATIONS_BY_YEAR_TABLE)
    citations_ref = Config.bq_table_ref(Config.CITATIONS_TABLE)
    papers_ref = Config.bq_table_ref(Config.PAPERS_TABLE)

    sql = f"""
    CREATE OR REPLACE TABLE {table_ref}
    CLUSTER BY citedcorpusid
    AS
    WITH deduped_citations AS (
      SELECT DISTINCT citingcorpusid, citedcorpusid, isinfluential
      FROM {citations_ref}
      WHERE citedcorpusid IS NOT NULL
    )
    SELECT
      c.citedcorpusid,
      p.year AS citing_year,
      COUNT(*) AS citation_count,
      COUNTIF(c.isinfluential) AS influential_count
    FROM deduped_citations c
    JOIN {papers_ref} p ON c.citingcorpusid = p.corpusid
    WHERE p.year IS NOT NULL
      AND p.year > 1900
      AND p.year <= EXTRACT(YEAR FROM CURRENT_DATE())
    GROUP BY c.citedcorpusid, p.year
    """
    logger.info("Building paper_citations_by_year...")
    client = _get_bq_client()
    job = client.query(sql)
    job.result()
    table = client.get_table(Config.bq_table(Config.PAPER_CITATIONS_BY_YEAR_TABLE))
    logger.info("paper_citations_by_year: %d rows", table.num_rows)
    return table.num_rows


def build_author_paper_bridge():
    """Materialize the author_paper_bridge table.

    Flattens the authors array from papers into a (authorid, corpusid, pub_year)
    bridge table, clustered by authorid. This enables efficient per-author
    lookups without UNNEST-ing the entire papers table on every query.

    Used by base_author_publications view for predicate pushdown.
    """
    table_ref = Config.bq_table_ref(Config.AUTHOR_PAPER_BRIDGE_TABLE)
    papers_ref = Config.bq_table_ref(Config.PAPERS_TABLE)

    sql = f"""
    CREATE OR REPLACE TABLE {table_ref}
    CLUSTER BY authorid
    AS
    SELECT
      LAX_STRING(a.authorId) AS authorid,
      p.corpusid,
      p.year AS pub_year
    FROM {papers_ref} p,
         UNNEST(JSON_QUERY_ARRAY(p.authors)) AS a
    WHERE LAX_STRING(a.authorId) IS NOT NULL
      AND p.year IS NOT NULL
      AND p.year > 1900
      AND p.year <= EXTRACT(YEAR FROM CURRENT_DATE())
    """
    logger.info("Building author_paper_bridge...")
    client = _get_bq_client()
    job = client.query(sql)
    job.result()
    table = client.get_table(Config.bq_table(Config.AUTHOR_PAPER_BRIDGE_TABLE))
    logger.info("author_paper_bridge: %d rows", table.num_rows)
    return table.num_rows


def build_author_paper_stats():
    """Materialize the author_paper_stats derived table.

    Flattens the authors array from papers and computes per-author
    publication and citation statistics.
    """
    table_ref = Config.bq_table_ref(Config.AUTHOR_PAPER_STATS_TABLE)
    papers_ref = Config.bq_table_ref(Config.PAPERS_TABLE)

    sql = f"""
    CREATE OR REPLACE TABLE {table_ref}
    CLUSTER BY authorid
    AS
    WITH author_papers AS (
      SELECT
        LAX_STRING(a.authorId) AS authorid,
        p.corpusid,
        p.year,
        p.citationcount
      FROM {papers_ref} p,
           UNNEST(JSON_QUERY_ARRAY(p.authors)) AS a
      WHERE LAX_STRING(a.authorId) IS NOT NULL
        AND p.year IS NOT NULL
        AND p.year > 1900
        AND p.year <= EXTRACT(YEAR FROM CURRENT_DATE())
    )
    SELECT
      authorid,
      COUNT(*) AS total_publications,
      COUNTIF(citationcount > 0) AS total_publications_with_citations,
      COUNTIF(citationcount >= 10) AS i10_index,
      SUM(citationcount) AS total_citations,
      MIN(year) AS year_of_first_pub,
      MIN(IF(citationcount > 0, year, NULL)) AS year_of_first_cited_pub
    FROM author_papers
    GROUP BY authorid
    """
    logger.info("Building author_paper_stats...")
    client = _get_bq_client()
    job = client.query(sql)
    job.result()
    table = client.get_table(Config.bq_table(Config.AUTHOR_PAPER_STATS_TABLE))
    logger.info("author_paper_stats: %d rows", table.num_rows)
    return table.num_rows


def build_qualifying_papers():
    """Materialize the qualifying_papers derived table.

    Contains corpusids of papers that have at least one author with
    >= MIN_AUTHOR_PUBLICATIONS total publications. Used by stats and
    distribution views to filter the paper population for percentile
    calculations. Papers from excluded authors still contribute
    citations to other papers — only the percentile population is filtered.
    """
    table_ref = Config.bq_table_ref(Config.QUALIFYING_PAPERS_TABLE)
    bridge_ref = Config.bq_table_ref(Config.AUTHOR_PAPER_BRIDGE_TABLE)
    stats_ref = Config.bq_table_ref(Config.AUTHOR_PAPER_STATS_TABLE)
    min_pubs = Config.MIN_AUTHOR_PUBLICATIONS

    sql = f"""
    CREATE OR REPLACE TABLE {table_ref}
    CLUSTER BY corpusid
    AS
    SELECT DISTINCT b.corpusid
    FROM {bridge_ref} b
    JOIN {stats_ref} ps ON b.authorid = ps.authorid
    WHERE ps.total_publications >= {min_pubs}
    """
    logger.info(
        "Building qualifying_papers (min_pubs=%d)...", min_pubs
    )
    client = _get_bq_client()
    job = client.query(sql)
    job.result()
    table = client.get_table(Config.bq_table(Config.QUALIFYING_PAPERS_TABLE))
    logger.info("qualifying_papers: %d rows", table.num_rows)
    return table.num_rows


def log_release(release_id, dataset_name, load_type, status, rows_loaded=0):
    """Record a load operation in the release_log table."""
    table_id = Config.bq_table(Config.RELEASE_LOG_TABLE)
    client = _get_bq_client()

    # Ensure release_log table exists
    schema = [
        bigquery.SchemaField("release_id", "STRING"),
        bigquery.SchemaField("dataset_name", "STRING"),
        bigquery.SchemaField("load_type", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("rows_loaded", "INTEGER"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

    rows = [
        {
            "release_id": release_id,
            "dataset_name": dataset_name,
            "load_type": load_type,
            "status": status,
            "rows_loaded": rows_loaded,
            "timestamp": "AUTO",
        }
    ]
    # Use parameterized SQL to avoid injection
    sql = f"""
    INSERT INTO `{table_id}` (release_id, dataset_name, load_type, status, rows_loaded, timestamp)
    VALUES (@release_id, @dataset_name, @load_type, @status, @rows_loaded, CURRENT_TIMESTAMP())
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("release_id", "STRING", release_id),
            bigquery.ScalarQueryParameter("dataset_name", "STRING", dataset_name),
            bigquery.ScalarQueryParameter("load_type", "STRING", load_type),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("rows_loaded", "INT64", rows_loaded),
        ]
    )
    client.query(sql, job_config=job_config).result()
    logger.info("Logged release %s/%s: %s", release_id, dataset_name, status)


def get_last_loaded_release():
    """Get the most recent release where ALL datasets' latest status is 'success'.

    Uses the most recent log entry per (release_id, dataset_name) to determine
    each dataset's current status. This correctly handles re-runs: if a release
    was loaded successfully but a later re-run fails, the latest status is
    'failed' and the release is not considered complete.

    Returns None if no fully successful release has been recorded.
    """
    table_id = Config.bq_table(Config.RELEASE_LOG_TABLE)
    markers = Config.REQUIRED_SUCCESS_MARKERS
    required_count = len(markers)
    client = _get_bq_client()

    try:
        sql = f"""
        WITH latest_per_dataset AS (
          SELECT release_id, dataset_name, status, timestamp,
            ROW_NUMBER() OVER (
              PARTITION BY release_id, dataset_name
              ORDER BY timestamp DESC
            ) AS rn
          FROM `{table_id}`
          WHERE dataset_name IN ({', '.join(f"'{d}'" for d in markers)})
        )
        SELECT release_id
        FROM latest_per_dataset
        WHERE rn = 1 AND status = 'success'
        GROUP BY release_id
        HAVING COUNT(DISTINCT dataset_name) = {required_count}
        ORDER BY MAX(timestamp) DESC
        LIMIT 1
        """
        result = list(client.query(sql).result())
        if result:
            return result[0].release_id
    except google_exceptions.NotFound:
        logger.info("No release_log table found — assuming first run")
    return None
