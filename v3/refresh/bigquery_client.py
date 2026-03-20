"""BigQuery client for refresh & expand queries.

Queries the raw data tables to find stale authors, error authors,
and coauthors not yet in the database.
"""

import logging
import random

from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter

from v3.refresh.config import Config

logger = logging.getLogger(__name__)

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = bigquery.Client(project=Config.PROJECT_ID)
    return _client


def _query(sql, params=None):
    """Execute a parameterized BigQuery query and return rows as dicts."""
    client = _get_client()
    job_config = bigquery.QueryJobConfig()
    if params:
        job_config.query_parameters = params
    rows = client.query(sql, job_config=job_config).result()
    return [dict(row) for row in rows]


def get_stale_authors(limit=None):
    """Find authors whose data is oldest, ordered by last update ascending.

    Returns a list of scholar_id strings for authors not updated within
    STALE_THRESHOLD_DAYS.
    """
    limit = limit or Config.STALE_BATCH_SIZE
    sql = f"""
        SELECT
            document_id AS scholar_id,
            timestamp AS last_updated
        FROM {Config.bq_raw('author_latest')}
        WHERE timestamp < TIMESTAMP_SUB(
            CURRENT_TIMESTAMP(), INTERVAL @threshold_days DAY
        )
        ORDER BY timestamp ASC
        LIMIT @limit
    """
    params = [
        ScalarQueryParameter("threshold_days", "INT64", Config.STALE_THRESHOLD_DAYS),
        ScalarQueryParameter("limit", "INT64", limit),
    ]
    rows = _query(sql, params)
    logger.info("Found %d stale authors (threshold=%d days)", len(rows), Config.STALE_THRESHOLD_DAYS)
    return [r["scholar_id"] for r in rows]


def get_error_authors(limit=None):
    """Find authors with the most fetch errors, respecting a cooldown period.

    An 'error' author is one whose latest raw data record contains an
    error field. We skip authors that were already re-attempted within
    ERROR_COOLDOWN_HOURS to avoid retry loops.

    Returns a list of scholar_id strings.
    """
    limit = limit or Config.ERROR_BATCH_SIZE
    sql = f"""
        SELECT
            document_id AS scholar_id,
            timestamp AS last_updated
        FROM {Config.bq_raw('author_latest')}
        WHERE JSON_EXTRACT_SCALAR(data, '$.error') IS NOT NULL
          AND timestamp < TIMESTAMP_SUB(
              CURRENT_TIMESTAMP(), INTERVAL @cooldown_hours HOUR
          )
        ORDER BY timestamp ASC
        LIMIT @limit
    """
    params = [
        ScalarQueryParameter("cooldown_hours", "INT64", Config.ERROR_COOLDOWN_HOURS),
        ScalarQueryParameter("limit", "INT64", limit),
    ]
    rows = _query(sql, params)
    logger.info("Found %d error authors (cooldown=%dh)", len(rows), Config.ERROR_COOLDOWN_HOURS)
    return [r["scholar_id"] for r in rows]


def get_coauthors_to_add(limit=None):
    """Find coauthors not yet in the database, ranked by frequency.

    Oversamples by COAUTHOR_OVERSAMPLE_FACTOR and randomly selects
    to avoid always picking the same top coauthors.

    Returns a list of scholar_id strings.
    """
    limit = limit or Config.COAUTHOR_BATCH_SIZE
    oversample = max(limit * Config.COAUTHOR_OVERSAMPLE_FACTOR, 1)
    sql = f"""
        SELECT
            coauthor_scholar_id,
            coauthor_name,
            SUM(cnt) AS total
        FROM {Config.bq_view('coauthors_to_add')}
        GROUP BY coauthor_scholar_id, coauthor_name
        ORDER BY total DESC
        LIMIT @oversample
    """
    params = [
        ScalarQueryParameter("oversample", "INT64", oversample),
    ]
    rows = _query(sql, params)
    ids = list({r["coauthor_scholar_id"] for r in rows if r.get("coauthor_scholar_id")})

    if len(ids) <= limit:
        result = ids
    else:
        result = random.sample(ids, limit)

    logger.info("Selected %d coauthors to add (from %d candidates)", len(result), len(ids))
    return result


def author_exists(scholar_id):
    """Check whether an author exists in the raw data."""
    sql = f"""
        SELECT 1
        FROM {Config.bq_raw('author')}
        WHERE document_id = @scholar_id
        LIMIT 1
    """
    params = [ScalarQueryParameter("scholar_id", "STRING", scholar_id)]
    rows = _query(sql, params)
    return len(rows) > 0
