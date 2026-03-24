"""Read-only BigQuery client for cache layer queries.

All queries use parameterized SQL to prevent injection.
"""

import logging
from datetime import datetime

from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter

from cache_layer.config import Config

logger = logging.getLogger(__name__)


class BigQueryClient:
    def __init__(self, client=None):
        self.client = client or bigquery.Client(project=Config.PROJECT_ID)

    def _query(self, sql, params=None):
        """Execute a parameterized BigQuery query and return a DataFrame."""
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = params
        try:
            return self.client.query(sql, job_config=job_config).result().to_dataframe()
        except Exception:
            logger.exception("BigQuery query failed")
            return None

    def get_author_pub_stats(self, scholar_id):
        """Get per-publication PiP inputs with metadata for an author."""
        sql = f"""
            WITH pub_details AS (
                SELECT
                    JSON_EXTRACT_SCALAR(DATA, '$.data.author_pub_id') AS author_pub_id,
                    JSON_EXTRACT_SCALAR(DATA, '$.data.bib.title') AS title,
                    JSON_EXTRACT_SCALAR(DATA, '$.data.bib.citation') AS citation,
                    CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.bib.pub_year') AS INT64) AS pub_year,
                    CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.num_citations') AS INT64) AS num_citations
                FROM {Config.bq_raw('pub_latest')}
                WHERE JSON_EXTRACT_SCALAR(DATA, '$.data.author_pub_id') LIKE CONCAT(@scholar_id, ':%')
            )
            SELECT P.*, S.num_citations_percentile, S.publication_rank, S.num_papers_percentile
            FROM {Config.bq_view('stats_author_publication_pip_inputs_current')} S
            JOIN pub_details P ON P.author_pub_id = S.author_pub_id
            WHERE S.scholar_id = @scholar_id
            ORDER BY S.publication_rank
        """
        params = [ScalarQueryParameter("scholar_id", "STRING", scholar_id)]
        df = self._query(sql, params)
        if df is None:
            return []
        # Deduplicate by author_pub_id to guard against upstream view issues
        df = df.drop_duplicates(subset=["author_pub_id"], keep="first")
        return df.to_dict("records")

    def get_author_stats(self, scholar_id):
        """Get author metrics with percentiles and PiP-AUC score."""
        sql = f"""
            SELECT S.*, P.pip_auc_score, P.pip_auc_score_percentile
            FROM {Config.bq_view('ranked_author_current')} S
            LEFT JOIN {Config.bq_view('ranked_author_pip_scores_current')} P
              ON P.scholar_id = S.scholar_id
            WHERE S.scholar_id = @scholar_id
        """
        params = [ScalarQueryParameter("scholar_id", "STRING", scholar_id)]
        df = self._query(sql, params)
        if df is None or df.empty:
            return None
        return df.iloc[0].to_dict()

    def get_publication_stats(self, author_pub_id):
        """Get temporal citation stats for a publication."""
        current_year = datetime.now().year
        sql = f"""
            SELECT
              citation_year, age, yearly_citations, cumulative_citations,
              perc_pub_year_yearly_citations AS perc_yearly_citations,
              perc_pub_year_cumulative_citations AS perc_cumulative_citations
            FROM {Config.bq_view('ranked_publication_citations_temporal')}
            WHERE author_pub_id = @author_pub_id
              AND citation_year >= pub_year
              AND citation_year <= @current_year
            ORDER BY citation_year
        """
        params = [
            ScalarQueryParameter("author_pub_id", "STRING", author_pub_id),
            ScalarQueryParameter("current_year", "INT64", current_year),
        ]
        df = self._query(sql, params)
        if df is None:
            return []
        return df.to_dict("records")

    def get_author_temporal_stats(self, scholar_id):
        """Get temporal evolution of author metrics."""
        sql = f"""
            SELECT *
            FROM {Config.bq_view('ranked_author_metrics_temporal')}
            WHERE scholar_id = @scholar_id
            ORDER BY state_year ASC
        """
        params = [ScalarQueryParameter("scholar_id", "STRING", scholar_id)]
        df = self._query(sql, params)
        if df is None:
            return []
        return df.to_dict("records")

    def author_has_raw_pubs(self, scholar_id):
        """Check if the author has any raw publication data in the pub table."""
        sql = f"""
            SELECT 1
            FROM {Config.bq_raw('pub')}
            WHERE STARTS_WITH(document_id, @prefix_colon)
               OR STARTS_WITH(document_id, @prefix_underscore)
            LIMIT 1
        """
        params = [
            ScalarQueryParameter("prefix_colon", "STRING", f"{scholar_id}:"),
            ScalarQueryParameter("prefix_underscore", "STRING", f"{scholar_id}_"),
        ]
        df = self._query(sql, params)
        return df is not None and not df.empty

    def refresh_author_pubs(self, scholar_id):
        """Incrementally refresh pub_latest_table for a specific author.

        When publications are newly ingested but the daily materialization
        hasn't run yet, pub_latest_table is stale and analytics views return
        no data. This method updates just the rows for one author by:
        1. Deleting existing entries for the author
        2. Inserting fresh deduplicated+parsed entries from the raw pub table

        The DELETE+INSERT is wrapped in a transaction so a failed INSERT
        rolls back the DELETE, preventing data loss on partial failure.

        Returns the number of rows inserted, or -1 on failure.
        """
        prefix_colon = f"{scholar_id}:"
        prefix_underscore = f"{scholar_id}_"

        sql = f"""
            BEGIN TRANSACTION;

            DELETE FROM {Config.bq_raw('pub_latest_table')}
            WHERE scholar_id = @scholar_id;

            INSERT INTO {Config.bq_raw('pub_latest_table')}
            WITH raw_filtered AS (
                SELECT document_id, timestamp, data,
                    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY timestamp DESC) AS rn
                FROM {Config.bq_raw('pub')}
                WHERE STARTS_WITH(document_id, @prefix_colon)
                   OR STARTS_WITH(document_id, @prefix_underscore)
            ),
            parsed AS (
                SELECT
                    CASE
                        WHEN ENDS_WITH(document_id, '.json')
                        THEN SUBSTR(document_id, 1, LENGTH(document_id) - 5)
                        ELSE document_id
                    END AS document_id,
                    timestamp,
                    JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id') AS author_pub_id,
                    SPLIT(JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id'), ':')[SAFE_OFFSET(0)] AS scholar_id,
                    CAST(JSON_EXTRACT_SCALAR(data, '$.data.bib.pub_year') AS INT64) AS pub_year,
                    JSON_EXTRACT_SCALAR(data, '$.data.bib.title') AS title,
                    JSON_EXTRACT_SCALAR(data, '$.data.bib.author') AS author,
                    CAST(JSON_EXTRACT_SCALAR(data, '$.data.num_citations') AS INT64) AS num_citations,
                    JSON_QUERY(data, '$.data.cites_per_year') AS cites_per_year,
                    data
                FROM raw_filtered
                WHERE rn = 1
            ),
            deduped AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY author_pub_id ORDER BY timestamp DESC) AS rn2
                FROM parsed
            )
            SELECT document_id, timestamp, author_pub_id, scholar_id, pub_year,
                   title, author, num_citations, cites_per_year, data
            FROM deduped
            WHERE rn2 = 1;

            COMMIT TRANSACTION;
        """
        params = [
            ScalarQueryParameter("scholar_id", "STRING", scholar_id),
            ScalarQueryParameter("prefix_colon", "STRING", prefix_colon),
            ScalarQueryParameter("prefix_underscore", "STRING", prefix_underscore),
        ]
        try:
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            job = self.client.query(sql, job_config=job_config)
            job.result()
            # For multi-statement scripts, num_dml_affected_rows on the parent
            # job is unreliable (may be None/0). Sum rows from child jobs instead.
            rows = 0
            for child_job in self.client.list_jobs(parent_job=job.job_id):
                rows += child_job.num_dml_affected_rows or 0
            logger.info("Refreshed pub_latest_table for %s: %d rows", scholar_id, rows)
            return rows
        except Exception:
            logger.exception("Failed to refresh pub_latest_table for %s", scholar_id)
            return -1

    def get_author_freshness(self, scholar_id):
        """Check author existence and get last_updated in a single query.

        Returns (exists: bool, last_updated: datetime|None).
        Handles both document_id formats: 'SCHOLAR_ID' and 'SCHOLAR_ID.json'.
        """
        sql = f"""
            SELECT MAX(ts) AS last_updated FROM (
                SELECT MAX(timestamp) AS ts
                FROM {Config.bq_raw('author')}
                WHERE document_id IN (@scholar_id, @scholar_id_json)
                UNION ALL
                SELECT MAX(timestamp) AS ts
                FROM {Config.bq_raw('pub')}
                WHERE STARTS_WITH(document_id, @scholar_id_colon)
                   OR STARTS_WITH(document_id, @scholar_id_underscore)
            )
        """
        params = [
            ScalarQueryParameter("scholar_id", "STRING", scholar_id),
            ScalarQueryParameter("scholar_id_json", "STRING", f"{scholar_id}.json"),
            ScalarQueryParameter("scholar_id_colon", "STRING", f"{scholar_id}:"),
            ScalarQueryParameter("scholar_id_underscore", "STRING", f"{scholar_id}_"),
        ]
        df = self._query(sql, params)
        if df is None or df.empty or df.iloc[0]["last_updated"] is None:
            return False, None
        return True, df.iloc[0]["last_updated"]

    def get_recently_analyzed_authors(self, limit=20):
        """Get the most recently updated authors with their PiP-AUC scores."""
        sql = f"""
            SELECT S.scholar_id, S.name, S.affiliation,
                   S.hindex, S.citedby,
                   ROUND(MAX(P.pip_auc_score), 4) AS pip_auc_score,
                   ROUND(MAX(P.pip_auc_score_percentile), 4) AS pip_auc_percentile,
                   S.last_updated
            FROM {Config.bq_view('ranked_author_current_table')} S
            LEFT JOIN {Config.bq_view('ranked_author_pip_scores_current_table')} P
              ON P.scholar_id = S.scholar_id
            GROUP BY S.scholar_id, S.name, S.affiliation, S.hindex, S.citedby, S.last_updated
            ORDER BY S.last_updated DESC
            LIMIT @limit
        """
        params = [ScalarQueryParameter("limit", "INT64", limit)]
        df = self._query(sql, params)
        if df is None:
            return []
        return df.to_dict("records")

    def get_all_author_ids(self):
        """Get all author scholar_ids for full cache rebuild.

        Strips '.json' suffix from document_ids to normalize to scholar_id format.
        """
        sql = f"""
            SELECT DISTINCT
              CASE
                WHEN ENDS_WITH(document_id, '.json')
                THEN SUBSTR(document_id, 1, LENGTH(document_id) - 5)
                ELSE document_id
              END AS scholar_id
            FROM {Config.bq_raw('author')}
        """
        df = self._query(sql)
        if df is None:
            return []
        return df["scholar_id"].tolist()
