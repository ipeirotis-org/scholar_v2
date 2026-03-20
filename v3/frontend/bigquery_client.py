"""BigQuery client for frontend analytics queries.

All queries use parameterized SQL to prevent injection.
"""

import logging
from datetime import datetime

from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter

from v3.frontend.config import Config

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
                FROM {Config.bq_raw('pub')}
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

    def get_all_authors_stats(self):
        """Get all authors stats from materialized tables (for CSV export)."""
        sql = f"""
            SELECT S.*, P.pip_auc_score, P.pip_auc_score_percentile
            FROM {Config.bq_view('ranked_author_current_table')} S
            LEFT JOIN {Config.bq_view('ranked_author_pip_scores_current_table')} P
              ON P.scholar_id = S.scholar_id
        """
        return self._query(sql)

    def get_author_last_updated(self, scholar_id):
        """Get the latest timestamp for an author's data."""
        sql = f"""
            SELECT MAX(ts) AS last_updated FROM (
                SELECT MAX(timestamp) AS ts
                FROM {Config.bq_raw('author')}
                WHERE document_id = @scholar_id
                UNION ALL
                SELECT MAX(timestamp) AS ts
                FROM {Config.bq_raw('pub')}
                WHERE STARTS_WITH(document_id, @scholar_id_prefix)
            )
        """
        params = [
            ScalarQueryParameter("scholar_id", "STRING", scholar_id),
            ScalarQueryParameter("scholar_id_prefix", "STRING", f"{scholar_id}:"),
        ]
        df = self._query(sql, params)
        if df is None or df.empty or df.iloc[0]["last_updated"] is None:
            return None
        return df.iloc[0]["last_updated"]

    def author_exists(self, scholar_id):
        """Check whether an author exists in the raw data."""
        sql = f"""
            SELECT 1
            FROM {Config.bq_raw('author')}
            WHERE document_id = @scholar_id
            LIMIT 1
        """
        params = [ScalarQueryParameter("scholar_id", "STRING", scholar_id)]
        df = self._query(sql, params)
        return df is not None and not df.empty
