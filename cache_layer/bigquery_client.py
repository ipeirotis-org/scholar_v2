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
        """Get per-publication PiP inputs with metadata for an author.

        Sources paper details (title, year, citations) from S2 papers table
        and PiP inputs (percentiles, rank) from the statistics views.
        """
        sql = f"""
            SELECT
                S.author_pub_id,
                p.title,
                CONCAT(
                    COALESCE(p.venue, ''),
                    CASE WHEN p.venue IS NOT NULL AND p.venue != '' THEN ', ' ELSE '' END,
                    CAST(p.year AS STRING)
                ) AS citation,
                p.year AS pub_year,
                CAST(p.citationcount AS INT64) AS num_citations,
                S.num_citations_percentile,
                S.publication_rank,
                S.num_papers_percentile
            FROM {Config.bq_view('stats_author_publication_pip_inputs_current')} S
            JOIN `{Config.PROJECT_ID}.s2_data.papers` p
                ON CAST(p.corpusid AS STRING) = S.author_pub_id
            WHERE S.scholar_id = @scholar_id
            ORDER BY S.publication_rank
        """
        params = [ScalarQueryParameter("scholar_id", "STRING", scholar_id)]
        df = self._query(sql, params)
        if df is None:
            return None
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

    def get_author_freshness(self, scholar_id):
        """Check author existence and get last_updated timestamp.

        Returns (exists: bool, last_updated: datetime|None).
        Queries the S2-backed stats view. The last_updated timestamp comes
        from the S2 author record (CURRENT_TIMESTAMP at view computation time).
        """
        sql = f"""
            SELECT last_updated
            FROM {Config.bq_view('stats_author_current')}
            WHERE scholar_id = @scholar_id
            LIMIT 1
        """
        params = [ScalarQueryParameter("scholar_id", "STRING", scholar_id)]
        df = self._query(sql, params)
        if df is not None and not df.empty:
            return True, df.iloc[0].get("last_updated")
        return False, None

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
        """Get all author IDs from S2 data for full cache rebuild.

        Returns author IDs from the active_authors benchmark population
        (hindex >= 3 AND total_publications >= 3) to avoid rebuilding
        cache for 100M+ low-activity authors.
        """
        sql = f"""
            SELECT CAST(authorid AS STRING) AS scholar_id
            FROM `{Config.PROJECT_ID}.s2_data.author_paper_stats`
            WHERE total_publications >= 3
              AND i10_index >= 1
        """
        df = self._query(sql)
        if df is None:
            return []
        return df["scholar_id"].tolist()
