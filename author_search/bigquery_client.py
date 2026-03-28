"""BigQuery queries for author search.

The primary search path is the in-memory index (loaded from
ranked_author_current_table via get_all_author_names). BigQuery
is only queried to refresh this index, not at search time.
"""

import logging

from google.cloud import bigquery

from author_search.config import Config

logger = logging.getLogger(__name__)


class BigQuerySearchClient:
    def __init__(self, client=None):
        self.client = client or bigquery.Client(project=Config.PROJECT_ID)

    def _query(self, sql, params=None):
        """Execute a parameterized BigQuery query and return list of dicts."""
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = params
        try:
            rows = self.client.query(sql, job_config=job_config).result()
            return [dict(row) for row in rows]
        except Exception:
            logger.exception("BigQuery search query failed")
            return []

    def get_all_author_names(self):
        """Fetch active S2 authors for the in-memory search index.

        Loads from the daily-materialized ranked_author_current_table,
        filtered to authors with meaningful activity:
        - h-index >= 10
        - Total citations > 500
        - More than 10 publications with citations
        This yields ~3M authors. For authors not in this index, the
        search service falls back to the Semantic Scholar API.
        """
        sql = f"""
            SELECT scholar_id, name, affiliation, citedby, hindex
            FROM {Config.bq_view('ranked_author_current_table')}
            WHERE hindex >= 10
              AND citedby > 500
              AND total_publications_with_citations > 10
            ORDER BY name
        """
        return self._query(sql)
