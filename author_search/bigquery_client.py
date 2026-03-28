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
        """Fetch all active S2 authors for the in-memory search index.

        Loads from the daily-materialized ranked_author_current_table,
        filtered to authors with meaningful activity:
        - More than 10 publications with citations
        - h-index >= 5
        This yields ~6.4M authors. The index is the sole data source
        for all author search — no BigQuery queries happen at search time.
        """
        sql = f"""
            SELECT scholar_id, name, affiliation, citedby, hindex
            FROM {Config.bq_view('ranked_author_current_table')}
            WHERE total_publications_with_citations > 10
              AND hindex >= 5
            ORDER BY name
        """
        return self._query(sql)
