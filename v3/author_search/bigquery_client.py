"""BigQuery queries for author search.

Searches local data (crawled authors + coauthor network) before
falling back to Google Scholar.
"""

import logging

from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter

from v3.author_search.config import Config

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

    def search_crawled_authors(self, name_pattern):
        """Search authors already in the database by name.

        Returns authors from stats_author_current whose name matches
        the pattern (case-insensitive LIKE).
        """
        sql = f"""
            SELECT
                scholar_id,
                name,
                affiliation,
                email_domain,
                citedby,
                hindex
            FROM {Config.bq_view('stats_author_current')}
            WHERE LOWER(name) LIKE @pattern
            ORDER BY citedby DESC
            LIMIT 20
        """
        params = [
            ScalarQueryParameter("pattern", "STRING", f"%{name_pattern.lower()}%"),
        ]
        return self._query(sql, params)

    def get_all_author_names(self):
        """Fetch all author names/IDs/affiliations for the in-memory index."""
        sql = f"""
            SELECT scholar_id, name, affiliation, citedby
            FROM {Config.bq_view('ranked_author_current_table')}
            ORDER BY name
        """
        return self._query(sql)

    def search_coauthor_network(self, name_pattern):
        """Search the coauthor network for authors not yet crawled.

        Returns coauthors whose name matches the pattern. These are
        authors known from coauthor lists but not yet in the database.
        """
        sql = f"""
            SELECT
                coauthor_scholar_id AS scholar_id,
                coauthor_name AS name,
                coauthor_affiliation AS affiliation,
                '' AS email_domain,
                0 AS citedby,
                0 AS hindex
            FROM {Config.bq_view('coauthors_to_add')}
            WHERE LOWER(coauthor_name) LIKE @pattern
            ORDER BY cnt DESC
            LIMIT 20
        """
        params = [
            ScalarQueryParameter("pattern", "STRING", f"%{name_pattern.lower()}%"),
        ]
        return self._query(sql, params)
