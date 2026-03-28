"""BigQuery queries for author search.

Searches local data: the in-memory name index (from ranked_author_current_table),
crawled authors in stats_author_current, and the full S2 authors universe.
"""

import logging

from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter

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

    def search_crawled_authors(self, name_pattern):
        """Search authors already in the statistics views by name.

        Returns authors from stats_author_current whose name matches
        the pattern (case-insensitive LIKE).
        """
        sql = f"""
            SELECT
                scholar_id,
                name,
                affiliation,
                '' AS email_domain,
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

    def search_s2_universe(self, name_pattern, limit=20):
        """Search S2 authors beyond those in our statistics views.

        Joins the S2 authors table with author_paper_stats to restrict the
        scan to authors with at least some activity (total_publications >= 1),
        avoiding a full scan of the 102M-row authors table. Results are
        ordered by citation count for relevance and cached by the caller.
        """
        sql = f"""
            SELECT
                CAST(a.authorid AS STRING) AS scholar_id,
                a.name,
                IFNULL(
                    JSON_EXTRACT_SCALAR(a.affiliations, '$[0]'),
                    ''
                ) AS affiliation,
                '' AS email_domain,
                IFNULL(a.citationcount, 0) AS citedby,
                IFNULL(a.hindex, 0) AS hindex
            FROM {Config.bq_s2('authors')} a
            INNER JOIN {Config.bq_s2('author_paper_stats')} s
                ON CAST(a.authorid AS STRING) = s.authorid
            WHERE LOWER(a.name) LIKE @pattern
                AND s.total_publications >= 3
            ORDER BY a.citationcount DESC
            LIMIT @limit
        """
        params = [
            ScalarQueryParameter("pattern", "STRING", f"%{name_pattern.lower()}%"),
            ScalarQueryParameter("limit", "INT64", limit),
        ]
        return self._query(sql, params)
