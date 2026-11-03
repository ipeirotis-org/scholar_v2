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
        """Fetch prominent S2 authors for the in-memory search index.

        Loads from the daily-materialized ranked_author_current_table,
        filtered to well-established researchers:
        - h-index >= 20
        - Total citations > 5,000
        This yields ~360K authors (~70MB in memory). For less-known
        researchers, the search service falls back to the Semantic
        Scholar API which covers all 102M S2 authors.
        """
        if Config.IS_GS:
            # Every profiled GS author (~16K), from the gs_statistics view.
            sql = f"""
                SELECT scholar_id, name, affiliation, citedby, hindex
                FROM {Config.bq_view('ranked_author_current')}
                ORDER BY name
            """
            profiled = self._query(sql)
            if not profiled:
                return []  # never publish an index of only unprofiled names
            return profiled + self.get_wikidata_author_names()
        sql = f"""
            SELECT scholar_id, name, affiliation, citedby, hindex
            FROM {Config.bq_view('ranked_author_current_table')}
            WHERE hindex >= 20
              AND citedby > 5000
            ORDER BY name
        """
        return self._query(sql)

    def get_wikidata_author_names(self):
        """Unprofiled researchers whose GS ID is on Wikidata (~130K), GS mode only.

        GS name search needs sign-in, so this is how the app finds people it has
        not crawled yet; the results page crawls them on demand. Returns [] if
        the table is missing or the query fails, leaving the profiled index intact.
        """
        sql = f"""
            SELECT w.scholar_id, w.name, w.description AS affiliation, w.sitelinks,
                   'wikidata' AS source
            FROM {Config.bq_gs_table(Config.WIKIDATA_AUTHORS_TABLE)} w
            WHERE w.scholar_id NOT IN (
              SELECT scholar_id FROM {Config.bq_view('ranked_author_current')})
            ORDER BY w.name
        """
        return self._query(sql)
