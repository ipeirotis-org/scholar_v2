import logging 
from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter 
from datetime import datetime
import pandas as pd
from ..config import Config  


class BigQueryService:
    def __init__(self):
        self.client = bigquery.Client(project=Config.PROJECT_ID)

    def query(self, sql, query_params=None):
        """ Executes a BigQuery SQL query with optional parameters. """
        job_config = bigquery.QueryJobConfig()
        if query_params:
            job_config.query_parameters = query_params

        query_job = self.client.query(sql, job_config=job_config)
        try:
            results = query_job.result()  # Waits for the job to complete.
            return results.to_dataframe()
        except Exception as e:
            logging.error(f"BigQuery query failed: {e}")
            # We may want to raise the exception. 
            # Returning empty DF for now.
            return pd.DataFrame()


    def get_author_pub_stats(self, author_id):
        sql = """
            WITH pub_details AS ((
                SELECT
                    JSON_EXTRACT_SCALAR(DATA, '$.data.author_pub_id') AS author_pub_id,
                    JSON_EXTRACT_SCALAR(DATA, '$.data.bib.title') AS title,
                    JSON_EXTRACT_SCALAR(DATA, '$.data.bib.citation') AS citation,
                    CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.bib.pub_year') AS INT64) AS pub_year,
                    CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.num_citations') AS INT64) AS num_citations
                FROM `scholar-version2.firestore_export.scholar_raw_pub_raw_latest`
            ))
            SELECT P.*, S.num_citations_percentile, S.publication_rank, S.num_papers_percentile
            FROM `scholar-version2.statistics.stats_author_publication_pip_inputs_current` S
            JOIN pub_details P ON P.author_pub_id = S.author_pub_id
            WHERE S.scholar_id = @author_id
            ORDER BY S.publication_rank
        """
        query_params = [
            ScalarQueryParameter("author_id", "STRING", author_id)
        ]
        df = self.query(sql, query_params=query_params)
        return df.to_dict("records")

    def get_author_stats(self, author_id):
        sql = """
            SELECT S.*, P.pip_auc_score, P.pip_auc_score_percentile
            FROM `scholar-version2.statistics.stats_author_current` S
            LEFT JOIN `scholar-version2.statistics.stats_author_pip_scores_current` P ON P.scholar_id = S.scholar_id
            WHERE S.scholar_id = @author_id
        """
        query_params = [
            ScalarQueryParameter("author_id", "STRING", author_id)
        ]
        df = self.query(sql, query_params=query_params)
        # Original logic: return df.to_dict("records")[0] if len(df) == 1 else None
        # Safer logic:
        records = df.to_dict("records")
        if len(records) == 1:
            return records[0]
        elif len(records) > 1:
            logging.warning(f"Multiple author stats rows found for author_id: {author_id}. Returning first.")
            return records[0]
        else:
            logging.warning(f"No author stats found for author_id: {author_id}.")
            return None

    def get_all_authors_stats(self):
        # This query doesn't have external parameters, so no injection risk here.
        sql = """
            SELECT S.*, P.pip_auc_score, P.pip_auc_score_percentile
            FROM `scholar-version2.statistics.stats_author_current` S
            LEFT JOIN `scholar-version2.statistics.stats_author_pip_scores_current` P ON P.scholar_id = S.scholar_id
        """
        df = self.query(sql) # No query_params needed
        return df

    def get_publication_stats(self, author_pub_id):
        current_year = datetime.now().year
        sql = """
            SELECT
              citation_year,
              age,
              yearly_citations,
              cumulative_citations,
              perc_pub_year_yearly_citations AS perc_yearly_citations,
              perc_pub_year_cumulative_citations AS perc_cumulative_citations
            FROM
              `scholar-version2.statistics.stats_publication_citations_temporal`
            WHERE
              author_pub_id = @author_pub_id
              AND citation_year >= pub_year
              AND citation_year <= @current_year
            ORDER BY citation_year
        """
        query_params = [
            ScalarQueryParameter("author_pub_id", "STRING", author_pub_id),
            ScalarQueryParameter("current_year", "INT64", current_year)
        ]
        df = self.query(sql, query_params=query_params)
        return df.to_dict("records")

    def get_author_temporal_stats(self, author_id):
        """Fetches the temporal evolution of metrics and percentiles for an author."""
        if not author_id:
            logging.error("Author ID is required to fetch temporal stats.")
            return []  # Return empty list or None

        sql = """
            SELECT *
            FROM `scholar-version2.statistics.stats_author_metrics_temporal`
            WHERE scholar_id = @author_id
            ORDER BY state_year ASC
        """
        query_params = [
            ScalarQueryParameter("author_id", "STRING", author_id)
        ]
        try:
            df = self.query(sql, query_params=query_params)
            # Convert integer years to strings or datetime objects if needed downstream
            # Ensure data types are suitable for JSON serialization if caching
            return df.to_dict("records")
        except Exception as e:
            # Query function now logs the error, so just return empty list
            # logging.error(f"Error fetching temporal stats for author {author_id}: {e}")
            return []  # Return empty list on error