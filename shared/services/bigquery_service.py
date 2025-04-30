from google.cloud import bigquery
from datetime import datetime
from ..config import Config  # Ensure this import matches your project structure


class BigQueryService:
    def __init__(self):
        self.client = bigquery.Client(project=Config.PROJECT_ID)

    def query(self, sql):
        query_job = self.client.query(sql)
        results = query_job.result()
        return results.to_dataframe()

    def get_author_pub_stats(self, author_id):
        sql = f"""
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
            FROM `scholar-version2.statistics.author_pub_stats` S
            JOIN pub_details P ON P.author_pub_id = S.author_pub_id
            WHERE S.scholar_id = '{author_id}'
            ORDER BY S.publication_rank
        """
        return self.query(sql).to_dict("records")

    def get_author_stats(self, author_id):
        sql = f"""
            SELECT S.*, P.pip_auc_score, P.pip_auc_score_percentile
            FROM `scholar-version2.statistics.author_stats` S
            LEFT JOIN `scholar-version2.statistics.author_pip_scores` P ON P.scholar_id = S.scholar_id
            WHERE S.scholar_id = '{author_id}'
        """
        df = self.query(sql)
        return df.to_dict("records")[0] if len(df) == 1 else None

    def get_all_authors_stats(self):
        sql = """
            SELECT S.*, P.pip_auc_score, P.pip_auc_score_percentile
            FROM `scholar-version2.statistics.author_stats` S
            LEFT JOIN `scholar-version2.statistics.author_pip_scores` P ON P.scholar_id = S.scholar_id
        """
        df = self.query(sql)
        return df

    def get_publication_stats(self, author_pub_id):
        current_year = datetime.now().year
        sql = f"""
            SELECT
              citation_year,
              age,
              yearly_citations,
              cumulative_citations,
              perc_pub_year_yearly_citations AS perc_yearly_citations,
              perc_pub_year_cumulative_citations AS perc_cumulative_citations
            FROM
              `scholar-version2.statistics.publication_citations`
            WHERE
              author_pub_id = '{author_pub_id}'
              AND citation_year >= pub_year
              AND citation_year <= {current_year}
            ORDER BY citation_year
        """
        df = self.query(sql).to_dict("records")
        return df
    
    def get_author_temporal_stats(self, author_id):
        """Fetches the temporal evolution of metrics and percentiles for an author."""
        if not author_id:
            logging.error("Author ID is required to fetch temporal stats.")
            return [] # Return empty list or None
    
        sql = f"""
            SELECT *
            FROM `scholar-version2.statistics.author_metrics_temporal_percentiles`
            WHERE scholar_id = '{author_id}'
            ORDER BY state_year ASC
        """
        try:
            df = self.query(sql)
            # Convert integer years to strings or datetime objects if needed downstream
            # Ensure data types are suitable for JSON serialization if caching
            return df.to_dict("records")
        except Exception as e:
            logging.error(f"Error fetching temporal stats for author {author_id}: {e}")
            return [] # Return empty list on error