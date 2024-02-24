from google.cloud import bigquery
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

