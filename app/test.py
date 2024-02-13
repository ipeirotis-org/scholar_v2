from queue_handler import refresh_authors
from google.cloud import bigquery

bq = bigquery.Client()

QUERY = '''

SELECT
  *
FROM
  `scholar-version2.firestore_views.debug_authors_missing_pubs_in_db`
WHERE
  pubs_in_pubsdb = 0
LIMIT 10

'''
query_job = bq.query(QUERY)

results = query_job.result()  # Waits for the query to finish

df = results.to_dataframe()

refresh = df.scholar_id.values

refresh_authors(refresh, num_authors=refresh.shape[0])