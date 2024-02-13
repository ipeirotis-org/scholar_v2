from queue_handler import refresh_authors
from google.cloud import bigquery

bq = bigquery.Client()

QUERY = '''

SELECT
  *,
  ABS(pubs_in_pubsdb - total_publications) / total_publications AS err
FROM
  `scholar-version2.firestore_views.debug_authors_missing_pubs_in_db`
WHERE
  -- ABS(pubs_in_pubsdb - total_publications) / total_publications = 1.0
  pubs_in_pubsdb = 0
ORDER BY
  ABS(pubs_in_pubsdb - total_publications) / total_publications DESC, total_publications DESC
LIMIT 100

'''
query_job = bq.query(QUERY)

results = query_job.result()  # Waits for the query to finish

df = results.to_dataframe()

refresh = df.scholar_id.values

refresh_authors(refresh, num_authors=refresh.shape[0])