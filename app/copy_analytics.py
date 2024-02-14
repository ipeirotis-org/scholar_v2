from google.cloud import bigquery
from google.cloud import firestore
from tqdm import tqdm
from data_access import get_firestore_cache, set_firestore_cache

# Initialize BigQuery client
bq_client = bigquery.Client()


# Initialize Firestore client
db = firestore.Client()

# Your BigQuery SQL query
QUERY = """
SELECT
  S.*, P.pip_auc_score, P.pip_auc_score_percentile
FROM
  `scholar-version2.statistics.author_stats` S
LEFT JOIN
  `scholar-version2.statistics.author_pip_scores` P
ON
  P.scholar_id = S.scholar_id
"""

# Run the query
query_job = bq_client.query(QUERY)
results = query_job.result()  # Waits for the job to complete
df = results.to_dataframe()
list_of_dicts = df.to_dict("records")

# Firestore collection where the results will be stored
collection_ref = db.collection("author_stats")

for row in tqdm(list_of_dicts):
    # Convert the row to a dictionary
    # Assume row.to_dict() is available or construct a dict manually if needed
    doc_data = dict(row)

    # Optional: Use a specific field as the document ID in Firestore
    # If you don't provide a document ID, Firestore generates one automatically
    doc_id = doc_data.get("scholar_id", None)

    # Store the document in Firestore
    set_firestore_cache("author_stats", doc_id, doc_data)
