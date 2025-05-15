import functions_framework
import json
import logging
from flask import jsonify

from scholarly import scholarly
from scholarly.data_types import PublicationSource

# NEW IMPORTS for BigQuery
from google.cloud import bigquery
import datetime # For timestamp

from shared.config import Config
from shared.utils import convert_integers_to_strings
from shared.services.firestore_service import FirestoreService

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Instantiate services
firestore_service = FirestoreService()

# NEW: Initialize BigQuery client and define table ID
bigquery_client = bigquery.Client()
pub_table_id = "scholar-version2.scholar_raw_data.pub"


@functions_framework.http
def fill_publication(request):
    """HTTP Cloud Function to fill publication details from Google Scholar and cache them."""
    request_json = request.get_json(silent=True)

    # Validate input
    pub_data_from_author = request_json.get("pub") # Renamed to avoid confusion with 'detailed_pub'
    if not pub_data_from_author or not isinstance(pub_data_from_author, dict) or "author_pub_id" not in pub_data_from_author:
        logging.error("Invalid publication data provided.")
        return jsonify({"error": "Missing or invalid 'pub' data"}), 400

    try:
        filled_pub_details = process_publication(pub_data_from_author)
        if filled_pub_details:
            return jsonify(filled_pub_details), 200
        else:
            # process_publication now logs specific errors
            return jsonify({"error": "Failed to process publication"}), 500
    except Exception as e: # Catch any unexpected errors in the HTTP handler
        logging.error(f"Unhandled exception in fill_publication: {e}")
        return jsonify({"error": "Internal server error processing publication"}), 500


def process_publication(pub_data_from_author):
    """Fetches, serializes, and caches publication details."""
    author_pub_id = pub_data_from_author["author_pub_id"]
    logging.info(f"Processing publication details for {author_pub_id}")

    # Construct the publication object expected by scholarly.fill()
    # It needs more than just author_pub_id; it needs the structure from the author's publication list.
    # Ensure `pub_data_from_author` has the necessary structure that `scholarly.fill` expects.
    # If `pub_data_from_author` is already the dictionary from author['publications'], it might be okay.
    # Otherwise, you might need to pre-fill it or fetch a stub first.
    # Based on `search_author_id`, `pub` passed to `enqueue_publication_task` is an element of author['publications']
    # which should be suitable for `scholarly.fill`.

    # Set source for scholarly.fill, if it's not already in pub_data_from_author
    if 'source' not in pub_data_from_author:
         pub_data_from_author["source"] = PublicationSource.AUTHOR_PUBLICATION_ENTRY # Or other appropriate source if known
    if 'container_type' not in pub_data_from_author:
        pub_data_from_author['container_type'] = 'Publication'


    try:
        # Fetch full publication details
        detailed_pub = scholarly.fill(pub_data_from_author)
    except Exception as e:
        logging.error(f"Scholarly.fill failed for {author_pub_id}: {e}")
        return None # Indicate failure

    # Convert large integers to strings to avoid serialization issues
    # Ensure the full 'detailed_pub' is what you want to store.
    # Sometimes 'fill' might add very large unserializable objects if not handled carefully.
    try:
        serialized_pub = convert_integers_to_strings(json.loads(json.dumps(detailed_pub)))
    except Exception as e:
        logging.error(f"Failed to serialize detailed publication {author_pub_id}: {e}")
        return None # Indicate failure

    # Save to Firestore (existing logic)
    success_firestore = firestore_service.set_firestore_cache(
        Config.FIRESTORE_COLLECTION_PUB, author_pub_id, serialized_pub
    )
    if success_firestore:
        logging.info(
            f"Publication details for {author_pub_id} have been updated and cached in Firestore."
        )
    else:
        logging.error(f"Failed to store publication {author_pub_id} in Firestore.")
        # Decide if you want to return None or continue

    # NEW: Save to BigQuery
    try:
        document_id = author_pub_id
        timestamp_val = datetime.datetime.now(datetime.timezone.utc).isoformat()
        # 'serialized_pub' is already a dict from json.loads(json.dumps(detailed_pub))
        data_json_str = json.dumps(serialized_pub)


        merge_sql_pub = f"""
        MERGE `{pub_table_id}` T
        USING (SELECT '{document_id}' as document_id, TIMESTAMP('{timestamp_val}') as timestamp, JSON '{data_json_str}' as data) S
        ON T.document_id = S.document_id
        WHEN MATCHED THEN
          UPDATE SET T.timestamp = S.timestamp, T.data = S.data
        WHEN NOT MATCHED THEN
          INSERT (document_id, timestamp, data) VALUES(S.document_id, S.timestamp, S.data)
        """
        query_job = bigquery_client.query(merge_sql_pub)
        query_job.result()  # Wait for the job to complete
        logging.info(f"Publication {author_pub_id} data merged into BigQuery table {pub_table_id}.")
    except Exception as e:
        logging.error(f"Error merging publication {author_pub_id} data into BigQuery: {e}")
        # Optional: handle BigQuery write failure

    if not success_firestore: # If Firestore save failed initially
        return None

    return serialized_pub