import functions_framework
import json
import logging
from flask import jsonify
from scholarly import scholarly
from scholarly.data_types import PublicationSource

# Import StorageService for GCS operations
from shared.services.storage_service import StorageService # MODIFIED
from shared.config import Config
from shared.utils import convert_integers_to_strings
from shared.services.firestore_service import FirestoreService

# datetime is needed for timestamping filenames or paths in GCS
import datetime # MODIFIED

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Instantiate services
firestore_service = FirestoreService()
storage_service = StorageService() # MODIFIED: Initialize StorageService

# BigQuery client and pub_table_id are no longer directly needed here for individual inserts
# bigquery_client = bigquery.Client() # REMOVED
# pub_table_id = "scholar-version2.scholar_raw_data.pub" # REMOVED

@functions_framework.http
def fill_publication(request):
    """HTTP Cloud Function to fill publication details from Google Scholar,
    cache them in Firestore, and save JSON to GCS."""
    request_json = request.get_json(silent=True)

    # Validate input
    pub_data_from_author = request_json.get("pub")
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
    """Fetches, serializes, caches publication details to Firestore, and saves JSON to GCS."""
    author_pub_id = pub_data_from_author["author_pub_id"]
    logging.info(f"Processing publication details for {author_pub_id}")

    # Construct the publication object expected by scholarly.fill()
    if 'source' not in pub_data_from_author:
         pub_data_from_author["source"] = PublicationSource.AUTHOR_PUBLICATION_ENTRY
    if 'container_type' not in pub_data_from_author:
        pub_data_from_author['container_type'] = 'Publication'

    try:
        # Fetch full publication details
        detailed_pub = scholarly.fill(pub_data_from_author)
    except Exception as e:
        logging.error(f"Scholarly.fill failed for {author_pub_id}: {e}")
        return None # Indicate failure

    try:
        # Convert large integers to strings
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
        # Depending on requirements, you might return None here if Firestore save is critical

    # MODIFIED: Save serialized_pub to Google Cloud Storage
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        # Sanitize author_pub_id for use as a filename
        sanitized_author_pub_id = author_pub_id.replace(":", "_").replace("/", "___")
        destination_blob_name = f"publications_json/{now.strftime('%Y/%m/%d')}/{sanitized_author_pub_id}.json"
        
        json_string_to_upload = json.dumps(serialized_pub)
        # Assuming 'upload_string_to_gcs' is implemented in your StorageService
        # to take a string, blob name, and content type.
        storage_service.upload_string_to_gcs(
            data_string=json_string_to_upload,
            destination_blob_name=destination_blob_name,
            content_type='application/json'
        )
        
        logging.info(f"Publication {author_pub_id} JSON data saved to GCS bucket {Config.BUCKET_NAME} at {destination_blob_name}.")

    except Exception as e:
        logging.error(f"Error saving publication {author_pub_id} JSON data to GCS: {e}")
        # If Firestore save failed and GCS save also fails, definitely return None
        if not success_firestore:
            return None
        # If Firestore succeeded but GCS failed, you might still want to return serialized_pub
        # or handle this as a partial success/failure based on your application's needs.
        # For now, we assume if Firestore succeeded, we still return the pub details.
        # However, logging the error is important for monitoring.

    # If Firestore save failed initially, and we decided not to proceed, this ensures None is returned.
    if not success_firestore:
        return None

    return serialized_pub