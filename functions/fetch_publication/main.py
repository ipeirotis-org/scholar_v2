import functions_framework
import json
import logging
import time # For duration
import datetime # Already present
from flask import jsonify

from scholarly import scholarly
from scholarly.data_types import PublicationSource

from shared.services.storage_service import StorageService
from shared.config import Config
from shared.utils import convert_integers_to_strings

# --- Structured Logging Setup (similar to fetch_author) ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# (Optional: Add JSON formatter as shown in fetch_author example if not globally configured)
# --- End Structured Logging Setup ---

# Instantiate services
storage_service = StorageService()

@functions_framework.http
def fetch_publication(request):
    """HTTP Cloud Function to fill publication details from Google Scholar,
    cache them in Firestore, and save JSON to GCS."""
    start_time = time.time()
    request_json = request.get_json(silent=True) or {} # Ensure request_json is a dict

    pub_data_from_author = request_json.get("pub")
    author_pub_id_log = pub_data_from_author.get("author_pub_id", "unknown_author_pub_id") if isinstance(pub_data_from_author, dict) else "unknown_author_pub_id"
    log_extra = {"author_pub_id": author_pub_id_log, "requestId": request.headers.get("Function-Execution-Id")}

    if not pub_data_from_author or not isinstance(pub_data_from_author, dict) or "author_pub_id" not in pub_data_from_author:
        duration = time.time() - start_time
        logger.error("Invalid publication data provided.", extra={"custom_extra_fields": {**log_extra, "duration_seconds": duration, "outcome": "failure_bad_request", "received_pub_data": pub_data_from_author}})
        return jsonify({"error": "Missing or invalid 'pub' data"}), 400

    logger.info(f"Starting fetch_publication for {author_pub_id_log}", extra={"custom_extra_fields": {**log_extra, "status": "processing_started"}})

    try:
        filled_pub_details = process_publication(pub_data_from_author, log_extra) # Pass log_extra
        duration = time.time() - start_time
        log_extra_final = {**log_extra, "duration_seconds": duration}

        if filled_pub_details:
            logger.info(f"Successfully processed publication {author_pub_id_log}", extra={"custom_extra_fields": {**log_extra_final, "outcome": "success"}})
            return jsonify(filled_pub_details), 200
        else:
            logger.error(f"Failed to process publication {author_pub_id_log}", extra={"custom_extra_fields": {**log_extra_final, "outcome": "failure_processing"}})
            return jsonify({"error": "Failed to process publication"}), 500
    except Exception as e: # Catch any unexpected errors
        duration = time.time() - start_time
        logger.error(f"Unhandled exception in fetch_publication for {author_pub_id_log}: {e}", extra={"custom_extra_fields": {**log_extra, "duration_seconds": duration, "error_message": str(e), "outcome": "failure_unhandled_exception"}})
        return jsonify({"error": "Internal server error processing publication"}), 500


def process_publication(pub_data_from_author, parent_log_extra=None):
    """Fetches, serializes, (optionally) caches publication details to Firestore, and saves JSON to GCS."""
    processing_start_time = time.time()
    author_pub_id = pub_data_from_author["author_pub_id"] # Assumes valid at this point
    log_extra = {**(parent_log_extra or {}), "author_pub_id": author_pub_id} # Inherit and ensure author_pub_id

    logger.info(f"Processing publication details for {author_pub_id}", extra={"custom_extra_fields": log_extra})

    if 'source' not in pub_data_from_author:
         pub_data_from_author["source"] = PublicationSource.AUTHOR_PUBLICATION_ENTRY
    if 'container_type' not in pub_data_from_author:
        pub_data_from_author['container_type'] = 'Publication'

    try:
        # Fetch full publication details
        log_extra_scholarly = {**log_extra, "external_call": "scholarly.fill"}
        logger.debug(f"Calling scholarly.fill for {author_pub_id}", extra={"custom_extra_fields": log_extra_scholarly})
        detailed_pub = scholarly.fill(pub_data_from_author)
        logger.debug(f"scholarly.fill successful for {author_pub_id}", extra={"custom_extra_fields": log_extra_scholarly})
    except Exception as e:
        logger.error(f"Scholarly.fill failed for {author_pub_id}: {e}", extra={"custom_extra_fields": {**log_extra, "error_message": str(e), "detail": "scholarly_fill_failed"}})
        return None

    try:
        serialized_pub = convert_integers_to_strings(json.loads(json.dumps(detailed_pub)))
        logger.debug(f"Successfully serialized publication {author_pub_id}", extra={"custom_extra_fields": log_extra})
    except Exception as e:
        logger.error(f"Failed to serialize detailed publication {author_pub_id}: {e}", extra={"custom_extra_fields": {**log_extra, "error_message": str(e), "detail": "serialization_failed"}})
        return None

    # Save to Google Cloud Storage
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        sanitized_author_pub_id = author_pub_id.replace(":", "_").replace("/", "___")
        destination_blob_name = f"publications_json/{now.strftime('%Y/%m/%d')}/{sanitized_author_pub_id}.json"
        json_string_to_upload = json.dumps(serialized_pub)
        storage_service.upload_string_to_gcs(
            data_string=json_string_to_upload,
            destination_blob_name=destination_blob_name,
            content_type='application/json'
        )
        logger.info(f"Publication {author_pub_id} JSON data saved to GCS bucket {Config.BUCKET_NAME} at {destination_blob_name}.", extra={"custom_extra_fields": {**log_extra, "gcs_path": destination_blob_name}})
    except Exception as e:
        logger.error(f"Error saving publication {author_pub_id} JSON data to GCS: {e}", extra={"custom_extra_fields": {**log_extra, "error_message": str(e), "detail": "gcs_upload_failed"}})
        # if not success_firestore: # If Firestore also failed
        return None

    processing_duration = time.time() - processing_start_time
    logger.info(f"process_publication completed for {author_pub_id}", extra={"custom_extra_fields": {**log_extra, "process_publication_duration_seconds": processing_duration}})
    return serialized_pub