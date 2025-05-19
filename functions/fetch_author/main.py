import functions_framework
import json
import logging
import copy
import time
import datetime # Already present

from shared.utils import convert_integers_to_strings
from shared.services.firestore_service import FirestoreService
from shared.services.task_queue_service import TaskQueueService
from shared.services.storage_service import StorageService
from shared.config import Config

# --- Structured Logging Setup ---
# Option 1: Basic JSON logging
# Configure logging to output JSON.
# You might use a custom formatter or a library like python-json-logger
# For a simple approach, you can structure your log messages as dicts.

# Get the root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Set default logging level

# If you have existing handlers, you might want to remove them
# to avoid duplicate logs or non-JSON logs.
# for handler in logger.handlers:
#     logger.removeHandler(handler)

# Add a handler that formats messages as JSON (simplified example)
# For more robust JSON logging, consider libraries like 'python-json-logger'.
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "severity": record.levelname,
            "message": record.getMessage(),
            "functionName": record.funcName,
            "fileName": record.filename,
            "lineNumber": record.lineno
        }
        # Add any extra fields from the log record
        if hasattr(record, 'custom_extra_fields'):
            log_record.update(record.custom_extra_fields)
        return json.dumps(log_record)

# Example: Adding a stream handler with JSON formatter
# In a real Cloud Function environment, logs typically go to stdout/stderr
# and are captured by Cloud Logging.
# json_handler = logging.StreamHandler()
# json_handler.setFormatter(JsonFormatter())
# logger.addHandler(json_handler)
# --- End Structured Logging Setup ---


# Instantiate services
firestore_service = FirestoreService()
task_queue_service = TaskQueueService()
storage_service = StorageService()

@functions_framework.http
def fetch_author(request):
    """Responds to HTTP requests with author information from Google Scholar,
    caches to Firestore, and saves JSON to GCS."""
    start_time = time.time()
    scholar_id = request.args.get("scholar_id") or (
        request.get_json(silent=True) or {}
    ).get("scholar_id")
    skip_pubs = request.args.get("skip_pubs") or (
        request.get_json(silent=True) or {}
    ).get("skip_pubs")

    log_extra = {"scholar_id": scholar_id, "skip_pubs": skip_pubs, "requestId": request.headers.get("Function-Execution-Id")} # Example of adding request ID

    if not scholar_id:
        duration = time.time() - start_time
        logger.error("Missing author id", extra={"custom_extra_fields": {**log_extra, "duration_seconds": duration, "outcome": "failure_bad_request"}})
        return jsonify({"error": "Missing author id"}), 400

    logger.info(f"Starting fetch_author for {scholar_id}", extra={"custom_extra_fields": {**log_extra, "status": "processing_started"}})

    author_info = process_author(scholar_id, skip_pubs, log_extra) # Pass log_extra for context

    duration = time.time() - start_time
    log_extra_final = {**log_extra, "duration_seconds": duration}

    if author_info is None:
        logger.error(f"Failed to fetch or process author data for {scholar_id}", extra={"custom_extra_fields": {**log_extra_final, "outcome": "failure_processing"}})
        return jsonify({"error": "Failed to fetch or process author data"}), 500

    logger.info(f"Successfully processed author {scholar_id}", extra={"custom_extra_fields": {**log_extra_final, "outcome": "success"}})
    return jsonify(author_info), 200


def process_author(scholar_id, skip_pubs=None, parent_log_extra=None): # Accept parent_log_extra
    """Fetches, processes, and stores an author's information and enqueues publications."""
    processing_start_time = time.time()
    log_extra = {**(parent_log_extra or {}), "scholar_id": scholar_id} # Inherit and add specific context

    author = fetch_author_from_scholar(scholar_id, log_extra) # Pass context
    if author is None:
        logger.error(f"No information returned from Google Scholar for author {scholar_id}.", extra={"custom_extra_fields": {**log_extra, "detail": "fetch_author_from_scholar_failed"}})
        return None

    serialized_author = serialize_author(author, log_extra) # Pass context
    if not serialized_author:
        logger.error(f"Failed to serialize author {scholar_id}.", extra={"custom_extra_fields": {**log_extra, "detail": "serialize_author_failed"}})
        return None

    # Firestore saving was commented out in the original, uncomment if used
    # success_firestore = author_repository.save_author(scholar_id, serialized_author)
    # if success_firestore:
    #     logger.info(f"Saved author {scholar_id} to Firestore.", extra={"custom_extra_fields": log_extra})
    # else:
    #     logger.error(f"Failed to store author {scholar_id} in Firestore.", extra={"custom_extra_fields": log_extra})
    #     # Depending on requirements, you might return None here

    # Save to Google Cloud Storage
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        destination_blob_name = f"authors_json/{now.strftime('%Y/%m/%d')}/{scholar_id}.json"
        json_string_to_upload = json.dumps(serialized_author)
        storage_service.upload_string_to_gcs(
            data_string=json_string_to_upload,
            destination_blob_name=destination_blob_name,
            content_type='application/json'
        )
        logger.info(f"Author {scholar_id} JSON data saved to GCS bucket {Config.BUCKET_NAME} at {destination_blob_name}.", extra={"custom_extra_fields": {**log_extra, "gcs_path": destination_blob_name}})
    except Exception as e:
        logger.error(f"Error saving author {scholar_id} JSON data to GCS: {e}", extra={"custom_extra_fields": {**log_extra, "error_message": str(e)}})
        # if not success_firestore: # If Firestore save also failed
        return None

    # if not success_firestore: # If Firestore save failed initially
    #      return None

    if skip_pubs is None:
        publications_to_enqueue = author.get("publications", [])
        if isinstance(publications_to_enqueue, list):
            enqueue_publications(publications_to_enqueue, scholar_id, log_extra) # Pass context
            logger.info(f"Enqueued {len(publications_to_enqueue)} publications for author {scholar_id}.", extra={"custom_extra_fields": {**log_extra, "publications_enqueued_count": len(publications_to_enqueue)}})
        else:
            logger.warning(f"No valid publications list found for author {scholar_id} to enqueue.", extra={"custom_extra_fields": log_extra})
    else:
        logger.info(f"Skipping publication enqueue for author {scholar_id} as per request.", extra={"custom_extra_fields": log_extra})

    processing_duration = time.time() - processing_start_time
    logger.info(f"process_author completed for {scholar_id}", extra={"custom_extra_fields": {**log_extra, "process_author_duration_seconds": processing_duration}})
    return serialized_author


def fetch_author_from_scholar(scholar_id, parent_log_extra=None):
    """Fetches detailed author data from Google Scholar."""
    log_extra = {**(parent_log_extra or {}), "scholar_id": scholar_id, "external_call": "scholarly.search_author_id"}
    logger.info(f"Fetching author entry from Google Scholar for {scholar_id}", extra={"custom_extra_fields": log_extra})
    try:
        author_data = scholarly.search_author_id(scholar_id)
        filled_author = scholarly.fill(
            author_data,
            sections=['basics', 'indices', 'counts', 'coauthors', 'publications']
        )
        logger.info(f"Successfully fetched and filled author data for {scholar_id} from Google Scholar.", extra={"custom_extra_fields": log_extra})
        return filled_author
    except Exception as e:
        logger.error(f"Error fetching author data from Google Scholar for {scholar_id}: {e}", extra={"custom_extra_fields": {**log_extra, "error_message": str(e)}})
        return None


def enqueue_publications(publications, author_id, parent_log_extra=None):
    """Enqueues tasks for processing each publication."""
    # log_extra_base = {**(parent_log_extra or {}), "author_id": author_id} # author_id is already in parent_log_extra if passed correctly
    log_extra_base = {**(parent_log_extra or {})}

    for i, pub in enumerate(publications):
        pub_id_for_log = pub.get('author_pub_id', f'unknown_pub_id_{i}')
        log_extra_pub = {**log_extra_base, "author_pub_id": pub_id_for_log, "action": "enqueue_publication_task"}
        time.sleep(0.1)
        if not task_queue_service.enqueue_publication_task(pub):
            logger.error(f"Failed to enqueue publication task for {pub_id_for_log}", extra={"custom_extra_fields": log_extra_pub})
        else:
            logger.debug(f"Successfully enqueued publication task for {pub_id_for_log}", extra={"custom_extra_fields": log_extra_pub})


def serialize_author(author, parent_log_extra=None):
    """Serializes author data for storage, simplifying publication list."""
    scholar_id = author.get('scholar_id', 'unknown_author')
    log_extra = {**(parent_log_extra or {}), "scholar_id_serialization": scholar_id}
    try:
        author_copy = copy.deepcopy(author)
        if 'publications' in author_copy and isinstance(author_copy['publications'], list):
            author_copy["publications"] = [
                {
                    "author_pub_id": p.get("author_pub_id"),
                    "num_citations": p.get("num_citations", 0),
                    "filled": False,
                    "bib": {
                        "title": p.get("bib", {}).get("title"),
                        "pub_year": p.get("bib", {}).get("pub_year")
                    }
                }
                for p in author_copy["publications"] if p.get("author_pub_id")
            ]
        else:
            author_copy["publications"] = []
        serialized = convert_integers_to_strings(
            json.loads(json.dumps(author_copy))
        )
        logger.debug(f"Successfully serialized author data for {scholar_id}", extra={"custom_extra_fields": log_extra})
        return serialized
    except Exception as e:
        logger.error(f"Error serializing author data for {scholar_id}: {e}", extra={"custom_extra_fields": {**log_extra, "error_message": str(e)}})
        return None