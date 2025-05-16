import functions_framework
import json
import logging
import copy
import time
from flask import jsonify
from scholarly import scholarly

# datetime is used for GCS path generation and was already present
import datetime

from shared.utils import convert_integers_to_strings
from shared.services.firestore_service import FirestoreService
from shared.services.task_queue_service import TaskQueueService
# Import StorageService for GCS operations
from shared.services.storage_service import StorageService # ADDED
from shared.repositories.author_repository import AuthorRepository
from shared.repositories.publication_repository import PublicationRepository
from shared.config import Config # ADDED for BUCKET_NAME

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Instantiate services
firestore_service = FirestoreService()
task_queue_service = TaskQueueService()
storage_service = StorageService() # ADDED: Initialize StorageService

publication_repository = PublicationRepository(firestore_service)
author_repository = AuthorRepository(firestore_service, publication_repository)

# BigQuery client and author_table_id are no longer directly needed here for individual inserts
# from google.cloud import bigquery # REMOVED
# bigquery_client = bigquery.Client() # REMOVED
# author_table_id = "scholar-version2.scholar_raw_data.author" # REMOVED


@functions_framework.http
def fetch_author(request):
    """Responds to HTTP requests with author information from Google Scholar,
    caches to Firestore, and saves JSON to GCS."""
    scholar_id = request.args.get("scholar_id") or (
        request.get_json(silent=True) or {}
    ).get("scholar_id")
    skip_pubs = request.args.get("skip_pubs") or (
        request.get_json(silent=True) or {}
    ).get("skip_pubs")

    if not scholar_id:
        return jsonify({"error": "Missing author id"}), 400

    author_info = process_author(scholar_id, skip_pubs)
    if author_info is None:
        return jsonify({"error": "Failed to fetch or process author data"}), 500

    return jsonify(author_info), 200


def process_author(scholar_id, skip_pubs=None):
    """Fetches, processes, and stores an author's information and enqueues publications."""
    author = fetch_author_from_scholar(scholar_id)
    if author is None:
        logging.error(f"No information returned for author {scholar_id}.")
        return None

    serialized_author = serialize_author(author)
    if not serialized_author:
        logging.error(f"Failed to serialize author {scholar_id}.")
        return None

    # Save to Firestore (existing logic)
    success_firestore = author_repository.save_author(scholar_id, serialized_author)
    if success_firestore:
        logging.info(f"Saved author {scholar_id} to Firestore.")
    else:
        logging.error(f"Failed to store author {scholar_id} in Firestore.")
        # Depending on requirements, you might return None here

    # MODIFIED: Save serialized_author to Google Cloud Storage
    # This block replaces the direct BigQuery MERGE
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        # Sanitize scholar_id if necessary, though typically scholar_ids are GCS-path friendly
        destination_blob_name = f"authors_json/{now.strftime('%Y/%m/%d')}/{scholar_id}.json"
        
        json_string_to_upload = json.dumps(serialized_author)
        # Assuming 'upload_string_to_gcs' is implemented in your StorageService
        storage_service.upload_string_to_gcs(
            data_string=json_string_to_upload,
            destination_blob_name=destination_blob_name,
            content_type='application/json'
        )
        logging.info(f"Author {scholar_id} JSON data saved to GCS bucket {Config.BUCKET_NAME} at {destination_blob_name}.")

    except Exception as e:
        logging.error(f"Error saving author {scholar_id} JSON data to GCS: {e}")
        # If Firestore save failed and GCS save also fails, definitely return None
        if not success_firestore:
            return None
        # Handle GCS write failure. If Firestore succeeded, this is a partial failure.

    if not success_firestore: # If Firestore save failed initially
         return None

    if skip_pubs is None:
        # Ensure 'publications' key exists and is a list before trying to enqueue
        publications_to_enqueue = author.get("publications", [])
        if isinstance(publications_to_enqueue, list):
            enqueue_publications(publications_to_enqueue)
        else:
            logging.warning(f"No valid publications list found for author {scholar_id} to enqueue.")


    return serialized_author


def fetch_author_from_scholar(scholar_id):
    """Fetches detailed author data from Google Scholar."""
    try:
        logging.info(f"Fetching author entry from Google Scholar for {scholar_id}")
        # Ensure sections are filled, especially publications for enqueuing
        return scholarly.fill(
            scholarly.search_author_id(scholar_id),
            sections=['basics', 'indices', 'counts', 'coauthors', 'publications']
        )
    except Exception as e:
        logging.error(
            f"Error fetching author data from Google Scholar for {scholar_id}: {e}"
        )
        return None


def enqueue_publications(publications):
    """Enqueues tasks for processing each publication."""
    for pub in publications:
        # Small delay to avoid overwhelming the task queue service or downstream services
        time.sleep(0.1) 
        if not task_queue_service.enqueue_publication_task(pub):
            logging.error(
                f"Failed to enqueue publication task for {pub.get('author_pub_id')}"
            )


def serialize_author(author):
    """Serializes author data for storage, simplifying publication list."""
    try:
        # Use a deep copy to avoid modifying the original 'author' object,
        # especially if it's used later (e.g., for enqueue_publications)
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
            # If 'publications' is not a list or not present, set it to empty list
            author_copy["publications"] = []
            
        # Convert potentially large integers to strings
        # The structure should be serializable to JSON at this point
        serialized = convert_integers_to_strings(
            json.loads(json.dumps(author_copy)) # Ensures valid JSON structure before conversion
        )
        return serialized
    except Exception as e:
        logging.error(f"Error serializing author data for {author.get('scholar_id', 'unknown_author')}: {e}")
        return None