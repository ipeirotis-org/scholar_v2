import functions_framework
import json
import logging
import copy
import time
from flask import jsonify
from scholarly import scholarly

# NEW IMPORTS for BigQuery
from google.cloud import bigquery
import datetime # For timestamp

from shared.utils import convert_integers_to_strings
from shared.services.firestore_service import FirestoreService
from shared.services.task_queue_service import TaskQueueService
from shared.repositories.author_repository import AuthorRepository
from shared.repositories.publication_repository import PublicationRepository

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Instantiate services
firestore_service = FirestoreService()
task_queue_service = TaskQueueService()

publication_repository = PublicationRepository(firestore_service)
author_repository = AuthorRepository(firestore_service, publication_repository)

# NEW: Initialize BigQuery client and define table ID
bigquery_client = bigquery.Client()
author_table_id = "scholar-version2.scholar_raw_data.author"


@functions_framework.http
def search_author_id(request):
    """Responds to HTTP requests with author information from Google Scholar.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        flask.Response: HTTP response object.
    """
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
    """Fetches and processes an author's information and publications.
    Args:
        scholar_id (str): Google Scholar ID of the author.
    Returns:
        dict: Serialized author information, or None upon failure.
    """
    author = fetch_author(scholar_id)
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
        # Decide if you want to return None or continue to BigQuery write

    # NEW: Save to BigQuery
    try:
        document_id = scholar_id
        timestamp_val = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data_json_str = json.dumps(serialized_author)

        merge_sql_author = f"""
        MERGE `{author_table_id}` T
        USING (SELECT '{document_id}' as document_id, TIMESTAMP('{timestamp_val}') as timestamp, '{data_json_str}' as data) S
        ON T.document_id = S.document_id
        WHEN MATCHED THEN
          UPDATE SET T.timestamp = S.timestamp, T.data = S.data
        WHEN NOT MATCHED THEN
          INSERT (document_id, timestamp, data) VALUES(S.document_id, S.timestamp, S.data)
        """
        query_job = bigquery_client.query(merge_sql_author)
        query_job.result()  # Wait for the job to complete
        logging.info(f"Author {scholar_id} data merged into BigQuery table {author_table_id}.")
    except Exception as e:
        logging.error(f"Error merging author {scholar_id} data into BigQuery: {e}")
        # Optional: handle BigQuery write failure (e.g., if Firestore succeeded, this might be a partial failure)

    if not success_firestore: # If Firestore save failed initially
         return None


    if skip_pubs is None:
        enqueue_publications(author.get("publications", []))

    return serialized_author


def fetch_author(scholar_id):
    """Fetches detailed author data from Google Scholar.
    Args:
        scholar_id (str): The unique identifier for the author.
    Returns:
        dict: Author data, or None if an error occurs.
    """
    try:
        logging.info(f"Fetching author entry from Google Scholar for {scholar_id}")
        # Ensure sections are filled, especially publications for enqueuing
        return scholarly.fill(scholarly.search_author_id(scholar_id), sections=['basics', 'indices', 'counts', 'coauthors', 'publications'])
    except Exception as e:
        logging.error(
            f"Error fetching author data from Google Scholar for {scholar_id}: {e}"
        )
        return None


def enqueue_publications(publications):
    """Enqueues tasks for processing each publication.
    Args:
        publications (list): A list of publication data dictionaries.
    """
    for pub in publications:
        time.sleep(0.1)  # avoid overloading the queue service
        # Ensure the pub object passed to the task queue contains necessary fields
        # The default 'pub' from author['publications'] by scholarly.fill should be sufficient
        if not task_queue_service.enqueue_publication_task(pub):
            logging.error(
                f"Failed to enqueue publication task for {pub.get('author_pub_id')}"
            )


def serialize_author(author):
    """Serializes author data for storage, handling large data sizes.
    Args:
        author (dict): The author data to serialize.
    Returns:
        dict: The serialized author data.
    """
    try:
        author_copy = copy.deepcopy(author) # Use a copy to avoid modifying the original 'author' object
        # Simplify publications array for author document, actual pub details are filled by fill_publication
        if 'publications' in author_copy:
            author_copy["publications"] = [
                {
                    "author_pub_id": p.get("author_pub_id"),
                    "num_citations": p.get("num_citations", 0),
                    "filled": False, # Indicates that full details are not in this author doc
                    "bib": {
                        "title": p.get("bib", {}).get("title"), # Keep title for quick reference
                        "pub_year": p.get("bib", {}).get("pub_year") # Keep pub_year
                    }
                }
                for p in author_copy.get("publications", []) if p.get("author_pub_id")
            ]
        # Remove other potentially large fields if not needed in the 'author' document itself
        # e.g. author_copy.pop('coauthors', None) # if coauthor details are very large and stored separately

        # Convert large integers to strings (already does this)
        serialized = convert_integers_to_strings(
            json.loads(json.dumps(author_copy))
        )
        return serialized
    except Exception as e:
        logging.error(f"Error serializing author data: {e}")
        return None