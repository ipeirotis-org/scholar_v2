import functions_framework
import json
import logging
import copy
import time
from flask import jsonify
from scholarly import scholarly


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


@functions_framework.http
def search_author_id(request):
    """Responds to HTTP requests with author information from Google Scholar.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        flask.Response: HTTP response object.
    """
    scholar_id = request.args.get("scholar_id") or (request.get_json(silent=True) or {}).get("scholar_id")
    skip_pubs = request.args.get("skip_pubs") or (request.get_json(silent=True) or {}).get("skip_pubs")

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

    success = author_repository.save_author(scholar_id, serialized_author)
    logging.info(f"Saved author {scholar_id}.")

    if not success:
        logging.error(f"Failed to store author {scholar_id} in Firestore.")
        return None

    # TODO: When the number of publications are large, the app
    # does not work well. We should consider refactoring this.
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
        return scholarly.fill(scholarly.search_author_id(scholar_id))
    except Exception as e:
        logging.error(f"Error fetching author data from Google Scholar for {scholar_id}: {e}")
        return None


def enqueue_publications(publications):
    """Enqueues tasks for processing each publication.
    Args:
        publications (list): A list of publication data dictionaries.
    """
    for pub in publications:
        time.sleep(0.1)  # avoid overloading the queue service
        if not task_queue_service.enqueue_publication_task(pub):
            logging.error(f"Failed to enqueue publication task for {pub.get('author_pub_id')}")


def serialize_author(author):
    """Serializes author data for storage, handling large data sizes.
    Args:
        author (dict): The author data to serialize.
    Returns:
        dict: The serialized author data.
    """
    try:
        author = copy.deepcopy(author)
        author["publications"] = [
            {
                "author_pub_id": pub.get("author_pub_id"),
                "num_citations": pub.get("num_citations", 0),
                "filled": False,
                "bib": {key: pub["bib"][key] for key in ["pub_year"] if key in pub.get("bib", {})},
                # "source" : pub.get("source"),
                # "container_type" : pub.get("container_type")
            }
            for pub in author.get("publications", [])
            if pub.get("author_pub_id")
        ]
        serialized = convert_integers_to_strings(json.loads(json.dumps(author)))  # Simplified serialization
        return serialized
    except Exception as e:
        logging.error(f"Error serializing author data: {e}")
        return None
