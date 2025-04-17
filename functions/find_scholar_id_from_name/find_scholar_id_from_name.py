import logging
from flask import jsonify
from scholarly import scholarly
from shared.services.firestore_service import FirestoreService

# Setup logging
logging.basicConfig(level=logging.INFO)

# Initialize Firestore service (caches across invocations)
firestore_service = FirestoreService()


def get_similar_authors(author_name: str):
    """
    Fetches or retrieves cached similar authors for the given name.
    """
    cached_data, _ = firestore_service.get_firestore_cache("queries", author_name)
    if cached_data:
        logging.info(f"Cache hit for similar authors of '{author_name}'.")
        return cached_data

    authors = fetch_authors_from_scholarly(author_name)
    if authors:
        firestore_service.set_firestore_cache("queries", author_name, authors)
    return authors


def fetch_authors_from_scholarly(author_name: str):
    """
    Uses the scholarly package to search for similar authors.
    """
    authors = []
    try:
        search_query = scholarly.search_author(author_name)
        for _ in range(10):  # Limit to 10 authors
            try:
                author = next(search_query)
                if author:
                    authors.append(process_author(author))
            except StopIteration:
                break
    except Exception as e:
        logging.error(f"Error fetching similar authors for '{author_name}': {e}")
    return authors


def process_author(author: dict) -> dict:
    """
    Normalize the author dict into a predictable structure.
    """
    return {
        "name": author.get("name", ""),
        "affiliation": author.get("affiliation", ""),
        "email": author.get("email", ""),
        "citedby": author.get("citedby", 0),
        "scholar_id": author.get("scholar_id", ""),
    }


def find_scholar_id_using_name(request):
    """
    HTTP Cloud Function entry point.

    Expects a GET or POST with parameter 'name'.
    Returns a JSON payload with the list of similar authors.
    """
    # Parse JSON body if present
    request_json = request.get_json(silent=True)

    # Check query parameters first
    if request.args and 'name' in request.args:
        author_name = request.args.get('name')
    elif request_json and 'name' in request_json:
        author_name = request_json['name']
    else:
        return jsonify({"error": "Missing 'name' parameter"}), 400

    logging.info(f"Looking up scholar ids for author name: '{author_name}'")
    authors = get_similar_authors(author_name)
    return jsonify({"authors": authors}), 200
