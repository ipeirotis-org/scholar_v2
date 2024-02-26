import functions_framework
import json
import logging
from flask import make_response, jsonify

from scholarly import scholarly
from scholarly.data_types import PublicationSource

from shared.config import Config
from shared.utils import convert_integers_to_strings
from shared.services.firestore_service import FirestoreService

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Instantiate services
firestore_service = FirestoreService()

@functions_framework.http
def fill_publication(request):
    """HTTP Cloud Function to fill publication details from Google Scholar and cache them."""
    request_json = request.get_json(silent=True)
    
    # Validate input
    pub = request_json.get("pub")
    if not pub or not isinstance(pub, dict) or "author_pub_id" not in pub:
        logging.error("Invalid publication data provided.")
        return jsonify({"error": "Missing or invalid 'pub' data"}), 400

    try:
        filled_pub = process_publication(pub)
        return jsonify(filled_pub), 200
    except Exception as e:
        logging.error(f"Failed to process publication: {e}")
        return jsonify({"error": "Failed to process publication"}), 500

def process_publication(pub):
    """Fetches, serializes, and caches publication details."""
    author_pub_id = pub['author_pub_id']
    logging.info(f"Fetching publication details for {author_pub_id}")

    pub["source"] = PublicationSource.AUTHOR_PUBLICATION_ENTRY
    pub["container_type"] = 'Publication'

    # Fetch publication details
    detailed_pub = scholarly.fill(pub)
    
    # Convert large integers to strings to avoid serialization issues
    serialized_pub = convert_integers_to_strings(json.loads(json.dumps(detailed_pub)))
    
    # Cache publication details
    firestore_service.set_firestore_cache(Config.FIRESTORE_COLLECTION_PUB, author_pub_id, serialized_pub)
    
    logging.info(f"Publication details for {author_pub_id} have been updated and cached.")
    return serialized_pub

