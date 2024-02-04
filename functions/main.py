import functions_framework
import json
import logging
from datetime import datetime
from scholarly import scholarly
from flask import make_response
import logging
from datetime import datetime
import pytz
from google.cloud import firestore

db = firestore.Client()


def get_firestore_cache(collection, doc_id):
    logging.info(
        f"Trying to fetch from cache for '{doc_id}' in collection {collection}."
    )
    doc_ref = db.collection(collection).document(doc_id)
    try:
        doc = doc_ref.get()
        if doc.exists:
            cached_data = doc.to_dict()
            cached_time = cached_data["timestamp"]
            current_time = datetime.utcnow().replace(tzinfo=pytz.utc)
            if (current_time - cached_time).days < 30:
                logging.info(f"Fetched data from cache for '{doc_id}'.")
                return cached_data["data"]
            else:
                logging.info(f"Old data for '{doc_id}'. Going to google scholar")
                return None

    except Exception as e:
        logging.error(f"Error accessing Firestore: {e}")
    return None


def set_firestore_cache(collection, doc_id, data):
    if not doc_id.strip():
        logging.error("Firestore document ID is empty or invalid.")
        return

    doc_ref = db.collection(collection).document(doc_id)

    cache_data = {"timestamp": datetime.utcnow().replace(tzinfo=pytz.utc), "data": data}

    try:
        doc_ref.set(cache_data)
    except Exception as e:
        logging.error(f"Error updating Firestore: {e}")



@functions_framework.http
def search_author_id(request):
    """HTTP Cloud Function.
    Args:
       request (flask.Request): The request object.
    Returns:
       The response text, or any set of values that can be turned into a
       Response object using `make_response`.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    scholar_id = request_json.get("scholar_id", request_args.get("scholar_id"))
    use_cache = request_json.get("use_cache", request_args.get("use_cache"))
    
    if not scholar_id:
        return "Missing author id", 400


    author = get_author(scholar_id, use_cache)
    if author is None:
        return "Error getting data from Google Scholar", 500

    response = make_response(author)
    response.headers['Content-Type'] = 'application/json'


    
    return response, 200

@functions_framework.http
def fill_publication(request):
    """HTTP Cloud Function.
    Args:
       request (flask.Request): The request object.
    Returns:
       The response text, or any set of values that can be turned into a
       Response object using `make_response`.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    pub = request_json.get("pub", request_args.get("pub"))
    use_cache = request_json.get("use_cache", request_args.get("use_cache"))
    
    if not pub:
        return "Missing pub", 400

    pub = fill_pub(pub, use_cache)
    if pub is None:
        return "Error fill pblication from Google Scholar", 500

    response = make_response(pub)
    response.headers['Content-Type'] = 'application/json'
    
    return response, 200


def convert_integers_to_strings(data):
    if isinstance(data, dict):
        return {key: convert_integers_to_strings(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_integers_to_strings(element) for element in data]
    elif isinstance(data, int):
        if abs(data) > 2**62:
            return str(data)
        else:
            return data
    else:
        return data


def get_author(author_id, use_cache):

    
    if use_cache:
        cached_data = get_firestore_cache("scholar_raw_author", author_id)
        if cached_data:
            logging.info(f"Cache hit for raw scholar data for author: '{author_id}'.")
            return cached_data    

    try:
        logging.info(f"Fetching author entry for {author_id}")
        author = scholarly.search_author_id(author_id)
        author = scholarly.fill(author)
        
        serialized = convert_integers_to_strings(json.loads(json.dumps(author)))
        
        set_firestore_cache("scholar_raw_author", author_id, serialized)
        return serialized

    except Exception as e:
        logging.error(f"Error fetching detailed author data: {e}")
        return None

def fill_pub(pub, use_cache):

    if use_cache:
        cached_data = get_firestore_cache("scholar_raw_pub", pub['author_pub_id'])
        if cached_data:
            logging.info(
                f"Cache hit for raw scholar data for publication: '{pub['author_pub_id']}'."
            )
            return cached_data

    try:
        logging.info(f"Fetching pub entry for publication {pub['author_pub_id']}")
        pub = scholarly.fill(pub)
       
        serialized = convert_integers_to_strings(json.loads(json.dumps(pub)))
        set_firestore_cache("scholar_raw_pub", pub["author_pub_id"], serialized)
        return serialized

    except Exception as e:
        logging.error(f"Error fetching detailed author data: {e}")
        return None


