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
from google.cloud import tasks_v2

logging.basicConfig(level=logging.INFO)

db = firestore.Client()
client = tasks_v2.CloudTasksClient()


# Google Cloud project ID and queue location
project = "scholar-version2"
location = "northamerica-northeast1"


# Construct the fully qualified queue name
authors_queue = client.queue_path(project, location, "process-authors")
pubs_queue = client.queue_path(project, location, "process-pubs")


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
        logging.error(
            f"Error accessing Firestore for collection{collection} and doc {doc_id}: {e}"
        )
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
        logging.error(
            f"Error updating Firestore for collection{collection} and doc {doc_id}: {e}"
        )



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
    # use_cache = request_json.get("use_cache", request_args.get("use_cache"))

    if not pub:
        return "Missing pub", 400

    # pub = json.loads(pub)
    pub = fill_pub(pub)
    if pub is None:
        return "Error fill publication from Google Scholar", 500

    response = make_response(pub)
    response.headers["Content-Type"] = "application/json"

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



def fill_pub(pub):
    try:
        logging.info(f"Fetching pub entry for publication {pub['author_pub_id']}")
        pub = scholarly.fill(pub)

        serialized = convert_integers_to_strings(json.loads(json.dumps(pub)))
        set_firestore_cache("scholar_raw_pub", pub["author_pub_id"], serialized)
        return serialized

    except Exception as e:
        logging.error(
            f"Error fetching detailed data for publication {pub['author_pub_id']}: {e}"
        )
        return None
