import logging
from datetime import datetime
import pytz
from google.cloud import firestore

db = firestore.Client()


def get_firestore_cache(collection, doc_id):
    logging.info(
        f"Fetching from Firestore for '{doc_id}' in collection {collection}."
    )
    doc_ref = db.collection(collection).document(doc_id)
    try:
        doc = doc_ref.get()
        if doc.exists:
            cached_data = doc.to_dict()
            cached_time = cached_data["timestamp"]
            # current_time = datetime.utcnow().replace(tzinfo=pytz.utc)
            # if (current_time - cached_time).days < 30:
            logging.info(f"Fetched data from Firestore for '{doc_id}'.")
            return cached_data["data"], cached_data["timestamp"]
            #else:
            #    logging.info(f"Old data for '{doc_id}'. Going to google scholar")
            #    return None

    except Exception as e:
        logging.error(f"Error accessing Firestore: {e}")
    return None, None


def set_firestore_cache(collection, doc_id, data):
    if not doc_id.strip():
        logging.error("Firestore document ID is empty or invalid.")
        return

    doc_ref = db.collection(collection).document(doc_id)

    current_time = datetime.utcnow().replace(tzinfo=pytz.utc)

    cache_data = {"timestamp": current_time, "data": data}

    try:
        doc_ref.set(cache_data)
        return True # success
    except Exception as e:
        logging.error(f"Error updating Firestore: {e}")
        return False # failure