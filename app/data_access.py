import logging
from datetime import datetime
import pytz
from google.cloud import firestore

db = firestore.Client()

def get_firestore_cache(collection, author_id):
    logging.info(f"Trying to fetch from cache for '{author_id}'.")
    doc_ref = db.collection(collection).document(author_id)
    try:
        doc = doc_ref.get()
        if doc.exists:
            cached_data = doc.to_dict()
            cached_time = cached_data['timestamp']
            current_time = datetime.utcnow().replace(tzinfo=pytz.utc)
            if (current_time - cached_time).days < 30:
                return cached_data['data']
            else:
                return None 
    except Exception as e:
        logging.error(f"Error accessing Firestore: {e}")
    return None

def set_firestore_cache(collection, author_id, data):
    doc_ref = db.collection(collection).document(author_id)

    cache_data = {
        'timestamp': datetime.utcnow().replace(tzinfo=pytz.utc),
        'data': data
    }
    if not author_id.strip():
        logging.error("Firestore document ID is empty or invalid.")
        return

    try:
        doc_ref.set(cache_data)
    except Exception as e:
        logging.error(f"Error updating Firestore: {e}")
