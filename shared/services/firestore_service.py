import logging
from google.cloud import firestore
from datetime import datetime
import pytz
from ..config import Config  # Adjust the import path based on your project structure

class FirestoreService:
    def __init__(self):
        self.db = firestore.Client(project=Config.PROJECT_ID)

    def get_firestore_cache(self, collection, doc_id):
        logging.info(f"Fetching from Firestore for '{doc_id}' in collection {collection}.")
        doc_ref = self.db.collection(collection).document(doc_id)
        try:
            doc = doc_ref.get()
            if doc.exists:
                cached_data = doc.to_dict()
                cached_time = cached_data["timestamp"]
                logging.info(f"Fetched data from Firestore for '{doc_id}'.")
                return cached_data["data"], cached_data["timestamp"]
        except Exception as e:
            logging.error(f"Error accessing Firestore: {e}")
        return None, None

    def set_firestore_cache(self, collection, doc_id, data):
        if not doc_id.strip():
            logging.error("Firestore document ID is empty or invalid.")
            return False

        doc_ref = self.db.collection(collection).document(doc_id)
        current_time = datetime.utcnow().replace(tzinfo=pytz.utc)
        cache_data = {"timestamp": current_time, "data": data}

        try:
            doc_ref.set(cache_data)
            logging.info(f"Data set in Firestore for '{doc_id}'.")
            return True  # success
        except Exception as e:
            logging.error(f"Error updating Firestore: {e}")
            return False  # failure
