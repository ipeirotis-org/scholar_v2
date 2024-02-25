import logging
from google.cloud import firestore
from datetime import datetime
import pytz
from ..config import Config

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

    def query_by_prefix(self, collection, field, prefix):
        """
        Perform a query in a Firestore collection using a prefix on a specified field.
        
        :param collection: The name of the Firestore collection.
        :param field: The document field to query on.
        :param prefix: The prefix string to match against.
        :return: A list of documents matching the prefix query.
        """
        end_at = prefix + '\uf8ff'
        query = self.db.collection(collection).where(field, ">=", prefix).where(field, "<=", end_at)
        results = query.stream()
        return [doc.to_dict() for doc in results]


    def objects_needing_refresh(self, collection, days_since_last_update, limit, key_attr):
        """
        Fetch objects from a collection that have not been updated recently.

        Parameters:
        - collection: The Firestore collection name.
        - days_since_last_update: Number of days to consider for an object being outdated.
        - limit: Maximum number of objects to fetch.
        - key_attr: The key attribute in the document to return.
        
        Returns:
        A list of values for the key attribute from documents that match the criteria.
        """
        cutoff_date = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=days_since_last_update)
        query = self.db.collection(collection)\
            .where("timestamp", "<", cutoff_date)\
            .order_by("timestamp", direction=firestore.Query.ASCENDING)\
            .limit(limit)
        
        return [doc.to_dict().get(key_attr) for doc in query.stream() if key_attr in doc.to_dict()]