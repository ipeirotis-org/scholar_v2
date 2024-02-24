from ..config import Config

class PublicationRepository:
    def __init__(self, firestore_service):
        self.firestore_service = firestore_service

    def get_publications_by_author(self, author_id):
        # This method assumes you've adjusted FirestoreService to include a generic query method
        return self.firestore_service.query_by_prefix(Config.FIRESTORE_COLLECTION_PUB, "data.author_pub_id", author_id)

    def save_publication(self, author_pub_id, publication_data):
        self.firestore_service.set_firestore_cache(Config.FIRESTORE_COLLECTION_PUB, author_pub_id, publication_data)

    def get_publication(self, author_pub_id):
        return self.firestore_service.get_firestore_cache(Config.FIRESTORE_COLLECTION_PUB, author_pub_id)[0]

    def get_latest_publication_timestamp(self, author_id):
        publications = self.firestore_service.query_by_prefix(
            Config.FIRESTORE_COLLECTION_PUB, "data.author_pub_id", author_id)
        timestamps = [pub['timestamp'] for pub in publications if 'timestamp' in pub]
        return max(timestamps) if timestamps else None