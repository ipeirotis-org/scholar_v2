from ..config import Config

class AuthorRepository:
    def __init__(self, firestore_service, publication_repository):
        self.firestore_service = firestore_service
        self.publication_repository = publication_repository

    def get_author(self, author_id):
        return self.firestore_service.get_firestore_cache(Config.FIRESTORE_COLLECTION_AUTHOR, author_id)[0]

    def save_author(self, author_id, author_data):
        self.firestore_service.set_firestore_cache(Config.FIRESTORE_COLLECTION_AUTHOR, author_id, author_data)

    def get_author_last_modification(self, author_id):
        # Assuming you have a method or logic to determine the last modification timestamp
        # This is an example and might need to be adjusted based on your actual data structure
        _, latest_author_change = self.firestore_service.get_firestore_cache(Config.FIRESTORE_COLLECTION_AUTHOR, author_id)
        return latest_author_change

    def get_author_last_modification(self, author_id):
        # Fetch the last modification time of the author itself
        _, latest_author_change = self.firestore_service.get_firestore_cache("scholar_raw_author", author_id)
        
        # Use PublicationRepository to find the latest publication timestamp
        latest_pub_change = self.publication_repository.get_latest_publication_timestamp(author_id)
        
        # Compare and return the latest of the two timestamps
        return max(filter(None, [latest_author_change, latest_pub_change]))