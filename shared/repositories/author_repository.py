from ..config import Config
from random import sample

class AuthorRepository:
    def __init__(self, firestore_service, publication_repository):
        self.firestore_service = firestore_service
        self.publication_repository = publication_repository

    def get_author(self, author_id):
        return self.firestore_service.get_firestore_cache(Config.FIRESTORE_COLLECTION_AUTHOR, author_id)[0]

    def save_author(self, author_id, author_data):
        return self.firestore_service.set_firestore_cache(Config.FIRESTORE_COLLECTION_AUTHOR, author_id, author_data)

    def get_author_last_modification(self, author_id):
        # Fetch the last modification time of the author itself
        _, latest_author_change = self.firestore_service.get_firestore_cache(Config.FIRESTORE_COLLECTION_AUTHOR, author_id)

        # Use PublicationRepository to find the latest publication timestamp
        latest_pub_change = self.publication_repository.get_latest_publication_timestamp(author_id)

        # Compare and return the latest of the two timestamps
        return max(filter(None, [latest_author_change, latest_pub_change]))

    def get_authors_needing_refresh(self, num_authors=1):
        """
        Fetch author IDs that need refreshing based on their last update timestamp.

        Parameters:
        - num_authors: The number of author IDs to fetch.

        Returns:
        A list of author IDs.
        """
        authors = self.firestore_service.objects_needing_refresh(
            collection=Config.FIRESTORE_COLLECTION_AUTHOR,
            days_since_last_update=90,  # Considering authors not updated in the last 90 days
            limit=num_authors*100, # overfetch to avoid fetching the same authors
            key_attr="scholar_id",
        )

        return sample(authors, num_authors)
