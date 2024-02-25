import json
import logging
from shared.config import Config
from shared.services.firestore_service import FirestoreService
from shared.services.task_queue_service import TaskQueueService
from shared.repositories.author_repository import AuthorRepository
from shared.repositories.publication_repository import PublicationRepository


# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize services
firestore_service = FirestoreService()
task_queue_service = TaskQueueService()
publication_repository = PublicationRepository(firestore_service_
author_repository = AuthorRepository(firestore_service, publication_repository)  # Assuming publication_repository is None or similarly initialized



def get_authors_to_refresh(num_authors=10):
    # Use the AuthorRepository to fetch authors needing refresh
    return author_repository.get_authors_needing_refresh(num_authors)



def refresh_authors(refresh=[], num_authors=1):
    if not refresh:
        refresh = get_authors_to_refresh(num_authors)

    total_authors = 0
    total_pubs = 0
    authors = []

    for scholar_id in refresh:
        doc = firestore_service.db.collection(Config.FIRESTORE_COLLECTION_AUTHOR).document(scholar_id).get()
        if not doc.exists:
            # Enqueue author task if document does not exist
            if task_queue_service.enqueue_author_task(scholar_id):
                total_authors += 1
                authors.append({"author_id": scholar_id})
            continue

        author = doc.to_dict().get("data")
        if not author:
            # Handle the case where the document exists but has no relevant data
            if task_queue_service.enqueue_author_task(scholar_id):
                total_authors += 1
                authors.append({"doc_id": doc.id, "author_id": scholar_id})
            continue

        author_id = author.get("scholar_id")
        # Enqueue the author for updating
        if task_queue_service.enqueue_author_task(scholar_id):
            publications = author.get("publications", [])
            total_authors += 1
            total_pubs += len(publications)
            authors.append({
                "doc_id": doc.id,
                "author_id": author_id,
                "publications": len(publications),
                "name": author.get("name")
            })

        # Additional logic for handling document IDs not matching author IDs could go here
        # For example, deleting or updating records as necessary

    return {
        "total_authors": total_authors,
        "total_publications": total_pubs,
        "authors": authors
    }
