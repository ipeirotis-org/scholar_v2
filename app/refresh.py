import logging
from shared.config import Config
from shared.services.firestore_service import FirestoreService
from shared.services.task_queue_service import TaskQueueService
from shared.repositories.author_repository import AuthorRepository
from shared.repositories.publication_repository import PublicationRepository

from coauthor_service import new_coauthors

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize services
firestore_service = FirestoreService()
task_queue_service = TaskQueueService()
publication_repository = PublicationRepository(firestore_service)
author_repository = AuthorRepository(
    firestore_service, publication_repository
)  # Assuming publication_repository is None or similarly initialized


def get_authors_to_refresh(num_authors=10):
    # Use the AuthorRepository to fetch authors needing refresh
    return author_repository.get_authors_needing_refresh(num_authors)


def refresh_authors(
    refresh: list[str] | None = None,
    *,
    num_authors: int = 1,
    include_new_coauthors: bool = False,  # <-- new flag
):
    """
    Queue authors—and optionally new co‑authors—for update.
    If 'refresh' is empty, fall back to automatic selection.
    """
    ids: set[str] = set(refresh or [])

    if include_new_coauthors:
        ids.update(new_coauthors(num_authors))  # <-- add sampled co‑authors

    if not ids:
        ids.update(get_authors_to_refresh(num_authors))

    total_authors = 0
    total_pubs = 0
    authors = []

    for scholar_id in ids:
        doc = (
            firestore_service.db.collection(Config.FIRESTORE_COLLECTION_AUTHOR)
            .document(scholar_id)
            .get()
        )

        if not doc.exists:
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
            authors.append(
                {
                    "doc_id": doc.id,
                    "author_id": author_id,
                    "publications": len(publications),
                    "name": author.get("name"),
                }
            )

        # Additional logic for handling document IDs not matching author IDs could go here
        # For example, deleting or updating records as necessary

    return {
        "total_authors": total_authors,
        "total_publications": total_pubs,
        "authors": authors,
    }
