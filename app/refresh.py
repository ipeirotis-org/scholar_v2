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


def get_stale_authors(num_authors=10):
    # Use the AuthorRepository to fetch authors needing refresh
    return author_repository.get_authors_needing_refresh(num_authors)

def get_coauthors_not_in_db(num_authors=10):
    # Use the AuthorRepository to fetch authors needing refresh
    return new_coauthors(num_authors)


def fetch_authors(
    refresh: list[str] | None = None,
):
    """
    Queue authors—and optionally new co‑authors—for update.
    If 'refresh' is empty, fall back to automatic selection.
    """
    ids: set[str] = set(refresh or [])

    total_authors = 0
    authors = []

    for scholar_id in ids:

        if task_queue_service.enqueue_author_task(scholar_id):
            total_authors += 1
            authors.append({"author_id": scholar_id})
        continue

    return {
        "total_authors": total_authors,
        "authors": authors,
    }


