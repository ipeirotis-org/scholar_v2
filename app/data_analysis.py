import logging

from shared.services.firestore_service import FirestoreService
from shared.services.bigquery_service import BigQueryService
from shared.repositories.author_repository import AuthorRepository
from shared.repositories.publication_repository import PublicationRepository

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize services and repositories
firestore_service = FirestoreService()
bigquery_service = BigQueryService()
publication_repository = PublicationRepository(firestore_service)
author_repository = AuthorRepository(firestore_service, publication_repository)


def get_author_stats(author_id):
    # Fetch author details
    author = author_repository.get_author(author_id)
    if not author:
        logging.warning(f"No author found with ID: {author_id}")
        return None

    # Check for last modification to determine if cache needs refresh
    author_last_modified = author_repository.get_author_last_modification(author_id)

    author["last_modified"] = author_last_modified

    # Fetch and cache author publication stats
    author_pub_stats, pub_stats_timestamp = firestore_service.get_firestore_cache(
        "author_pub_stats", author_id
    )
    if not author_pub_stats or author_last_modified > pub_stats_timestamp:
        author_pub_stats = bigquery_service.get_author_pub_stats(author_id)
        if author_pub_stats:
            firestore_service.set_firestore_cache(
                "author_pub_stats", author_id, author_pub_stats
            )

    # Fetch and cache author stats
    author_stats, stats_timestamp = firestore_service.get_firestore_cache(
        "author_stats", author_id
    )
    if not author_stats or author_last_modified > stats_timestamp:
        author_stats = bigquery_service.get_author_stats(author_id)
        if author_stats:
            firestore_service.set_firestore_cache(
                "author_stats", author_id, author_stats
            )

    author["publications"] = author_pub_stats or []
    author["stats"] = author_stats or {}

    temporal_stats = bigquery_service.get_author_temporal_stats(author_id)
    author["temporal_stats"] = temporal_stats

    return author


def get_publication_stats(author_id, author_pub_id):
    pub = publication_repository.get_publication(author_pub_id)
    if not pub:
        logging.warning(f"No publication found with ID: {author_pub_id}")
        return None

    author_last_modified = author_repository.get_author_last_modification(author_id)

    pub["last_modified"] = author_last_modified

    pub_stats, pub_stats_timestamp = firestore_service.get_firestore_cache(
        "pub_stats", author_pub_id
    )
    if not pub_stats or author_last_modified > pub_stats_timestamp:
        pub_stats = bigquery_service.get_publication_stats(author_pub_id)
        if pub_stats:
            firestore_service.set_firestore_cache("pub_stats", author_pub_id, pub_stats)

    # Append stats to author object
    if pub_stats:
        pub["stats"] = pub_stats
    else:
        logging.warning(f"No pub stats found for pub ID: {author_pub_id}")
        pub["stats"] = {}

    return pub


def download_all_authors_stats():
    # Assuming bigquery_service is an instance of your BigQueryService class
    df = bigquery_service.get_all_authors_stats()
    return df
