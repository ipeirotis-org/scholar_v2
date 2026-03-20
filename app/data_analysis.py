import logging
from concurrent.futures import ThreadPoolExecutor

from shared.config import Config
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


def _fetch_and_cache(cache_key, author_id, author_last_modified, bq_fetch_fn):
    """Check cache; on miss, fetch from BigQuery and update cache."""
    cached_data, cached_timestamp = firestore_service.get_firestore_cache(
        cache_key, author_id
    )
    if cached_data and (not author_last_modified or author_last_modified <= cached_timestamp):
        return cached_data

    fresh_data = bq_fetch_fn(author_id)
    if fresh_data:
        firestore_service.set_firestore_cache(cache_key, author_id, fresh_data)
    return fresh_data


def get_author_stats(author_id, cache_only=False):
    """Fetch author details and stats, with caching.

    If cache_only=True, returns None when stats require a BigQuery fetch
    instead of blocking on the query. This enables "not ready" UX.
    """
    author_data, author_timestamp = firestore_service.get_firestore_cache(
        Config.FIRESTORE_COLLECTION_AUTHOR, author_id
    )
    if not author_data:
        logging.warning(f"No author found with ID: {author_id}")
        return None

    author = author_data

    # Fast path: check if both caches are valid against just the author timestamp.
    # This avoids the expensive publication timestamp prefix query when caches are
    # already newer than the author record (the common case for repeat visits).
    cached_pub_stats, cached_pub_ts = firestore_service.get_firestore_cache(
        "author_pub_stats", author_id
    )
    cached_author_stats, cached_author_ts = firestore_service.get_firestore_cache(
        "author_stats", author_id
    )

    if (cached_pub_stats and cached_author_stats
            and cached_pub_ts and cached_author_ts
            and author_timestamp
            and author_timestamp <= cached_pub_ts
            and author_timestamp <= cached_author_ts):
        logging.info(f"Fast-path cache hit for author {author_id}")
        author["last_modified"] = author_timestamp
        author["publications"] = cached_pub_stats
        author["stats"] = cached_author_stats
        return author

    # Slow path: need full cache invalidation check including publication timestamps
    latest_pub_change = publication_repository.get_latest_publication_timestamp(author_id)
    timestamps = [t for t in (author_timestamp, latest_pub_change) if t]
    author_last_modified = max(timestamps) if timestamps else None

    author["last_modified"] = author_last_modified

    # Re-check caches against the full author_last_modified timestamp
    pub_stats_valid = (cached_pub_stats and cached_pub_ts
                       and (not author_last_modified or author_last_modified <= cached_pub_ts))
    author_stats_valid = (cached_author_stats and cached_author_ts
                          and (not author_last_modified or author_last_modified <= cached_author_ts))

    if pub_stats_valid and author_stats_valid:
        author["publications"] = cached_pub_stats
        author["stats"] = cached_author_stats
        return author

    # Cache miss — need BigQuery. In cache_only mode, signal "not ready".
    if cache_only:
        return None

    # Fetch from BigQuery (only the stale ones)
    with ThreadPoolExecutor(max_workers=2) as executor:
        if not pub_stats_valid:
            pub_future = executor.submit(
                _fetch_and_cache,
                "author_pub_stats", author_id, author_last_modified,
                bigquery_service.get_author_pub_stats,
            )
        if not author_stats_valid:
            stats_future = executor.submit(
                _fetch_and_cache,
                "author_stats", author_id, author_last_modified,
                bigquery_service.get_author_stats,
            )

        author["publications"] = (pub_future.result() if not pub_stats_valid
                                  else cached_pub_stats) or []
        author["stats"] = (stats_future.result() if not author_stats_valid
                           else cached_author_stats) or {}

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
