"""Core orchestration logic for refresh & expand operations.

This module coordinates BigQuery queries (to find what needs refreshing)
with Cloud Tasks enqueueing (to instruct the Crawler what to fetch).
"""

import logging

from refresh import bigquery_client as bq
from refresh import task_enqueuer

logger = logging.getLogger(__name__)


def refresh_stale_authors(limit=None):
    """Find stale authors and enqueue them for re-crawl.

    Returns a summary dict with query results and enqueue stats.
    """
    scholar_ids = bq.get_stale_authors(limit=limit)
    if not scholar_ids:
        logger.info("No stale authors found")
        return {"source": "stale", "found": 0, "enqueued": 0, "duplicates": 0, "errors": []}

    result = task_enqueuer.enqueue_authors(scholar_ids)
    return {
        "source": "stale",
        "found": len(scholar_ids),
        **result,
    }


def refresh_error_authors(limit=None):
    """Find authors with fetch errors and enqueue them for re-crawl.

    Respects the error cooldown period to avoid retry loops.
    Returns a summary dict.
    """
    scholar_ids = bq.get_error_authors(limit=limit)
    if not scholar_ids:
        logger.info("No error authors found (or all within cooldown)")
        return {"source": "errors", "found": 0, "enqueued": 0, "duplicates": 0, "errors": []}

    result = task_enqueuer.enqueue_authors(scholar_ids)
    return {
        "source": "errors",
        "found": len(scholar_ids),
        **result,
    }


def expand_coauthors(limit=None):
    """Find coauthors not in the database and enqueue them for initial crawl.

    Returns a summary dict.
    """
    scholar_ids = bq.get_coauthors_to_add(limit=limit)
    if not scholar_ids:
        logger.info("No new coauthors to add")
        return {"source": "coauthors", "found": 0, "enqueued": 0, "duplicates": 0, "errors": []}

    result = task_enqueuer.enqueue_authors(scholar_ids)
    return {
        "source": "coauthors",
        "found": len(scholar_ids),
        **result,
    }


def fetch_author(scholar_id):
    """Enqueue a single author for crawl (user-triggered).

    Returns a summary dict including whether the author already exists.
    """
    exists = bq.author_exists(scholar_id)
    try:
        enqueued = task_enqueuer.enqueue_author(scholar_id)
    except Exception as exc:
        logger.error("Failed to enqueue author %s: %s", scholar_id, exc)
        return {
            "scholar_id": scholar_id,
            "exists": exists,
            "enqueued": False,
            "error": str(exc),
        }

    return {
        "scholar_id": scholar_id,
        "exists": exists,
        "enqueued": enqueued,
    }


def fetch_authors(scholar_ids):
    """Enqueue multiple authors for crawl (user-triggered).

    Returns a summary dict with per-author results.
    """
    result = task_enqueuer.enqueue_authors(scholar_ids)
    return {
        "source": "user_request",
        "found": len(scholar_ids),
        **result,
    }
