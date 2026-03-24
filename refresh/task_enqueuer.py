"""Enqueue author fetch tasks to Cloud Tasks.

This is the only module that writes to Cloud Tasks queues.
The Refresh & Expand service uses this to instruct the Crawler
what to fetch. All tasks include OIDC tokens for authenticated
invocation of Cloud Functions.
"""

import json
import logging

from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2

from refresh.config import Config

logger = logging.getLogger(__name__)

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = tasks_v2.CloudTasksClient()
    return _client


def _sanitize_task_id(raw_id):
    """Sanitize an ID for use as a Cloud Tasks task name."""
    return raw_id.replace(":", "__").replace("/", "___")


def _oidc_token(url):
    """Build an OIDC token dict for the given function URL.

    For Cloud Functions Gen2, the audience must match the function's URL
    (including path), which is registered as a custom-audience on the
    underlying Cloud Run service.  Using only the domain results in 401.
    """
    return {
        "service_account_email": Config.CLOUD_TASKS_SA_EMAIL,
        "audience": url,
    }


def enqueue_author(scholar_id):
    """Enqueue a task to fetch an author from Google Scholar.

    Returns True if enqueued, False if duplicate (already exists).
    Raises on other errors.
    """
    client = _get_client()
    queue_path = Config.queue_path()
    task_name = f"{queue_path}/tasks/{_sanitize_task_id(scholar_id)}"
    url = Config.function_url("v3_fetch_author")

    task = {
        "name": task_name,
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"scholar_id": scholar_id}).encode(),
            "oidc_token": _oidc_token(url),
        },
    }

    try:
        client.create_task(parent=queue_path, task=task)
        logger.info("Enqueued author task: %s", scholar_id)
        return True
    except AlreadyExists:
        logger.info("Author task already exists: %s", scholar_id)
        return False


def enqueue_authors(scholar_ids):
    """Enqueue tasks for a list of author IDs.

    Returns a dict with counts of enqueued, duplicates, and errors.
    """
    enqueued = 0
    duplicates = 0
    errors = []

    for sid in scholar_ids:
        try:
            if enqueue_author(sid):
                enqueued += 1
            else:
                duplicates += 1
        except Exception as exc:
            logger.error("Failed to enqueue author %s: %s", sid, exc)
            errors.append({"scholar_id": sid, "error": str(exc)})

    logger.info(
        "Enqueue summary: %d enqueued, %d duplicates, %d errors",
        enqueued, duplicates, len(errors),
    )
    return {"enqueued": enqueued, "duplicates": duplicates, "errors": errors}


# ── Cache warming ────────────────────────────────────────────────────────────


def enqueue_cache_warm(scholar_id):
    """Enqueue a cache warming task to the batch queue.

    Called after a crawl is enqueued so the cache layer pre-populates
    the author's data before the user returns. Non-fatal — failures are
    logged but don't propagate.

    Returns True if enqueued, False otherwise.
    """
    cache_url = Config.CACHE_LAYER_URL
    if not cache_url:
        return False

    client = _get_client()
    queue_path = Config.queue_path(Config.QUEUE_NAME_CACHE_BATCH)
    target_url = f"{cache_url.rstrip('/')}/tasks/batch"

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": target_url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "type": "warm_author",
                "scholar_id": scholar_id,
            }).encode(),
            "oidc_token": _oidc_token(target_url),
        },
    }

    try:
        client.create_task(parent=queue_path, task=task)
        logger.info("Enqueued cache warm task: %s", scholar_id)
        return True
    except Exception:
        logger.exception("Failed to enqueue cache warm for %s", scholar_id)
        return False


def enqueue_cache_warm_batch(scholar_ids):
    """Enqueue cache warming tasks for a list of scholar IDs.

    Returns the number of tasks successfully enqueued.
    """
    if not Config.CACHE_LAYER_URL:
        return 0

    count = 0
    for sid in scholar_ids:
        if enqueue_cache_warm(sid):
            count += 1
    if count:
        logger.info("Enqueued %d cache warm tasks", count)
    return count
