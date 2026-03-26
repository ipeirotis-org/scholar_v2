"""Enqueue author and publication fetch tasks to Cloud Tasks.

All tasks include OIDC tokens for authenticated invocation of
Cloud Functions that require authentication.
"""

import json
import logging
import time
from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2

from crawler.config import Config
from crawler.failure_tracker import record_partial_enqueue_failure

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
    queue_path = Config.queue_path(Config.QUEUE_NAME_AUTHORS)
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
        logger.info(f"Enqueued author task: {scholar_id}")
        return True
    except AlreadyExists:
        logger.info(f"Author task already exists: {scholar_id}")
        return False


def enqueue_publication(pub_entry, delay=None, priority=False):
    """Enqueue a task to fetch a publication from Google Scholar.

    Args:
        pub_entry: Publication dict with author_pub_id.
        delay: Not used (kept for API compat); stagger is in enqueue_publications.
        priority: If True, use priority queue and include priority flag in body.

    Returns True if enqueued, False if duplicate.
    Raises on other errors.
    """
    client = _get_client()
    author_pub_id = pub_entry.get("author_pub_id", "")
    queue_name = Config.QUEUE_NAME_PUBS_PRIORITY if priority else Config.QUEUE_NAME_PUBS
    queue_path = Config.queue_path(queue_name)
    # Include a 10-minute time bucket to avoid Cloud Tasks tombstone blocking:
    # completed/failed task names can't be reused for ~1 hour.
    time_bucket = int(time.time()) // 600
    task_name = f"{queue_path}/tasks/{_sanitize_task_id(author_pub_id)}-{time_bucket}"
    url = Config.function_url("v3_fetch_publication")

    body = {"pub": pub_entry}
    if priority:
        body["priority"] = True

    task = {
        "name": task_name,
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body).encode(),
            "oidc_token": _oidc_token(url),
        },
    }

    try:
        client.create_task(parent=queue_path, task=task)
        logger.info(f"Enqueued publication task: {author_pub_id} (priority={priority})")
        return True
    except AlreadyExists:
        logger.info(f"Publication task already exists: {author_pub_id}")
        return False


def enqueue_publications(publications, delay=None, priority=False):
    """Enqueue tasks for a list of publications with stagger delay.

    Args:
        publications: List of publication dicts (from author.publications).
        delay: Seconds between enqueue calls (default: Config.PUB_ENQUEUE_DELAY).
        priority: If True, use priority queue and pass priority flag through.

    Returns:
        Count of newly enqueued tasks.

    Raises:
        RuntimeError: If every publication in a non-empty list failed to enqueue
            (indicates Cloud Tasks is unavailable). This lets the caller return
            an error status so Cloud Tasks retries the parent author task.
    """
    delay = delay if delay is not None else Config.PUB_ENQUEUE_DELAY
    count = 0
    errors = 0
    failed_pub_ids = []
    for pub in publications:
        try:
            if enqueue_publication(pub, priority=priority):
                count += 1
        except Exception:
            errors += 1
            pub_id = pub.get("author_pub_id", "unknown")
            failed_pub_ids.append(pub_id)
            logger.exception(f"Failed to enqueue publication task: {pub_id}")
        if delay > 0:
            time.sleep(delay)
    logger.info(
        f"Enqueued {count}/{len(publications)} publication tasks "
        f"(priority={priority}, errors={errors})"
    )
    if publications and count == 0 and errors > 0:
        raise RuntimeError(
            f"All {errors} publication enqueue attempts failed"
        )
    # Track partial failures so they're visible in the failure dashboard
    if failed_pub_ids and count > 0:
        # Extract scholar_id from the first pub's author_pub_id (format: "scholar_id:pub_id")
        first_pub_id = publications[0].get("author_pub_id", "")
        scholar_id = first_pub_id.split(":")[0] if ":" in first_pub_id else "unknown"
        record_partial_enqueue_failure(scholar_id, failed_pub_ids)
    return count
