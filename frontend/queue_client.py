"""Thin client to enqueue tasks to Cloud Tasks queues.

Supports two queue types:
- cache-priority: for cache population tasks on cache miss
- process-authors-priority: for user-initiated author crawl tasks

All crawler invocations go through Cloud Tasks queues with OIDC
authentication. No direct HTTP calls to crawler functions.
"""

import json
import logging
import time

from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2

from frontend.config import Config

logger = logging.getLogger(__name__)

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = tasks_v2.CloudTasksClient()
    return _client


def _queue_path(queue_name=None):
    queue_name = queue_name or Config.QUEUE_NAME_CACHE_PRIORITY
    return (
        f"projects/{Config.PROJECT_ID}"
        f"/locations/{Config.QUEUE_LOCATION}"
        f"/queues/{queue_name}"
    )


def enqueue_cache_populate(request_type, payload):
    """Enqueue a cache population task to the priority queue.

    Args:
        request_type: e.g. "populate_author_profile", "populate_publication_detail"
        payload: dict with task-specific fields (scholar_id, author_pub_id, etc.)

    Returns True if enqueued, False on failure.
    """
    cache_layer_url = Config.CACHE_LAYER_URL
    if not cache_layer_url:
        logger.warning("CACHE_LAYER_URL not configured, cannot enqueue cache task")
        return False

    target_url = f"{cache_layer_url.rstrip('/')}/tasks/priority"

    task_body = {"type": request_type}
    task_body.update(payload)

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": target_url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(task_body).encode(),
            "oidc_token": {
                "service_account_email": Config.CLOUD_TASKS_SA_EMAIL,
                "audience": cache_layer_url.rstrip("/"),
            },
        },
    }

    try:
        _get_client().create_task(parent=_queue_path(), task=task)
        logger.info("Enqueued cache task: %s %s", request_type, payload)
        return True
    except Exception:
        logger.exception("Failed to enqueue cache task: %s", request_type)
        return False


def _sanitize_task_id(raw_id):
    """Sanitize an ID for use as a Cloud Tasks task name."""
    return raw_id.replace(":", "__").replace("/", "___")


def enqueue_author_crawl(scholar_id):
    """Trigger an author crawl via Cloud Tasks queue.

    All crawler invocations go through Cloud Tasks with OIDC authentication.
    Uses a rotating region URL to distribute load across Cloud Function
    regions and avoid rate-limiting from any single region.

    Returns True if enqueued, False on failure.
    """
    crawl_url = Config.get_rotating_crawl_url()
    if not crawl_url:
        logger.warning("CRAWL_FUNCTION_URL not configured, cannot enqueue crawl task")
        return False

    return _enqueue_author_crawl_task(scholar_id, crawl_url)


def _enqueue_author_crawl_task(scholar_id, crawl_url):
    """Enqueue an author crawl task to the priority Cloud Tasks queue.

    Task names include a 10-minute time bucket to avoid Cloud Tasks
    tombstone blocking (completed/failed task names can't be reused
    for up to 1 hour) while still deduplicating rapid retries.
    """
    queue_path = _queue_path(Config.QUEUE_NAME_CRAWL_PRIORITY)
    time_bucket = int(time.time()) // 600
    task_id = f"{_sanitize_task_id(scholar_id)}-{time_bucket}"
    task_name = f"{queue_path}/tasks/{task_id}"

    # Extract base URL (scheme + host) for OIDC audience
    from urllib.parse import urlparse
    parsed = urlparse(crawl_url)
    audience = f"{parsed.scheme}://{parsed.netloc}"

    task = {
        "name": task_name,
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": crawl_url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"scholar_id": scholar_id, "priority": True}).encode(),
            "oidc_token": {
                "service_account_email": Config.CLOUD_TASKS_SA_EMAIL,
                "audience": audience,
            },
        },
    }

    try:
        _get_client().create_task(parent=queue_path, task=task)
        logger.info("Enqueued author crawl task: %s → %s", scholar_id, crawl_url)
        return True
    except AlreadyExists:
        logger.info("Author crawl task already exists: %s", scholar_id)
        return True
    except Exception:
        logger.exception("Failed to enqueue crawl task: %s", scholar_id)
        return False
