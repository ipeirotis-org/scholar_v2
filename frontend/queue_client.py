"""Thin client to enqueue tasks to Cloud Tasks queues.

Supports two queue types:
- cache-priority: for cache population tasks on cache miss
- process-authors-priority: for user-initiated author crawl tasks
"""

import json
import logging

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
    """Enqueue an author crawl task to the priority crawler queue.

    Directly enqueues to process-authors-priority so the crawler Cloud
    Function picks it up. Used for user-initiated fetches.

    Returns True if enqueued, False if duplicate or on failure.
    """
    crawl_url = Config.CRAWL_FUNCTION_URL
    if not crawl_url:
        logger.warning("CRAWL_FUNCTION_URL not configured, cannot enqueue crawl task")
        return False

    queue_path = _queue_path(Config.QUEUE_NAME_CRAWL_PRIORITY)
    task_name = f"{queue_path}/tasks/{_sanitize_task_id(scholar_id)}"

    task = {
        "name": task_name,
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": crawl_url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"scholar_id": scholar_id}).encode(),
        },
    }

    try:
        _get_client().create_task(parent=queue_path, task=task)
        logger.info("Enqueued author crawl task: %s", scholar_id)
        return True
    except AlreadyExists:
        logger.info("Author crawl task already exists: %s", scholar_id)
        return True
    except Exception:
        logger.exception("Failed to enqueue crawl task: %s", scholar_id)
        return False
