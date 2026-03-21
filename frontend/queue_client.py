"""Thin client to enqueue cache population tasks on cache miss.

The frontend enqueues to the cache-priority queue when Firestore
doesn't have the requested data. The Cache Layer picks up the task
and populates the cache.
"""

import json
import logging

from google.cloud import tasks_v2

from frontend.config import Config

logger = logging.getLogger(__name__)

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = tasks_v2.CloudTasksClient()
    return _client


def _queue_path():
    return (
        f"projects/{Config.PROJECT_ID}"
        f"/locations/{Config.QUEUE_LOCATION}"
        f"/queues/{Config.QUEUE_NAME_CACHE_PRIORITY}"
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
