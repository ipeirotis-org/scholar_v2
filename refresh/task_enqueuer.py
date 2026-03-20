"""Enqueue author fetch tasks to Cloud Tasks.

This is the only module that writes to Cloud Tasks queues.
The Refresh & Expand service uses this to instruct the Crawler
what to fetch.
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


def enqueue_author(scholar_id):
    """Enqueue a task to fetch an author from Google Scholar.

    Returns True if enqueued, False if duplicate (already exists).
    Raises on other errors.
    """
    client = _get_client()
    queue_path = Config.queue_path()
    task_name = f"{queue_path}/tasks/{_sanitize_task_id(scholar_id)}"
    url = Config.function_url("fetch_author")

    task = {
        "name": task_name,
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"scholar_id": scholar_id}).encode(),
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
