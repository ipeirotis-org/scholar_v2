"""Enqueue author and publication fetch tasks to Cloud Tasks."""

import json
import logging
import time

from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2

from v3.crawler.config import Config

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
    queue_path = Config.queue_path(Config.QUEUE_NAME_AUTHORS)
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
        logger.info(f"Enqueued author task: {scholar_id}")
        return True
    except AlreadyExists:
        logger.info(f"Author task already exists: {scholar_id}")
        return False


def enqueue_publication(pub_entry, delay=None):
    """Enqueue a task to fetch a publication from Google Scholar.

    Returns True if enqueued, False if duplicate.
    Raises on other errors.
    """
    client = _get_client()
    author_pub_id = pub_entry.get("author_pub_id", "")
    queue_path = Config.queue_path(Config.QUEUE_NAME_PUBS)
    task_name = f"{queue_path}/tasks/{_sanitize_task_id(author_pub_id)}"
    url = Config.function_url("fetch_publication")

    task = {
        "name": task_name,
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"pub": pub_entry}).encode(),
        },
    }

    try:
        client.create_task(parent=queue_path, task=task)
        logger.info(f"Enqueued publication task: {author_pub_id}")
        return True
    except AlreadyExists:
        logger.info(f"Publication task already exists: {author_pub_id}")
        return False


def enqueue_publications(publications, delay=None):
    """Enqueue tasks for a list of publications with stagger delay.

    Args:
        publications: List of publication dicts (from author.publications).
        delay: Seconds between enqueue calls (default: Config.PUB_ENQUEUE_DELAY).

    Returns:
        Count of newly enqueued tasks.
    """
    delay = delay if delay is not None else Config.PUB_ENQUEUE_DELAY
    count = 0
    for pub in publications:
        if enqueue_publication(pub):
            count += 1
        if delay > 0:
            time.sleep(delay)
    logger.info(f"Enqueued {count}/{len(publications)} publication tasks")
    return count
