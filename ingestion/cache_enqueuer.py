"""Enqueue cache invalidation tasks after successful BigQuery loads.

After ingestion loads new data for authors, this module notifies the
Cache Layer to refresh those authors' cached data.
"""

import json
import logging
import os
import re

from google.cloud import tasks_v2

logger = logging.getLogger(__name__)

_client = None

# Config
_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "scholar-version2")
_QUEUE_LOCATION = os.environ.get("QUEUE_LOCATION", "northamerica-northeast1")
_QUEUE_NAME = os.environ.get("QUEUE_NAME_CACHE_PRIORITY", "cache-priority")
_CACHE_LAYER_URL = os.environ.get("CACHE_LAYER_URL", "")


def _get_client():
    global _client
    if _client is None:
        _client = tasks_v2.CloudTasksClient()
    return _client


def _queue_path():
    return (
        f"projects/{_PROJECT_ID}"
        f"/locations/{_QUEUE_LOCATION}"
        f"/queues/{_QUEUE_NAME}"
    )


def _extract_scholar_ids_from_ndjson_lines(ndjson_lines):
    """Extract unique scholar IDs from NDJSON lines.

    Author files have document_id = "{scholar_id}.json"
    Publication files have document_id = "{scholar_id}:{pub_id}.json"
    """
    scholar_ids = set()
    for line in ndjson_lines:
        try:
            row = json.loads(line)
            doc_id = row.get("document_id", "")
            # Remove .json suffix
            doc_id = re.sub(r"\.json$", "", doc_id)
            # Extract scholar_id (before the first colon, if any)
            scholar_id = doc_id.split(":")[0]
            if scholar_id:
                scholar_ids.add(scholar_id)
        except (json.JSONDecodeError, AttributeError):
            continue
    return scholar_ids


def enqueue_cache_invalidations(scholar_ids):
    """Enqueue invalidate_author tasks for a set of scholar IDs.

    Returns the number of tasks enqueued.
    """
    if not _CACHE_LAYER_URL:
        logger.info("CACHE_LAYER_URL not configured, skipping cache invalidation")
        return 0

    client = _get_client()
    queue = _queue_path()
    target_url = f"{_CACHE_LAYER_URL.rstrip('/')}/tasks/priority"
    enqueued = 0

    for scholar_id in scholar_ids:
        task_body = json.dumps({
            "type": "invalidate_author",
            "scholar_id": scholar_id,
        }).encode()

        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": target_url,
                "headers": {"Content-Type": "application/json"},
                "body": task_body,
            },
        }

        try:
            client.create_task(parent=queue, task=task)
            enqueued += 1
        except Exception:
            logger.exception("Failed to enqueue cache invalidation for %s", scholar_id)

    if enqueued:
        logger.info("Enqueued %d cache invalidation tasks", enqueued)
    return enqueued
