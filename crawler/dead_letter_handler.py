"""Cloud Function: handle dead-lettered Cloud Tasks from all queues.

When a task in any queue exhausts all retries, Cloud Tasks forwards it
to a dead-letter Pub/Sub topic. This function subscribes to those topics
and:
1. Logs structured error events to Cloud Logging (for alerting).
2. Writes a persistent failure record to Firestore (for dashboards and recovery).

Handles both crawler tasks (fetch_author, fetch_publication) and cache
tasks (populate_author_profile, invalidate_author, warm_author, etc.).
"""

import base64
import json
import logging

import functions_framework

from crawler.failure_tracker import record_failure

logger = logging.getLogger(__name__)

# Cache task types use a "type" field in the body
_CACHE_TASK_TYPES = {
    "populate_author_profile",
    "populate_publication_detail",
    "invalidate_author",
    "warm_author",
    "populate_recent_authors",
    "rebuild_all",
}


def _classify_task(task_body):
    """Determine the task type and identifier from the task body.

    Returns (task_type, identifier, scholar_id, author_pub_id).
    """
    # Crawler tasks: identified by scholar_id or pub dict
    scholar_id = task_body.get("scholar_id", "")
    pub_data = task_body.get("pub", {})
    author_pub_id = (
        pub_data.get("author_pub_id", "") if isinstance(pub_data, dict) else ""
    )

    # Cache tasks: identified by "type" field
    cache_type = task_body.get("type", "")

    if cache_type in _CACHE_TASK_TYPES:
        # Cache task — identifier is scholar_id or author_pub_id from payload
        identifier = (
            scholar_id
            or task_body.get("author_pub_id", "")
            or cache_type
        )
        return cache_type, identifier, scholar_id, task_body.get("author_pub_id", "")

    if scholar_id:
        return "fetch_author", scholar_id, scholar_id, ""

    if author_pub_id:
        return "fetch_publication", author_pub_id, "", author_pub_id

    return "unknown", "", "", ""


@functions_framework.http
def v3_dead_letter_handler(request):
    """HTTP entry point for Pub/Sub push subscription."""
    envelope = request.get_json(silent=True)
    if not envelope:
        return json.dumps({"error": "no Pub/Sub message received"}), 400

    message = envelope.get("message", {})
    data_b64 = message.get("data", "")

    # Decode the original task body from the Pub/Sub message
    task_body = {}
    if data_b64:
        try:
            raw = base64.b64decode(data_b64)
            task_body = json.loads(raw)
        except Exception:
            logger.warning("Could not decode dead-letter message data")

    task_type, identifier, scholar_id, author_pub_id = _classify_task(task_body)
    priority = task_body.get("priority", False)

    # Extract Pub/Sub message attributes (Cloud Tasks may include queue info)
    attributes = message.get("attributes", {})
    subscription = envelope.get("subscription", "")

    # Log structured error for Cloud Logging / Monitoring
    error_event = {
        "event": "task_dead_lettered",
        "task_type": task_type,
        "scholar_id": scholar_id,
        "author_pub_id": author_pub_id,
        "priority": priority,
        "subscription": subscription,
        "attributes": attributes,
        "message": f"Task exhausted all retries: {task_type} {identifier}",
    }

    logger.error(json.dumps(error_event))

    # Persist failure record to Firestore for dashboard and recovery
    record_failure(
        task_type=task_type,
        identifier=identifier,
        priority=priority,
        source_subscription=subscription,
        scholar_id=scholar_id,
        author_pub_id=author_pub_id,
        attributes=attributes,
    )

    return json.dumps({"status": "logged", "task_type": task_type, "identifier": identifier}), 200
