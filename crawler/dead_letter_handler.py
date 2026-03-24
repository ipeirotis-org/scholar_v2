"""Cloud Function: handle dead-lettered Cloud Tasks from priority queues.

When a task in process-authors-priority or process-pub-priority exhausts
all retries, Cloud Tasks forwards it to the crawler-task-deadletter
Pub/Sub topic. This function subscribes to that topic and logs structured
error events so they appear in Cloud Logging and can trigger alerts.
"""

import base64
import json
import logging

import functions_framework

logger = logging.getLogger(__name__)


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

    # Extract identifiers from the task body
    scholar_id = task_body.get("scholar_id", "")
    pub_data = task_body.get("pub", {})
    author_pub_id = pub_data.get("author_pub_id", "") if isinstance(pub_data, dict) else ""
    priority = task_body.get("priority", False)

    # Determine task type
    if scholar_id:
        task_type = "fetch_author"
        identifier = scholar_id
    elif author_pub_id:
        task_type = "fetch_publication"
        identifier = author_pub_id
    else:
        task_type = "unknown"
        identifier = ""

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

    return json.dumps({"status": "logged", "task_type": task_type, "identifier": identifier}), 200
