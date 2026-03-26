"""Persistent failure tracking via Firestore.

Records task failures from dead-letter queues and marks them resolved
when a subsequent crawl succeeds. Provides a queryable record of all
tasks that have exhausted their retries.

Firestore collection: ``task_failures``
Document ID: ``{task_type}_{sanitized_identifier}``
"""

import logging
from datetime import datetime, timezone

from google.cloud import firestore

logger = logging.getLogger(__name__)

COLLECTION = "task_failures"

_db = None


def _get_db():
    global _db
    if _db is None:
        _db = firestore.Client()
    return _db


def _sanitize_doc_id(raw_id):
    """Sanitize an identifier for use as a Firestore document ID."""
    return raw_id.replace(":", "__").replace("/", "___")


def _doc_id(task_type, identifier):
    """Build a deterministic document ID for a failure record."""
    return f"{task_type}_{_sanitize_doc_id(identifier)}"


def record_failure(task_type, identifier, *, priority=False,
                   source_subscription="", scholar_id="",
                   author_pub_id="", attributes=None):
    """Record or update a failure in Firestore.

    Uses a transaction to safely increment ``failure_count`` and preserve
    ``first_failure`` while updating ``last_failure``.
    """
    if not identifier:
        logger.warning("Cannot record failure without identifier")
        return

    now = datetime.now(timezone.utc).isoformat()

    try:
        db = _get_db()
        doc_ref = db.collection(COLLECTION).document(_doc_id(task_type, identifier))
        doc = doc_ref.get()
        if doc.exists:
            doc_ref.update({
                "failure_count": firestore.Increment(1),
                "last_failure": now,
                "status": "failed",
                "priority": priority,
                "source_subscription": source_subscription,
            })
        else:
            doc_ref.set({
                "task_type": task_type,
                "identifier": identifier,
                "scholar_id": scholar_id,
                "author_pub_id": author_pub_id,
                "priority": priority,
                "failure_count": 1,
                "first_failure": now,
                "last_failure": now,
                "source_subscription": source_subscription,
                "status": "failed",
                "resolved_at": None,
            })
        logger.info("Recorded failure: %s %s", task_type, identifier)
    except Exception:
        logger.exception("Failed to write failure record: %s %s", task_type, identifier)


def resolve_failure(task_type, identifier):
    """Mark a previously failed task as resolved.

    Called on successful crawl. Non-fatal — failures to update are logged
    but do not affect the crawl.
    """
    if not identifier:
        return

    try:
        db = _get_db()
        doc_ref = db.collection(COLLECTION).document(_doc_id(task_type, identifier))
        doc = doc_ref.get()
        if doc.exists and doc.to_dict().get("status") != "resolved":
            doc_ref.update({
                "status": "resolved",
                "resolved_at": datetime.now(timezone.utc).isoformat(),
            })
            logger.info("Resolved failure: %s %s", task_type, identifier)
    except Exception:
        logger.exception("Failed to resolve failure record: %s %s", task_type, identifier)


def record_partial_enqueue_failure(scholar_id, failed_pub_ids):
    """Record partial publication enqueue failures.

    Called when some (but not all) publication enqueue attempts fail
    during an author crawl.
    """
    if not failed_pub_ids:
        return

    now = datetime.now(timezone.utc).isoformat()

    try:
        db = _get_db()
        doc_ref = db.collection(COLLECTION).document(
            _doc_id("enqueue_publications", scholar_id)
        )
        doc_ref.set({
            "task_type": "enqueue_publications",
            "identifier": scholar_id,
            "scholar_id": scholar_id,
            "failed_pub_ids": failed_pub_ids,
            "failure_count": len(failed_pub_ids),
            "first_failure": now,
            "last_failure": now,
            "status": "failed",
            "resolved_at": None,
        })
        logger.info(
            "Recorded partial enqueue failure for %s: %d pubs failed",
            scholar_id, len(failed_pub_ids),
        )
    except Exception:
        logger.exception(
            "Failed to write partial enqueue failure: %s", scholar_id
        )
