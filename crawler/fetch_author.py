"""Cloud Function entry point: fetch an author from Google Scholar.

Receives scholar_id → fetches full profile → serializes → uploads to GCS → enqueues pub tasks.
For priority (user-initiated) crawls, also triggers immediate GCS→BigQuery ingestion.
"""

import json
import logging
import time
import urllib.request

import functions_framework

from crawler.config import Config
from crawler.scholarly_client import (
    ErrorKind,
    ScholarlyError,
    fetch_author as _fetch_author,
    serialize_author,
)
from crawler.gcs_writer import author_blob_path, upload_json
from crawler.task_enqueuer import enqueue_publications

logger = logging.getLogger(__name__)


def _trigger_batch_load():
    """Fire-and-forget call to the batch_load function for immediate ingestion."""
    url = Config.function_url(Config.BATCH_LOAD_FUNCTION)
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info("Triggered batch load: HTTP %s", resp.status)
    except Exception:
        logger.exception("Failed to trigger batch load (non-fatal)")


@functions_framework.http
def v3_fetch_author(request):
    """HTTP entry point for the fetch_author Cloud Function."""
    start = time.time()

    body = request.get_json(silent=True) or {}
    scholar_id = request.args.get("scholar_id") or body.get("scholar_id")
    skip_pubs = request.args.get("skip_pubs") or body.get("skip_pubs")
    priority = body.get("priority", False)

    if not scholar_id:
        return json.dumps({"error": "scholar_id is required"}), 400

    request_id = request.headers.get("Function-Execution-Id", "unknown")
    logger.info(f"[{request_id}] Fetching author: {scholar_id} (priority={priority})")

    try:
        author = _fetch_author(scholar_id)
    except ScholarlyError as exc:
        elapsed = time.time() - start
        logger.error(f"[{request_id}] Failed to fetch {scholar_id}: {exc} (kind={exc.kind.value}, {elapsed:.1f}s)")
        status = 429 if exc.kind == ErrorKind.TRANSIENT else 500
        return json.dumps({"error": str(exc), "kind": exc.kind.value}), status

    serialized = serialize_author(author)
    blob_path = author_blob_path(scholar_id)
    upload_json(serialized, blob_path)

    if not skip_pubs:
        pubs = author.get("publications", [])
        enqueue_publications(pubs)
        logger.info(f"[{request_id}] Enqueued {len(pubs)} publication tasks for {scholar_id}")

    # For user-initiated crawls, trigger immediate GCS→BigQuery ingestion
    if priority:
        _trigger_batch_load()

    elapsed = time.time() - start
    logger.info(f"[{request_id}] Completed author {scholar_id} in {elapsed:.1f}s")
    return json.dumps(serialized), 200
