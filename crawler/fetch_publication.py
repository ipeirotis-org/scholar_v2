"""Cloud Function entry point: fetch a publication from Google Scholar.

Receives pub data → fetches full details → serializes → uploads to GCS.
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
    fetch_publication as _fetch_publication,
    serialize_publication,
)
from crawler.gcs_writer import publication_blob_path, upload_json

logger = logging.getLogger(__name__)


def _trigger_batch_load():
    """Fire-and-forget call to the batch_load function for immediate ingestion."""
    url = Config.batch_load_url()
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info("Triggered batch load: HTTP %s", resp.status)
    except Exception:
        logger.exception("Failed to trigger batch load (non-fatal)")


@functions_framework.http
def v3_fetch_publication(request):
    """HTTP entry point for the fetch_publication Cloud Function."""
    start = time.time()

    body = request.get_json(silent=True) or {}
    pub_data = body.get("pub")
    priority = body.get("priority", False)

    if not pub_data or not isinstance(pub_data, dict):
        return json.dumps({"error": "pub object is required"}), 400

    author_pub_id = pub_data.get("author_pub_id")
    if not author_pub_id:
        return json.dumps({"error": "pub must contain author_pub_id"}), 400

    request_id = request.headers.get("Function-Execution-Id", "unknown")
    logger.info(f"[{request_id}] Fetching publication: {author_pub_id} (priority={priority})")

    try:
        filled = _fetch_publication(pub_data)
    except ScholarlyError as exc:
        elapsed = time.time() - start
        logger.error(f"[{request_id}] Failed to fetch {author_pub_id}: {exc} (kind={exc.kind.value}, {elapsed:.1f}s)")
        status = 429 if exc.kind == ErrorKind.TRANSIENT else 500
        return json.dumps({"error": str(exc), "kind": exc.kind.value}), status

    serialized = serialize_publication(filled)
    blob_path = publication_blob_path(author_pub_id)
    upload_json(serialized, blob_path)

    # For user-initiated crawls, trigger immediate GCS→BigQuery ingestion
    if priority:
        _trigger_batch_load()

    elapsed = time.time() - start
    logger.info(f"[{request_id}] Completed publication {author_pub_id} in {elapsed:.1f}s")
    return json.dumps(serialized), 200
