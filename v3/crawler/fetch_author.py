"""Cloud Function entry point: fetch an author from Google Scholar.

Receives scholar_id → fetches full profile → serializes → uploads to GCS → enqueues pub tasks.
"""

import json
import logging
import time

import functions_framework

from v3.crawler.scholarly_client import (
    ErrorKind,
    ScholarlyError,
    fetch_author as _fetch_author,
    serialize_author,
)
from v3.crawler.gcs_writer import author_blob_path, upload_json
from v3.crawler.task_enqueuer import enqueue_publications

logger = logging.getLogger(__name__)


@functions_framework.http
def v3_fetch_author(request):
    """HTTP entry point for the fetch_author Cloud Function."""
    start = time.time()

    scholar_id = (
        request.args.get("scholar_id")
        or (request.get_json(silent=True) or {}).get("scholar_id")
    )
    skip_pubs = (
        request.args.get("skip_pubs")
        or (request.get_json(silent=True) or {}).get("skip_pubs")
    )

    if not scholar_id:
        return json.dumps({"error": "scholar_id is required"}), 400

    request_id = request.headers.get("Function-Execution-Id", "unknown")
    logger.info(f"[{request_id}] Fetching author: {scholar_id}")

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

    elapsed = time.time() - start
    logger.info(f"[{request_id}] Completed author {scholar_id} in {elapsed:.1f}s")
    return json.dumps(serialized), 200
