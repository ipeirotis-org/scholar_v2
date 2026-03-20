"""HTTP entry points for the Refresh & Expand service.

Deployed as a Cloud Run service with endpoints for:
- Scheduled tasks (called by Cloud Scheduler)
- User-triggered actions (called by the frontend)
"""

import json
import logging
import re
import time

import functions_framework

from refresh import refresh_service

logger = logging.getLogger(__name__)

# Validate scholar_id: alphanumeric, hyphens, underscores, 4-20 chars
SCHOLAR_ID_RE = re.compile(r"^[A-Za-z0-9_-]{4,20}$")


def _validate_scholar_id(scholar_id):
    if not scholar_id or not SCHOLAR_ID_RE.match(scholar_id):
        return None
    return scholar_id


def _get_request_id(request):
    return request.headers.get("Function-Execution-Id", "unknown")


def _get_int_param(request, name, default):
    """Extract an integer parameter from query args or JSON body."""
    val = request.args.get(name)
    if val is not None:
        try:
            return int(val)
        except ValueError:
            return default
    body = request.get_json(silent=True) or {}
    val = body.get(name)
    if val is not None:
        try:
            return int(val)
        except (ValueError, TypeError):
            return default
    return default


@functions_framework.http
def refresh_stale(request):
    """Scheduled: find stale authors and enqueue for re-crawl."""
    request_id = _get_request_id(request)
    start = time.time()
    logger.info("[%s] Starting stale author refresh", request_id)

    limit = _get_int_param(request, "limit", None)
    result = refresh_service.refresh_stale_authors(limit=limit)

    elapsed = time.time() - start
    logger.info("[%s] Stale refresh complete: %s (%.1fs)", request_id, result, elapsed)
    return json.dumps(result), 200


@functions_framework.http
def refresh_errors(request):
    """Scheduled: find error authors and enqueue for re-crawl."""
    request_id = _get_request_id(request)
    start = time.time()
    logger.info("[%s] Starting error author refresh", request_id)

    limit = _get_int_param(request, "limit", None)
    result = refresh_service.refresh_error_authors(limit=limit)

    elapsed = time.time() - start
    logger.info("[%s] Error refresh complete: %s (%.1fs)", request_id, result, elapsed)
    return json.dumps(result), 200


@functions_framework.http
def expand_coauthors(request):
    """Scheduled: find coauthors not in DB and enqueue for initial crawl."""
    request_id = _get_request_id(request)
    start = time.time()
    logger.info("[%s] Starting coauthor expansion", request_id)

    limit = _get_int_param(request, "limit", None)
    result = refresh_service.expand_coauthors(limit=limit)

    elapsed = time.time() - start
    logger.info("[%s] Coauthor expansion complete: %s (%.1fs)", request_id, result, elapsed)
    return json.dumps(result), 200


@functions_framework.http
def fetch_author(request):
    """User-triggered: enqueue a single author for crawl."""
    request_id = _get_request_id(request)
    start = time.time()

    body = request.get_json(silent=True) or {}
    scholar_id = request.args.get("scholar_id") or body.get("scholar_id", "")
    scholar_id = _validate_scholar_id(scholar_id.strip())

    if not scholar_id:
        return json.dumps({"error": "A valid scholar_id is required"}), 400

    logger.info("[%s] Fetching author: %s", request_id, scholar_id)
    result = refresh_service.fetch_author(scholar_id)

    elapsed = time.time() - start
    logger.info("[%s] Fetch author complete: %s (%.1fs)", request_id, result, elapsed)
    return json.dumps(result), 200


@functions_framework.http
def fetch_authors(request):
    """User-triggered: enqueue multiple authors for crawl."""
    request_id = _get_request_id(request)
    start = time.time()

    body = request.get_json(silent=True) or {}
    raw_ids = request.args.get("scholar_ids") or body.get("scholar_ids", "")

    if isinstance(raw_ids, list):
        id_list = raw_ids
    else:
        id_list = [s.strip() for s in raw_ids.split(",") if s.strip()]

    scholar_ids = [sid for sid in id_list if _validate_scholar_id(sid)]
    if not scholar_ids:
        return json.dumps({"error": "No valid scholar IDs provided"}), 400

    logger.info("[%s] Fetching %d authors", request_id, len(scholar_ids))
    result = refresh_service.fetch_authors(scholar_ids)

    elapsed = time.time() - start
    logger.info("[%s] Fetch authors complete: %s (%.1fs)", request_id, result, elapsed)
    return json.dumps(result), 200
