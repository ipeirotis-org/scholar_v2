"""Cache Layer Cloud Run entry points.

Handles HTTP requests from Cloud Tasks (priority and batch queues)
and admin endpoints for manual operations.
"""

import json
import logging
import os

from flask import Flask, jsonify, request

from functools import wraps

from cache_layer.cache_service import CacheService
from cache_layer.config import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
service = CacheService()


def require_admin_auth(f):
    """Require Bearer token authentication for admin endpoints.

    When CACHE_LAYER_ADMIN_TOKEN is set, requests must include a matching
    Authorization: Bearer <token> header. When unset (empty), all requests
    are allowed — this preserves backwards compatibility for environments
    where Cloud Run IAM or ingress restrictions provide auth instead.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        token = Config.ADMIN_AUTH_TOKEN
        if not token:
            return f(*args, **kwargs)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer ") or auth_header[7:] != token:
            return jsonify({"status": "error", "message": "Unauthorized"}), 401

        return f(*args, **kwargs)
    return decorated


@app.route("/tasks/priority", methods=["POST"])
def handle_priority_task():
    """Handle a task from the priority (interactive) queue."""
    return _handle_task()


@app.route("/tasks/batch", methods=["POST"])
def handle_batch_task():
    """Handle a task from the batch (background) queue."""
    return _handle_task()


def _handle_task():
    """Common handler for both queues. Dispatches by request type."""
    try:
        body = request.get_json(force=True)
    except Exception:
        return jsonify({"status": "error", "message": "Invalid JSON body"}), 400

    request_type = body.get("type")
    if not request_type:
        return jsonify({"status": "error", "message": "Missing 'type' field"}), 400

    logger.info("Processing task: type=%s", request_type)
    result = service.dispatch(request_type, body)

    status = result.get("status")
    if status == "error":
        status_code = 400
    elif status == "not_found":
        status_code = 404
    else:
        status_code = 200
    return jsonify(result), status_code


@app.route("/admin/populate", methods=["POST"])
@require_admin_auth
def admin_populate():
    """Manually populate cache for a specific author.

    Useful for debugging or one-off cache population.
    """
    try:
        body = request.get_json(force=True)
    except Exception:
        return jsonify({"status": "error", "message": "Invalid JSON body"}), 400

    scholar_id = body.get("scholar_id")
    if not scholar_id:
        return jsonify({"status": "error", "message": "Missing scholar_id"}), 400

    result = service.dispatch("populate_author_profile", {"scholar_id": scholar_id})
    return jsonify(result)


@app.route("/admin/flush_cache", methods=["POST"])
@require_admin_auth
def admin_flush_cache():
    """Delete all documents from all Firestore cache collections.

    The cache is fully disposable — it repopulates on-demand as users
    query the frontend (cache miss triggers priority queue population).
    """
    result = service.dispatch("flush_cache", {})
    status = result.get("status")
    if status == "ok":
        status_code = 200
    elif status == "partial_failure":
        status_code = 207
    else:
        status_code = 500
    return jsonify(result), status_code


@app.route("/admin/purge_legacy", methods=["POST"])
@require_admin_auth
def admin_purge_legacy():
    """Purge non-S2 (legacy Google Scholar) entries from all cache collections.

    S2 author IDs are purely numeric. This deletes any cache entries
    with non-numeric document IDs (legacy Google Scholar IDs).

    Returns 200 on full success, 207 on partial failure, 500 on total failure.
    """
    result = service.dispatch("purge_legacy_cache", {})
    status = result.get("status")
    if status == "ok":
        status_code = 200
    elif status == "partial_failure":
        status_code = 207
    else:
        status_code = 500
    return jsonify(result), status_code


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8081))
    app.run(host="0.0.0.0", port=port)
