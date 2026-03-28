"""HTTP entry point for the Author Search Cloud Function.

Deployed as a Cloud Function. Searches for authors by name across
the in-memory index, BigQuery statistics views, and optionally the
full S2 authors universe (102M authors).
"""

import json
import logging

import functions_framework

from author_search.search_service import AuthorSearchService

logger = logging.getLogger(__name__)

_service = None


def _get_service():
    global _service
    if _service is None:
        _service = AuthorSearchService()
    return _service


@functions_framework.http
def v3_search_authors(request):
    """Search for authors by name.

    Query params:
        author_name: The name to search for (required, min 2 chars)
        scholar: If "true", also search the full S2 universe (slower)

    Returns JSON array of matching authors.
    """
    author_name = request.args.get("author_name", "").strip()
    if not author_name or len(author_name) < 2:
        return json.dumps([]), 200

    scholar = request.args.get("scholar", "").lower() == "true"

    try:
        results = _get_service().search(author_name, scholar=scholar)
        return json.dumps(results), 200
    except Exception:
        logger.exception("Search failed for: %s", author_name)
        return json.dumps({"error": "Search failed"}), 500
