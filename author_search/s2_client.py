"""Semantic Scholar author search via the S2 Graph API.

Used only when local BigQuery search returns insufficient results.
Replaces the previous Google Scholar (scholarly) fallback.

API docs: https://api.semanticscholar.org/api-docs/graph#tag/Author-Data/operation/get_graph_get_author_search
"""

import logging
import os

import requests

from author_search.config import Config

logger = logging.getLogger(__name__)

_api_key = None

S2_GRAPH_API_BASE = "https://api.semanticscholar.org/graph/v1"
S2_AUTHOR_SEARCH_FIELDS = "authorId,name,affiliations,citationCount,hIndex,paperCount"


def _get_api_key():
    """Retrieve S2 API key from env var or Secret Manager (cached)."""
    global _api_key
    if _api_key is not None:
        return _api_key

    key = os.environ.get("S2_API_KEY")
    if key:
        _api_key = key.strip()
        return _api_key

    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=Config.S2_API_KEY_SECRET)
    _api_key = response.payload.data.decode("utf-8").strip()
    return _api_key


def search_authors(author_name, max_results=None):
    """Search Semantic Scholar for authors by name.

    Returns a list of normalized author dicts matching the standard
    response format used throughout author_search.
    """
    if max_results is None:
        max_results = Config.MAX_S2_RESULTS

    try:
        response = requests.get(
            f"{S2_GRAPH_API_BASE}/author/search",
            params={
                "query": author_name,
                "fields": S2_AUTHOR_SEARCH_FIELDS,
                "limit": max_results,
            },
            headers={"x-api-key": _get_api_key()},
            timeout=Config.S2_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
        return [_normalize_author(a) for a in data.get("data", []) if a]
    except requests.exceptions.Timeout:
        logger.warning("S2 author search timed out for: %s", author_name)
    except requests.exceptions.HTTPError as e:
        logger.warning("S2 author search HTTP %s for: %s", e.response.status_code, author_name)
    except Exception:
        logger.exception("S2 author search failed for: %s", author_name)
    return []


def _normalize_author(author):
    """Normalize an S2 author dict into the standard response format."""
    affiliations = author.get("affiliations") or []
    affiliation = affiliations[0] if affiliations else ""

    return {
        "scholar_id": str(author.get("authorId", "")),
        "name": author.get("name", ""),
        "affiliation": affiliation,
        "email_domain": "",
        "citedby": author.get("citationCount") or 0,
        "hindex": author.get("hIndex") or 0,
        "id_type": "s2",
    }
