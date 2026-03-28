"""Semantic Scholar author search via the S2 Graph API.

Used as a fallback when the in-memory index doesn't find enough results.
The index covers ~3M authors (hindex >= 10, citedby > 500); this API
reaches the full 102M S2 author universe for less-known researchers.

API docs: https://api.semanticscholar.org/api-docs/graph#tag/Author-Data
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

    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(name=Config.S2_API_KEY_SECRET)
        _api_key = response.payload.data.decode("utf-8").strip()
        return _api_key
    except Exception:
        logger.warning("Could not load S2 API key; fallback search disabled")
        return ""


def search_authors(author_name, max_results=10):
    """Search Semantic Scholar for authors by name.

    Returns a list of normalized author dicts, or empty list on failure.
    """
    api_key = _get_api_key()
    if not api_key:
        return []

    try:
        response = requests.get(
            f"{S2_GRAPH_API_BASE}/author/search",
            params={
                "query": author_name,
                "fields": S2_AUTHOR_SEARCH_FIELDS,
                "limit": max_results,
            },
            headers={"x-api-key": api_key},
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

    author_id = author.get("authorId")
    if author_id is None:
        author_id = ""
    else:
        author_id = str(author_id)

    return {
        "scholar_id": author_id,
        "name": author.get("name", ""),
        "affiliation": affiliation,
        "email_domain": "",
        "citedby": author.get("citationCount") or 0,
        "hindex": author.get("hIndex") or 0,
        "source": "s2_api",
    }
