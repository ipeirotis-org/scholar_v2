"""Google Scholar search fallback via scholarly.

Used only when local BigQuery search returns insufficient results.
"""

import logging

from scholarly import scholarly

from author_search.config import Config

logger = logging.getLogger(__name__)


def search_scholar(author_name, max_results=None):
    """Search Google Scholar for authors by name.

    Returns a list of normalized author dicts.
    This is the expensive, rate-limited path — only called when
    local search is insufficient.
    """
    if max_results is None:
        max_results = Config.MAX_SCHOLAR_RESULTS

    authors = []
    try:
        search_query = scholarly.search_author(author_name)
        for _ in range(max_results):
            try:
                author = next(search_query)
                if author:
                    authors.append(_normalize_author(author))
            except StopIteration:
                break
    except Exception:
        logger.exception("Scholar search failed for: %s", author_name)
    return authors


def _normalize_author(author):
    """Normalize a scholarly author dict into the standard response format."""
    return {
        "scholar_id": author.get("scholar_id", ""),
        "name": author.get("name", ""),
        "affiliation": author.get("affiliation", ""),
        "email_domain": author.get("email_domain", ""),
        "citedby": author.get("citedby", 0),
        "hindex": 0,
    }
