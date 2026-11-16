"""Live Google Scholar profile lookup by name, through SerpApi.

Google Scholar's own author search needs a signed-in browser, but a regular
Scholar search for a name still returns a "User profiles for <name>" block,
and links the authors of the listed papers to their profiles. SerpApi exposes
both (engine=google_scholar: `profiles.authors` and
`organic_results[].publication_info.authors`), which is enough to turn a name
into Google Scholar IDs. Checked 2026-11-16.

One request is one paid SerpApi search; the caller caches results and caps
lookups per day (see AuthorSearchService._supplement_with_gs).
API docs: https://serpapi.com/google-scholar-api
"""

import logging
import os
import re
import unicodedata

import requests

from author_search.config import Config

logger = logging.getLogger(__name__)

SERPAPI_SEARCH_URL = "https://serpapi.com/search.json"

_GS_ID_RE = re.compile(r"^[A-Za-z0-9_-]{12}$")
_EMAIL_DOMAIN_RE = re.compile(r"\bat\s+([a-z0-9.-]+\.[a-z]{2,})\s*$", re.IGNORECASE)

_api_key = None


def _get_api_key():
    """Retrieve the SerpApi key from env var or Secret Manager (cached)."""
    global _api_key
    if _api_key:
        return _api_key

    key = os.environ.get("SERPAPI_KEY")
    if key:
        _api_key = key.strip()
        return _api_key

    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(name=Config.SERPAPI_KEY_SECRET)
        _api_key = response.payload.data.decode("utf-8").strip()
        return _api_key
    except Exception:
        logger.warning("Could not load the SerpApi key; live Google Scholar search disabled")
        return ""


def _plain(text):
    """Lowercase letters without accents, for comparing names."""
    text = unicodedata.normalize("NFKD", (text or "").lower())
    return "".join(c for c in text if not unicodedata.combining(c))


def _name_words(text):
    return [w for w in re.split(r"[^\w'’-]+", _plain(text)) if w]


def _same_person_abbreviated(query_words, abbreviated):
    """Whether a paper's author label ("I Triantafyllopoulos") fits the name searched for.

    The surname must be the query's last word, and when the query has a first
    name, the label's initials must start with its first letter.
    """
    words = _name_words(abbreviated)
    if not words or not query_words or words[-1] != query_words[-1]:
        return False
    if len(query_words) == 1 or len(words) == 1:
        return True
    return words[0][0] == query_words[0][0]


def parse_profiles(data, name, max_results=10):
    """Author dicts from a SerpApi Google Scholar response, profile block first.

    Each has scholar_id, name, affiliation, email_domain, citedby, hindex (None:
    the search page does not show it) and source "gs_live". Authors known only
    from a paper's byline get that paper as their description, since the page
    gives no affiliation for them.
    """
    results, seen = [], set()

    for author in ((data.get("profiles") or {}).get("authors") or []):
        sid = author.get("author_id") or ""
        if not _GS_ID_RE.match(sid) or sid in seen or not author.get("name"):
            continue
        seen.add(sid)
        domain = _EMAIL_DOMAIN_RE.search(author.get("email") or "")
        results.append({
            "scholar_id": sid,
            "name": author["name"],
            "affiliation": author.get("affiliations") or "",
            "email_domain": domain.group(1).lower() if domain else "",
            "citedby": author.get("cited_by"),
            "hindex": None,
            "source": "gs_live",
        })

    query_words = _name_words(name)
    for paper in (data.get("organic_results") or []):
        for author in ((paper.get("publication_info") or {}).get("authors") or []):
            sid = author.get("author_id") or ""
            if (not _GS_ID_RE.match(sid) or sid in seen
                    or not _same_person_abbreviated(query_words, author.get("name"))):
                continue
            seen.add(sid)
            title = (paper.get("title") or "").strip()
            if len(title) > 70:
                title = title[:67].rstrip() + "..."
            results.append({
                "scholar_id": sid,
                "name": author["name"],
                "affiliation": f"Author of “{title}”" if title else "",
                "email_domain": "",
                "citedby": None,
                "hindex": None,
                "source": "gs_live",
            })

    return results[:max_results]


def search_profiles(name, max_results=10):
    """Look a name up on Google Scholar.

    Returns a list of author dicts (empty when Google Scholar shows no profile
    for the name), or None when the lookup itself failed and should not be cached.
    """
    api_key = _get_api_key()
    if not api_key:
        return None

    params = {"engine": "google_scholar", "q": name, "hl": "en", "num": 20, "api_key": api_key}
    try:
        resp = requests.get(SERPAPI_SEARCH_URL, params=params, timeout=Config.GS_LIVE_TIMEOUT_SECONDS)
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        # Not logger.exception: the request URL in the traceback carries the API key.
        logger.warning("Live Google Scholar search failed for '%s': %s", name, type(exc).__name__)
        return None

    if not isinstance(data, dict):
        return None
    error = data.get("error")
    if error:
        if "hasn't returned any results" in str(error):
            return []
        logger.warning("Live Google Scholar search failed for '%s': %s", name, error)
        return None
    if (data.get("search_metadata") or {}).get("status") != "Success":
        logger.warning("Live Google Scholar search for '%s' did not complete", name)
        return None

    results = parse_profiles(data, name, max_results)
    logger.info("Live Google Scholar search '%s': %d profiles", name, len(results))
    return results
