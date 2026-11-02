"""Client for SerpApi's Google Scholar Author API.

Only profile pages are fetched (engine=google_scholar_author, 100 articles per
request). SerpApi can return HTTP 200 with an error message or an empty page,
so every response is validated before it is treated as a success.
API docs: https://serpapi.com/google-scholar-author-api
"""

import logging
import os
import time

import requests

from gs_crawler.config import Config

logger = logging.getLogger(__name__)

_api_key = None


class SerpApiError(Exception):
    """A request that did not yield a valid profile page."""

    def __init__(self, message, transient):
        super().__init__(message)
        self.transient = transient


def _get_api_key():
    """Retrieve the SerpApi key from env var or Secret Manager (cached)."""
    global _api_key
    if _api_key is not None:
        return _api_key

    key = os.environ.get("SERPAPI_KEY")
    if key:
        _api_key = key.strip()
        return _api_key

    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=Config.SERPAPI_KEY_SECRET)
    _api_key = response.payload.data.decode("utf-8").strip()
    return _api_key


def validate_author_page(data, start):
    """Raise SerpApiError unless `data` is a usable author page at offset `start`."""
    if not isinstance(data, dict):
        raise SerpApiError("response is not a JSON object", transient=True)
    if "error" in data:
        msg = str(data["error"])
        # "Google hasn't returned any results" past the last page is not an error.
        if start > 0 and "hasn't returned any results" in msg:
            return
        transient = not any(s in msg.lower() for s in ("invalid", "not found", "doesn't exist"))
        raise SerpApiError(f"SerpApi error: {msg}", transient=transient)
    status = (data.get("search_metadata") or {}).get("status")
    if status and status != "Success":
        raise SerpApiError(f"search status {status}", transient=True)
    if start == 0:
        if not (data.get("author") or {}).get("name"):
            raise SerpApiError("first page has no author name", transient=True)
        if not data.get("articles"):
            raise SerpApiError("first page has no articles", transient=True)


def fetch_author_page(author_id, start=0):
    """Fetch one profile page (up to 100 articles). Retries transient failures."""
    params = {
        "engine": "google_scholar_author",
        "author_id": author_id,
        "start": start,
        "num": Config.PAGE_SIZE,
        "hl": "en",
        "no_cache": "true",
        "api_key": _get_api_key(),
    }
    last_error = None
    for attempt in range(1 + Config.SERPAPI_MAX_RETRIES):
        try:
            response = requests.get(
                f"{Config.SERPAPI_BASE}/search.json", params=params, timeout=Config.SERPAPI_TIMEOUT
            )
            if response.status_code in (429,) or response.status_code >= 500:
                raise SerpApiError(f"HTTP {response.status_code}", transient=True)
            data = response.json()
            if response.status_code != 200 and "error" not in data:
                raise SerpApiError(f"HTTP {response.status_code}", transient=False)
            validate_author_page(data, start)
            return data
        except (requests.RequestException, ValueError) as exc:
            # Never log the request URL: it carries the API key.
            last_error = SerpApiError(f"request failed: {type(exc).__name__}", transient=True)
        except SerpApiError as exc:
            last_error = exc
            if not exc.transient:
                raise
        if attempt < Config.SERPAPI_MAX_RETRIES:
            time.sleep(2 ** (attempt + 1))
    logger.warning("SerpApi page failed for %s start=%s: %s", author_id, start, last_error)
    raise last_error


def get_account():
    """Return SerpApi account info (plan, searches left). Free: not counted as a search."""
    response = requests.get(
        f"{Config.SERPAPI_BASE}/account.json",
        params={"api_key": _get_api_key()},
        timeout=Config.SERPAPI_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()
