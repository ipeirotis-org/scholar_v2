"""Wrapper around scholarly with timeout handling, retry logic, and error classification."""

import copy
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from enum import Enum

from crawler.config import Config

logger = logging.getLogger(__name__)


class ErrorKind(Enum):
    TRANSIENT = "transient"
    PERMANENT = "permanent"


class ScholarlyError(Exception):
    """Error from scholarly with classified kind."""

    def __init__(self, message, kind):
        super().__init__(message)
        self.kind = kind


_TRANSIENT_MARKERS = ["429", "rate limit", "captcha", "timeout", "connection", "cannot fetch"]


def _classify_error(exc):
    msg = str(exc).lower()
    for marker in _TRANSIENT_MARKERS:
        if marker in msg:
            return ErrorKind.TRANSIENT
    return ErrorKind.PERMANENT


def _run_with_timeout(fn, timeout):
    """Run fn() in a thread with a timeout. Returns the result or raises."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fn)
        return future.result(timeout=timeout)


def fetch_author(scholar_id, timeout=None):
    """Fetch a full author profile from Google Scholar.

    Returns the filled author dict.
    Raises ScholarlyError on failure with kind = TRANSIENT or PERMANENT.
    """
    timeout = timeout or Config.SCHOLARLY_TIMEOUT

    def _fetch():
        from scholarly import scholarly

        author = scholarly.search_author_id(scholar_id)
        return scholarly.fill(
            author,
            sections=["basics", "indices", "counts", "coauthors", "publications"],
        )

    return _call_with_retry(_fetch, timeout, context=f"author {scholar_id}")


def fetch_publication(pub_data, timeout=None):
    """Fetch full publication details from Google Scholar.

    pub_data should be a publication dict (as returned in author.publications).
    Returns the filled publication dict.
    Raises ScholarlyError on failure.
    """
    timeout = timeout or 60

    def _fetch():
        from scholarly import scholarly, PublicationSource

        pub = copy.deepcopy(pub_data)
        if "source" not in pub:
            pub["source"] = PublicationSource.AUTHOR_PUBLICATION_ENTRY
        if "container_type" not in pub:
            pub["container_type"] = "Publication"
        return scholarly.fill(pub)

    return _call_with_retry(_fetch, timeout, context=f"publication {pub_data.get('author_pub_id', '?')}")


def _call_with_retry(fn, timeout, context, max_retries=2):
    """Call fn with timeout and retry on transient errors.

    Retries up to max_retries times with exponential backoff (2s, 4s).
    """
    last_error = None
    for attempt in range(1 + max_retries):
        try:
            return _run_with_timeout(fn, timeout)
        except FuturesTimeoutError:
            last_error = ScholarlyError(
                f"Timeout after {timeout}s fetching {context}", ErrorKind.TRANSIENT
            )
            logger.warning(f"Attempt {attempt + 1}: {last_error}")
        except Exception as exc:
            kind = _classify_error(exc)
            last_error = ScholarlyError(
                f"Error fetching {context}: {exc}", kind
            )
            if kind == ErrorKind.PERMANENT:
                raise last_error
            logger.warning(f"Attempt {attempt + 1}: transient error fetching {context}: {exc}")

        if attempt < max_retries:
            backoff = 2 ** (attempt + 1)
            time.sleep(backoff)

    raise last_error


def convert_large_integers(data):
    """Convert integers > 2^62 to strings for BigQuery compatibility."""
    if isinstance(data, dict):
        return {k: convert_large_integers(v) for k, v in data.items()}
    if isinstance(data, list):
        return [convert_large_integers(v) for v in data]
    if isinstance(data, int) and abs(data) > 2**62:
        return str(data)
    return data


def serialize_author(author):
    """Serialize a scholarly author object for GCS storage.

    Simplifies publications to lightweight entries (id, citations, title, year).
    Converts large integers to strings.
    """
    author = copy.deepcopy(author)
    simplified_pubs = []
    for pub in author.get("publications", []):
        simplified_pubs.append({
            "author_pub_id": pub.get("author_pub_id"),
            "num_citations": pub.get("num_citations", 0),
            "filled": False,
            "bib": {
                "title": pub.get("bib", {}).get("title"),
                "pub_year": pub.get("bib", {}).get("pub_year"),
            },
        })
    author["publications"] = simplified_pubs
    author = convert_large_integers(author)
    return json.loads(json.dumps(author, default=str))


def serialize_publication(pub):
    """Serialize a scholarly publication object for GCS storage."""
    pub = convert_large_integers(copy.deepcopy(pub))
    return json.loads(json.dumps(pub, default=str))
