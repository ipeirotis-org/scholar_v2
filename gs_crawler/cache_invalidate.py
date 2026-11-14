"""Drop Firestore cache entries for authors whose GS snapshot was just refreshed.

The frontend serves cached profiles without checking BigQuery, so without this
a refreshed author keeps showing old metrics. Deleting the entries makes the
next visit a cache miss, which the cache layer repopulates from gs_statistics.
Collection names must match cache_layer/config.py.
"""

import logging
import re

from google.cloud import firestore

from gs_crawler.config import Config

logger = logging.getLogger(__name__)

_GS_ID_RE = re.compile(r"^[A-Za-z0-9_-]{12}$")
_S2_ID_RE = re.compile(r"^\d+$")

AUTHOR_CACHE_COLLECTIONS = (
    "v3_author_freshness",  # first: the frontend checks freshness before the data
    "v3_author_stats",
    "v3_author_pub_stats",
    "v3_author_temporal",
)

_db = None


def _client():
    global _db
    if _db is None:
        _db = firestore.Client(project=Config.PROJECT_ID)
    return _db


def invalidate_authors(scholar_ids, db=None):
    """Best-effort delete of the author-keyed cache docs. Returns docs deleted."""
    if not scholar_ids:
        return 0
    try:
        db = db or _client()
    except Exception:
        logger.exception("Firestore unavailable; cached profiles not invalidated")
        return 0
    deleted = 0
    for scholar_id in scholar_ids:
        for collection in AUTHOR_CACHE_COLLECTIONS:
            try:
                db.collection(collection).document(scholar_id).delete()
                deleted += 1
            except Exception:
                logger.exception("Failed to invalidate %s/%s", collection, scholar_id)
    logger.info("Invalidated cache for %d authors", len(scholar_ids))
    return deleted


def invalidate_missing_pip(db=None):
    """Invalidate cached GS author profiles that predate the PiP (pip_score) migration."""
    try:
        db = db or _client()
    except Exception:
        logger.exception("Firestore unavailable; pre-PiP cache scan skipped")
        return 0
    stale_ids = []
    for doc in db.collection("v3_author_stats").stream():
        sid = doc.id
        if not _GS_ID_RE.match(sid) or _S2_ID_RE.match(sid):
            continue
        payload = (doc.to_dict() or {}).get("data")
        if not isinstance(payload, dict) or "pip_score" not in payload:
            stale_ids.append(sid)
    if stale_ids:
        invalidate_authors(stale_ids, db=db)
    logger.info("Invalidated %d pre-PiP GS author cache entries", len(stale_ids))
    return len(stale_ids)
