"""Drop Firestore cache entries for authors whose GS snapshot was just refreshed.

The frontend serves cached profiles without checking BigQuery, so without this
a refreshed author keeps showing old metrics. Deleting the entries makes the
next visit a cache miss, which the cache layer repopulates from gs_statistics.
Collection names must match cache_layer/config.py.
"""

import logging

from google.cloud import firestore

from gs_crawler.config import Config

logger = logging.getLogger(__name__)

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
