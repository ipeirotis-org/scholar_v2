"""Health-aware region selection with in-memory caching."""

import logging
import random
import threading
import time
from datetime import datetime, timezone

from region_health.config import (
    AVAILABLE_FUNCTION_REGIONS,
    DEFAULT_WEIGHT,
    HEALTH_COLLECTION,
    SCORE_CACHE_TTL_SECONDS,
)

logger = logging.getLogger(__name__)

# Module-level in-memory cache (shared across the process).
_cache_lock = threading.Lock()
_cached_weights: dict[str, float] = {}
_cache_expiry: float = 0.0  # epoch seconds


def _load_weights_from_firestore(firestore_client=None, project_id="scholar-version2"):
    """Read region weights from Firestore.

    Returns ``{region: weight}`` or ``{}`` on failure.
    """
    try:
        if firestore_client is None:
            from google.cloud import firestore
            firestore_client = firestore.Client(project=project_id)
        docs = firestore_client.collection(HEALTH_COLLECTION).stream()
        weights = {}
        for doc in docs:
            data = doc.to_dict()
            region = data.get("region", doc.id)
            weights[region] = data.get("weight", DEFAULT_WEIGHT)
        return weights
    except Exception:
        logger.exception("Failed to load region weights from Firestore")
        return {}


def get_region_weights(firestore_client=None, project_id="scholar-version2"):
    """Return ``{region: weight}`` with in-memory TTL cache.

    Falls back to uniform DEFAULT_WEIGHT if Firestore data is unavailable.
    """
    global _cached_weights, _cache_expiry

    now = time.monotonic()
    with _cache_lock:
        if now < _cache_expiry and _cached_weights:
            return dict(_cached_weights)

    # Cache miss — read from Firestore (outside lock to avoid blocking)
    weights = _load_weights_from_firestore(firestore_client, project_id)

    # Ensure all known regions have a weight
    full_weights = {r: DEFAULT_WEIGHT for r in AVAILABLE_FUNCTION_REGIONS}
    full_weights.update(weights)

    with _cache_lock:
        _cached_weights = full_weights
        _cache_expiry = time.monotonic() + SCORE_CACHE_TTL_SECONDS

    return dict(full_weights)


def select_region(firestore_client=None, project_id="scholar-version2"):
    """Select a region using weighted random selection.

    Regions with higher health weights are selected more often.
    Intended for per-request use (frontend priority crawls).
    """
    weights = get_region_weights(firestore_client, project_id)
    regions = list(weights.keys())
    region_weights = [weights[r] for r in regions]
    return random.choices(regions, weights=region_weights, k=1)[0]


def select_best_region(firestore_client=None, project_id="scholar-version2"):
    """Return the region with the highest health weight.

    Intended for batch use (refresh service). Ties are broken randomly.
    """
    weights = get_region_weights(firestore_client, project_id)
    max_weight = max(weights.values())
    best_regions = [r for r, w in weights.items() if w == max_weight]
    return random.choice(best_regions)


def get_rotating_region(regions=None):
    """Select a region based on the current UTC day.

    Backward-compatible deterministic rotation:
    ``(hours_since_epoch // 24) % len(regions)``.
    """
    regions = regions or AVAILABLE_FUNCTION_REGIONS
    now_utc = datetime.now(timezone.utc)
    total_hours = int(now_utc.timestamp() // 3600)
    return regions[(total_hours // 24) % len(regions)]


def invalidate_cache():
    """Clear the in-memory cache (for testing)."""
    global _cached_weights, _cache_expiry
    with _cache_lock:
        _cached_weights = {}
        _cache_expiry = 0.0
