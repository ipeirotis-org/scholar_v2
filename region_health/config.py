"""Single source of truth for region list and health-scoring constants."""

# All regions where crawler / author_search Cloud Functions are deployed.
# US regions (original 9) + Canada + Europe + Asia for IP diversity.
AVAILABLE_FUNCTION_REGIONS = [
    # --- US (original) ---
    "us-central1",
    "us-east1",
    "us-east4",
    "us-east5",
    "us-south1",
    "us-west1",
    "us-west2",
    "us-west3",
    "us-west4",
    # --- Canada ---
    "northamerica-northeast1",  # Montreal
    "northamerica-northeast2",  # Toronto
    # --- Europe ---
    "europe-west1",  # Belgium
    "europe-west2",  # London
    # --- Asia ---
    "asia-east1",      # Taiwan
    "asia-northeast1",  # Tokyo
]

# Cloud Functions to monitor for health scoring
CLOUD_FUNCTION_NAMES = [
    "v3_fetch_author",
    "v3_fetch_publication",
]

# Firestore collection for persisted health scores
HEALTH_COLLECTION = "region_health"

# Cloud Monitoring lookback window (hours) for computing error rates
HEALTH_WINDOW_HOURS = 3

# Minimum weight — never fully exclude a region so it can recover
MIN_WEIGHT = 0.05

# Default weight for regions with no recent monitoring data
DEFAULT_WEIGHT = 0.5

# Sensitivity multiplier: weight = max(MIN, 1.0 - error_rate * MULTIPLIER)
ERROR_RATE_MULTIPLIER = 3.0

# In-memory cache TTL for region weights (seconds)
SCORE_CACHE_TTL_SECONDS = 900  # 15 minutes

# How often the background scorer thread runs (seconds)
SCORER_INTERVAL_SECONDS = 600  # 10 minutes
