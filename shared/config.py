import os
import datetime
import pytz

# Define the pool of regions where functions are deployed
# IMPORTANT: This list should match the regions to in the functions.yml GitHub Action!
AVAILABLE_FUNCTION_REGIONS = [
    "us-central1",
    "us-east1",
    "us-east4",
    "us-east5",
    "us-south1",
    "us-west1",
    "us-west2",
    "us-west3",
    "us-west4",
]


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "default-secret-key")
    PROJECT_ID = "scholar-version2"

    QUEUE_LOCATION = "northamerica-northeast1"
    QUEUE_NAME_AUTHORS = "process-authors"
    QUEUE_NAME_PUBS = "process-pubs"
    QUEUE_PATH_AUTHORS = (
        f"projects/{PROJECT_ID}/locations/{QUEUE_LOCATION}/queues/{QUEUE_NAME_AUTHORS}"
    )
    QUEUE_PATH_PUBS = (
        f"projects/{PROJECT_ID}/locations/{QUEUE_LOCATION}/queues/{QUEUE_NAME_PUBS}"
    )

    FIRESTORE_COLLECTION_AUTHOR = "scholar_raw_author"
    FIRESTORE_COLLECTION_PUB = "scholar_raw_pub"

    @staticmethod
    def get_hourly_region():
        """
        Selects a region cyclically based on the current UTC hour.

        We select different regions over time to avoid sending too many requests
        from one region and getting potentially rate-limited or blocked.
        """
        # Use UTC time to ensure consistency regardless of server timezone
        now_utc = datetime.datetime.now(pytz.utc)

        # Calculate total hours since the Unix epoch for a continuously cycling value
        # Using timestamp() gives seconds since epoch (Jan 1, 1970)
        total_hours_since_epoch = int(now_utc.timestamp() // 3600)

        # Calculate the index using modulo
        num_regions = len(AVAILABLE_FUNCTION_REGIONS)
        if num_regions == 0:
            raise ValueError(
                "AVAILABLE_FUNCTION_REGIONS cannot be empty."
            )  # Added error handling

        region_index = total_hours_since_epoch % num_regions
        return AVAILABLE_FUNCTION_REGIONS[region_index]

    FUNCTION_LOCATION = get_hourly_region()
    print(
        f"Config: Using Function Location: {FUNCTION_LOCATION}"
    )  # for logging/debugging

    API_SEARCH_AUTHOR_ID = (
        f"https://{FUNCTION_LOCATION}-{PROJECT_ID}.cloudfunctions.net/search_author_id"
    )
    API_FILL_PUBLICATION = (
        f"https://{FUNCTION_LOCATION}-{PROJECT_ID}.cloudfunctions.net/fill_publication"
    )
    API_FIND_SCHOLAR_ID = f"https://{FUNCTION_LOCATION}-{PROJECT_ID}.cloudfunctions.net/find_scholar_id_from_name"

    BUCKET_NAME = "scholar_data_share"

    DOWNLOADS_DIR = os.getenv("DOWNLOADS_DIR", "downloads")
    STATIC_DIR = os.getenv("STATIC_DIR", "static")
