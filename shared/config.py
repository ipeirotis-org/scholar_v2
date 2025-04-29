import os
import datetime

# Define the pool of regions where functions are deployed
# IMPORTANT: This list should match the regions to in the functions.yml GitHub Action!
AVAILABLE_FUNCTION_REGIONS = [
    "us-central1", 
    "us-east1", 
    "us-east4", 
    "us-south1", 
    "us-west1"
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

   # --- Dynamic Function Location based on Day ---
    @staticmethod
    def get_daily_region():
        """Selects a region from the available pool based on the day of the year."""
        today = datetime.date.today()
        day_of_year = today.timetuple().tm_yday
        region_index = (day_of_year - 1) % len(AVAILABLE_FUNCTION_REGIONS) # Cycle through regions
        return AVAILABLE_FUNCTION_REGIONS[region_index]    

    FUNCTION_LOCATION = get_daily_region()
    print(f"Config: Using Function Location for today: {FUNCTION_LOCATION}") # for logging/debugging

    API_SEARCH_AUTHOR_ID = (
        f"https://{FUNCTION_LOCATION}-{PROJECT_ID}.cloudfunctions.net/search_author_id"
    )
    API_FILL_PUBLICATION = (
        f"https://{FUNCTION_LOCATION}-{PROJECT_ID}.cloudfunctions.net/fill_publication"
    )
    API_FIND_SCHOLAR_ID = (
        f"https://{FUNCTION_LOCATION}-{PROJECT_ID}.cloudfunctions.net/find_scholar_id_from_name"
    )

    BUCKET_NAME = "scholar_data_share"

    DOWNLOADS_DIR = os.getenv("DOWNLOADS_DIR", "downloads")
    STATIC_DIR = os.getenv("STATIC_DIR", "static")
