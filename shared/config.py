import os


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

    FUNCTION_LOCATION = "northamerica-northeast2"
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
