"""Upload JSON data to Google Cloud Storage with retry on failure."""

import json
import logging
import time

from google.cloud import storage

from crawler.config import Config

logger = logging.getLogger(__name__)

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = storage.Client(project=Config.PROJECT_ID)
    return _client


def upload_json(data, blob_path, max_retries=2):
    """Upload a dict as JSON to GCS.

    Args:
        data: Dict to serialize and upload.
        blob_path: Full path within the bucket (e.g. "authors_json/2026/03/19/abc123.json").
        max_retries: Number of retries on transient failure.

    Raises:
        Exception on persistent failure.
    """
    client = _get_client()
    bucket = client.bucket(Config.BUCKET_NAME)
    blob = bucket.blob(blob_path)
    payload = json.dumps(data, default=str)

    last_error = None
    for attempt in range(1 + max_retries):
        try:
            blob.upload_from_string(payload, content_type="application/json")
            logger.info(f"Uploaded gs://{Config.BUCKET_NAME}/{blob_path}")
            return
        except Exception as exc:
            last_error = exc
            logger.warning(f"GCS upload attempt {attempt + 1} failed for {blob_path}: {exc}")
            if attempt < max_retries:
                time.sleep(2 ** (attempt + 1))

    raise last_error


def author_blob_path(scholar_id):
    """Return the GCS blob path for an author JSON file."""
    return f"authors_json/{Config.gcs_date_prefix()}/{scholar_id}.json"


def publication_blob_path(author_pub_id):
    """Return the GCS blob path for a publication JSON file."""
    sanitized = author_pub_id.replace(":", "_").replace("/", "___")
    return f"publications_json/{Config.gcs_date_prefix()}/{sanitized}.json"
