"""Download S2 dataset files from pre-signed URLs to GCS.

Streams data directly from S2's S3 bucket to GCS without buffering
the entire file in memory.
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
from google.cloud import storage

from dataset_ingestion.config import Config

logger = logging.getLogger(__name__)

_storage_client = None


def _get_storage_client():
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client(project=Config.PROJECT_ID)
    return _storage_client


def _gcs_blob_name(release_id, dataset_name, file_url):
    """Extract a filename from the S2 URL and build GCS blob path."""
    parsed = urlparse(file_url)
    # URLs look like: .../staging/2026-03-10/papers/20260313_070637_00001.gz
    filename = os.path.basename(parsed.path)
    return f"{Config.S2_DATASETS_PREFIX}{release_id}/{dataset_name}/{filename}"


def _blob_exists(blob_name):
    """Check if a GCS blob already exists (for resume support)."""
    bucket = _get_storage_client().bucket(Config.BUCKET_NAME)
    return bucket.blob(blob_name).exists()


def download_file(release_id, dataset_name, file_url):
    """Download a single file from S2 to GCS via streaming.

    Args:
        release_id: S2 release ID.
        dataset_name: e.g. "papers", "citations", "authors".
        file_url: Pre-signed S3 URL.

    Returns:
        GCS blob name of the uploaded file.
    """
    blob_name = _gcs_blob_name(release_id, dataset_name, file_url)

    if _blob_exists(blob_name):
        logger.info("Skipping %s (already exists)", blob_name)
        return blob_name

    logger.info("Downloading %s -> gs://%s/%s", dataset_name, Config.BUCKET_NAME, blob_name)

    bucket = _get_storage_client().bucket(Config.BUCKET_NAME)
    blob = bucket.blob(blob_name)

    # Stream from S2 URL directly to GCS using resumable upload
    with requests.get(file_url, stream=True, timeout=3600) as r:
        r.raise_for_status()
        content_type = r.headers.get("Content-Type", "application/gzip")

        with blob.open("wb", content_type=content_type) as gcs_writer:
            for chunk in r.iter_content(chunk_size=Config.DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    gcs_writer.write(chunk)

    logger.info("Completed %s", blob_name)
    return blob_name


def download_dataset(release_id, dataset_name, file_urls):
    """Download all files for a dataset using parallel workers.

    Args:
        release_id: S2 release ID.
        dataset_name: e.g. "papers", "citations", "authors".
        file_urls: List of pre-signed S3 URLs.

    Returns:
        Dict with keys: total, downloaded, skipped, failed, blobs.
    """
    results = {"total": len(file_urls), "downloaded": 0, "skipped": 0, "failed": 0, "blobs": []}

    logger.info(
        "Downloading %s: %d files to gs://%s/%s",
        dataset_name,
        len(file_urls),
        Config.BUCKET_NAME,
        Config.gcs_dataset_prefix(release_id, dataset_name),
    )

    with ThreadPoolExecutor(max_workers=Config.DOWNLOAD_WORKERS) as executor:
        future_to_url = {
            executor.submit(download_file, release_id, dataset_name, url): url
            for url in file_urls
        }
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                blob_name = future.result()
                results["blobs"].append(blob_name)
                # Check if it was a skip or a fresh download
                if _blob_exists(blob_name):
                    results["skipped"] += 1
                results["downloaded"] += 1
            except Exception:
                logger.exception("Failed to download %s", url)
                results["failed"] += 1

    logger.info(
        "Download %s complete: %d downloaded, %d skipped, %d failed",
        dataset_name,
        results["downloaded"],
        results["skipped"],
        results["failed"],
    )
    return results
