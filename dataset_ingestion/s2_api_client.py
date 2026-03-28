"""Client for the Semantic Scholar Datasets API.

Handles release listing, dataset file URL retrieval, and incremental diffs.
API docs: https://api.semanticscholar.org/api-docs/datasets
"""

import logging

import requests

from dataset_ingestion.config import Config

logger = logging.getLogger(__name__)

_api_key = None


def _get_api_key():
    """Retrieve S2 API key from env var or Secret Manager (cached)."""
    global _api_key
    if _api_key is not None:
        return _api_key

    import os

    # Prefer env var (set by Cloud Run Job config or CI)
    key = os.environ.get("S2_API_KEY")
    if key:
        _api_key = key.strip()
        return _api_key

    # Fall back to Secret Manager
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=Config.S2_API_KEY_SECRET)
    _api_key = response.payload.data.decode("utf-8").strip()
    return _api_key


def _request(path, timeout=30):
    """Make an authenticated GET request to the S2 Datasets API."""
    url = f"{Config.S2_API_BASE}{path}"
    headers = {"x-api-key": _get_api_key()}
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def list_releases():
    """Return list of available release IDs (date strings, ascending)."""
    return _request("/release/")


def get_latest_release_id():
    """Return the most recent release ID."""
    releases = list_releases()
    if not releases:
        raise RuntimeError("No S2 releases available")
    return releases[-1]


def get_release(release_id="latest"):
    """Return release metadata including dataset summaries.

    Returns dict with keys: release_id, README, datasets.
    Each dataset has: name, description, README.
    """
    return _request(f"/release/{release_id}")


def get_dataset_files(release_id, dataset_name):
    """Return list of pre-signed download URLs for a dataset.

    Args:
        release_id: Release date string (e.g. "2026-03-10") or "latest".
        dataset_name: One of "papers", "citations", "authors", etc.

    Returns:
        List of pre-signed S3 URLs (strings).
    """
    data = _request(f"/release/{release_id}/dataset/{dataset_name}")
    return data.get("files", [])


def get_diffs(start_release, end_release, dataset_name):
    """Return incremental diff info between two releases.

    Args:
        start_release: Release ID the client currently has.
        end_release: Target release ID (or "latest").
        dataset_name: Dataset to diff.

    Returns:
        Dict with keys: dataset, start_release, end_release, diffs.
        Each diff has: from_release, to_release, update_files, delete_files.
    """
    return _request(
        f"/diffs/{start_release}/to/{end_release}/{dataset_name}",
        timeout=60,
    )
