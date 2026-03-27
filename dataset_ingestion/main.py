"""Cloud Run Job entry point for S2 dataset ingestion.

Modes:
  - full: Download and load all datasets from a release (initial or forced rebuild).
  - diff: Apply incremental updates from the last loaded release to the latest.
  - auto: Check release_log and decide between full and diff.

Environment variables:
  INGESTION_MODE: "full", "diff", or "auto" (default: "auto")
  S2_RELEASE_ID: Specific release to load (default: "latest")
"""

import logging
import os
import sys

from dataset_ingestion import s2_api_client
from dataset_ingestion.config import Config
from dataset_ingestion.diff_updater import apply_diff
from dataset_ingestion.downloader import download_dataset
from dataset_ingestion.loader import (
    build_author_paper_stats,
    build_paper_citations_by_year,
    ensure_dataset_exists,
    get_last_loaded_release,
    load_dataset,
    log_release,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def run_full_load(release_id):
    """Download and load all datasets for a release.

    Raises RuntimeError if any dataset fails to download or load,
    preventing derived table rebuilds on inconsistent data.
    """
    logger.info("Starting full load for release %s", release_id)

    ensure_dataset_exists()
    failed_datasets = []

    for dataset_name in Config.DATASETS:
        logger.info("Processing dataset: %s", dataset_name)

        # Get download URLs
        file_urls = s2_api_client.get_dataset_files(release_id, dataset_name)
        logger.info("  %d files to download", len(file_urls))

        # Download to GCS
        dl_result = download_dataset(release_id, dataset_name, file_urls)
        if dl_result["failed"] > 0:
            logger.error(
                "  %d/%d downloads failed for %s",
                dl_result["failed"],
                dl_result["total"],
                dataset_name,
            )
            log_release(release_id, dataset_name, "full", "failed")
            failed_datasets.append(dataset_name)
            continue

        # Load from GCS into BigQuery
        try:
            rows = load_dataset(release_id, dataset_name, write_disposition="WRITE_TRUNCATE")
        except Exception:
            log_release(release_id, dataset_name, "full", "failed")
            failed_datasets.append(dataset_name)
            logger.exception("BigQuery load failed for %s", dataset_name)
            continue
        log_release(release_id, dataset_name, "full", "success", rows)

    if failed_datasets:
        raise RuntimeError(
            f"Full load aborted: {', '.join(failed_datasets)} failed. "
            "Derived tables NOT rebuilt to avoid inconsistent data."
        )

    # Build derived tables only when all base tables loaded successfully
    logger.info("Building derived tables...")
    build_paper_citations_by_year()
    build_author_paper_stats()
    log_release(release_id, "derived_tables", "full", "success")

    logger.info("Full load complete for release %s", release_id)


def _full_load_fallback(target_release, dataset_name):
    """Fall back to full reload for a single dataset.

    Raises RuntimeError if the fallback also fails. Always logs the
    outcome to release_log before raising so the latest status is
    accurate for subsequent auto-mode runs.
    """
    file_urls = s2_api_client.get_dataset_files(target_release, dataset_name)
    dl_result = download_dataset(target_release, dataset_name, file_urls)
    if dl_result["failed"] > 0:
        log_release(target_release, dataset_name, "full_fallback", "failed")
        raise RuntimeError(f"Full-load fallback failed for {dataset_name}")
    try:
        rows = load_dataset(target_release, dataset_name, write_disposition="WRITE_TRUNCATE")
    except Exception:
        log_release(target_release, dataset_name, "full_fallback", "failed")
        raise
    log_release(target_release, dataset_name, "full_fallback", "success", rows)


def run_diff_load(last_release, target_release):
    """Apply incremental diffs from last_release to target_release.

    Raises RuntimeError if any dataset fails to update (including fallback).
    """
    logger.info("Starting diff load: %s -> %s", last_release, target_release)
    failed_datasets = []

    for dataset_name in Config.DATASETS:
        logger.info("Getting diffs for %s", dataset_name)

        try:
            diff_data = s2_api_client.get_diffs(last_release, target_release, dataset_name)
        except Exception:
            logger.exception("Failed to get diffs for %s — falling back to full load", dataset_name)
            try:
                _full_load_fallback(target_release, dataset_name)
            except Exception:
                logger.exception("Full-load fallback also failed for %s", dataset_name)
                failed_datasets.append(dataset_name)
            continue

        diffs = diff_data.get("diffs", [])
        if not diffs:
            logger.info("No diffs for %s — already up to date", dataset_name)
            log_release(target_release, dataset_name, "diff", "success", 0)
            continue

        # Apply each diff step sequentially
        total_deleted = 0
        total_upserted = 0
        try:
            for diff in diffs:
                step_release = diff.get("to_release", target_release)
                logger.info("Applying diff step -> %s for %s", step_release, dataset_name)
                result = apply_diff(step_release, dataset_name, diff)
                total_deleted += result["deleted"]
                total_upserted += result["upserted"]

            logger.info(
                "Diff applied for %s: %d deleted, %d upserted",
                dataset_name, total_deleted, total_upserted,
            )
            log_release(target_release, dataset_name, "diff", "success", total_upserted)

        except Exception:
            logger.exception("Diff apply failed for %s — falling back to full load", dataset_name)
            try:
                _full_load_fallback(target_release, dataset_name)
            except Exception:
                logger.exception("Full-load fallback also failed for %s", dataset_name)
                failed_datasets.append(dataset_name)

    if failed_datasets:
        raise RuntimeError(
            f"Diff load aborted: {', '.join(failed_datasets)} failed. "
            "Derived tables NOT rebuilt to avoid inconsistent data."
        )

    # Rebuild derived tables only when all base tables updated successfully
    logger.info("Building derived tables...")
    build_paper_citations_by_year()
    build_author_paper_stats()
    log_release(target_release, "derived_tables", "diff", "success")

    logger.info("Diff load complete: %s -> %s", last_release, target_release)


def main():
    """Entry point for S2 dataset ingestion."""
    mode = os.environ.get("INGESTION_MODE", "auto")
    target_release = os.environ.get("S2_RELEASE_ID", "latest")

    # Resolve "latest" to an actual release ID
    if target_release == "latest":
        target_release = s2_api_client.get_latest_release_id()
    logger.info("Target release: %s", target_release)

    if mode == "auto":
        last_release = get_last_loaded_release()
        if last_release is None:
            logger.info("No previous load found — running full load")
            mode = "full"
        elif last_release == target_release:
            logger.info("Already up to date (release %s)", target_release)
            return
        else:
            logger.info("Last loaded: %s — running diff load", last_release)
            mode = "diff"

    if mode == "full":
        run_full_load(target_release)
    elif mode == "diff":
        last_release = get_last_loaded_release()
        if last_release is None:
            logger.warning("No previous load for diff mode — switching to full")
            run_full_load(target_release)
        else:
            run_diff_load(last_release, target_release)
    else:
        logger.error("Unknown mode: %s", mode)
        sys.exit(1)


if __name__ == "__main__":
    main()
