#!/usr/bin/env python3
"""Backfill script: enqueue all stale authors for re-crawl via Cloud Tasks.

Uses the Cloud Tasks REST API (not gRPC) for compatibility with environments
that have SSL proxy restrictions.

Usage:
    # Dry run (just count, don't enqueue):
    python scripts/backfill_authors.py --dry-run

    # Enqueue all stale authors (default: 90 days threshold):
    python scripts/backfill_authors.py

    # Custom staleness threshold (e.g., re-crawl everything older than 30 days):
    python scripts/backfill_authors.py --threshold-days 30

    # Limit how many to enqueue:
    python scripts/backfill_authors.py --limit 500

    # Spread tasks across multiple regions (round-robin):
    python scripts/backfill_authors.py --spread-regions
"""

import argparse
import base64
import json
import logging
import sys
import time
from datetime import datetime, timezone

import google.auth
import google.auth.transport.requests
import requests
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ID = "scholar-version2"
QUEUE_LOCATION = "northamerica-northeast1"
QUEUE_NAME = "process-authors"

REGIONS = [
    "us-central1",
    "us-east1",
    "us-east4",
    "us-east5",
    "us-west1",
    "us-west2",
    "us-west3",
    "us-west4",
    "us-south1",
]

CLOUD_TASKS_API = "https://cloudtasks.googleapis.com/v2"


def get_access_token():
    """Get an OAuth2 access token using application default credentials."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(google.auth.transport.requests.Request())
    return credentials.token


def get_stale_authors(threshold_days, limit=None):
    """Query BigQuery for authors not updated within threshold_days."""
    client = bigquery.Client(project=PROJECT_ID)

    limit_clause = f"LIMIT {limit}" if limit else ""
    sql = f"""
        SELECT
            document_id AS scholar_id,
            timestamp AS last_updated
        FROM `{PROJECT_ID}.scholar_raw_data.author_latest`
        WHERE timestamp < TIMESTAMP_SUB(
            CURRENT_TIMESTAMP(), INTERVAL {threshold_days} DAY
        )
        ORDER BY timestamp ASC
        {limit_clause}
    """
    rows = list(client.query(sql).result())
    return [(r.scholar_id, r.last_updated) for r in rows]


def sanitize_task_id(raw_id):
    """Sanitize an ID for use as a Cloud Tasks task name."""
    return raw_id.replace(":", "__").replace("/", "___")


def enqueue_authors(authors, spread_regions=False, batch_size=100):
    """Enqueue authors to Cloud Tasks via REST API.

    Uses a backfill-specific task name suffix to avoid conflicts with
    the daily scheduler's tasks.
    """
    queue_path = f"projects/{PROJECT_ID}/locations/{QUEUE_LOCATION}/queues/{QUEUE_NAME}"
    api_url = f"{CLOUD_TASKS_API}/{queue_path}/tasks"

    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Use epoch seconds as a unique suffix for this backfill run
    backfill_id = int(time.time())

    enqueued = 0
    duplicates = 0
    errors = 0
    total = len(authors)

    # Token refresh interval (refresh every 50 minutes to be safe)
    token_refresh_interval = 3000
    last_token_refresh = time.time()

    for i, (scholar_id, last_updated) in enumerate(authors):
        # Refresh token periodically
        if time.time() - last_token_refresh > token_refresh_interval:
            token = get_access_token()
            headers["Authorization"] = f"Bearer {token}"
            last_token_refresh = time.time()
            logger.info("Refreshed access token")

        # Pick region: round-robin across regions or use today's rotation
        if spread_regions:
            region = REGIONS[i % len(REGIONS)]
        else:
            now_utc = datetime.now(timezone.utc)
            total_hours = int(now_utc.timestamp() // 3600)
            region = REGIONS[(total_hours // 24) % len(REGIONS)]

        function_url = f"https://{region}-{PROJECT_ID}.cloudfunctions.net/fetch_author"
        task_name = f"{queue_path}/tasks/backfill-{backfill_id}-{sanitize_task_id(scholar_id)}"

        body_bytes = json.dumps({"scholar_id": scholar_id}).encode()
        body_b64 = base64.b64encode(body_bytes).decode()

        task_body = {
            "task": {
                "name": task_name,
                "httpRequest": {
                    "httpMethod": "POST",
                    "url": function_url,
                    "headers": {"Content-Type": "application/json"},
                    "body": body_b64,
                },
            }
        }

        try:
            resp = requests.post(api_url, headers=headers, json=task_body, timeout=30)
            if resp.status_code == 200 or resp.status_code == 201:
                enqueued += 1
            elif resp.status_code == 409:
                duplicates += 1
            else:
                errors += 1
                if errors <= 10:
                    logger.error(
                        "Failed to enqueue %s: HTTP %d: %s",
                        scholar_id, resp.status_code, resp.text[:200],
                    )
        except Exception as exc:
            errors += 1
            if errors <= 10:
                logger.error("Failed to enqueue %s: %s", scholar_id, exc)

        # Progress logging
        if (i + 1) % batch_size == 0 or i + 1 == total:
            logger.info(
                "Progress: %d/%d (enqueued=%d, duplicates=%d, errors=%d)",
                i + 1, total, enqueued, duplicates, errors,
            )

    return enqueued, duplicates, errors


def main():
    parser = argparse.ArgumentParser(description="Backfill author crawls via Cloud Tasks")
    parser.add_argument("--threshold-days", type=int, default=90,
                        help="Re-crawl authors not updated within this many days (default: 90)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max authors to enqueue (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Just query and report counts, don't enqueue")
    parser.add_argument("--spread-regions", action="store_true",
                        help="Distribute tasks across all 9 regions (round-robin)")
    args = parser.parse_args()

    logger.info("Querying stale authors (threshold=%d days)...", args.threshold_days)
    authors = get_stale_authors(args.threshold_days, limit=args.limit)
    logger.info("Found %d stale authors", len(authors))

    if not authors:
        logger.info("Nothing to do.")
        return

    # Show staleness summary
    oldest = authors[0][1]
    newest = authors[-1][1]
    logger.info("Oldest: %s, Newest: %s", oldest, newest)

    if args.dry_run:
        logger.info("Dry run — no tasks enqueued.")
        return

    logger.info("Enqueuing %d authors (spread_regions=%s)...", len(authors), args.spread_regions)
    enqueued, duplicates, errors = enqueue_authors(authors, spread_regions=args.spread_regions)

    logger.info("Backfill complete: enqueued=%d, duplicates=%d, errors=%d", enqueued, duplicates, errors)

    if errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
