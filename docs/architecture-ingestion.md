# Component 2: Ingestion Pipeline

> Part of [System Architecture](ARCHITECTURE.md)

**Purpose:** Read raw JSON from GCS and load it into BigQuery tables.

**Input:** JSON files in GCS (`authors_json/`, `publications_json/`).

**Output:** Rows appended to BigQuery tables (`scholar_raw_data.author`, `scholar_raw_data.pub`).

## What it does

1. Lists JSON files in GCS by date-prefix folders
2. Validates and wraps each JSON file under a `{"data": ...}` key
3. Creates NDJSON (newline-delimited JSON) in a temp GCS location
4. Loads NDJSON into BigQuery via `WRITE_APPEND`
5. On success: moves source files to archive prefixes
6. On failure: moves bad files to `dead_letter/` prefix

## Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | GCS (`authors_json/`, `publications_json/`) | |
| **Writes** | | BigQuery (`scholar_raw_data.author`, `scholar_raw_data.pub`) |
| | | GCS (archive moves, temp NDJSON, dead letter) |

## Deduplication

The raw tables use `WRITE_APPEND`, so they accumulate every historical version of each document. `_latest` views deduplicate to the most recent record per document using `ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY timestamp DESC)`. All downstream analytics read from these `_latest` views.

## Cadence

- **Default: daily batch.** This is a slow-moving field; daily is sufficient for routine operation.
- **On-demand:** When a user searches for an author not yet in the database, the frontend can trigger an immediate ingestion cycle after the crawler completes.

## Implementation

| File | Role |
|---|---|
| `ingestion/batch_load.py` | Cloud Function: GCS → NDJSON → BigQuery batch load |
| `ingestion/config.py` | Config with env var overrides |

## Infrastructure

- **Cloud Function (Gen2):** `batch_load_gcs_to_bq`, 1-hour timeout, 512MB memory
- **Cloud Scheduler:** Triggers the function daily
- **BigQuery schema:** Raw data stored as `{document_id, timestamp, data}` where `data` is a JSON string parsed by downstream views
