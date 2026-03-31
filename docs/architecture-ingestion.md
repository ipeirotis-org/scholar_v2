# Component 1: Dataset Ingestion

> Part of [System Architecture](ARCHITECTURE.md)

**Purpose:** Download Semantic Scholar bulk datasets and load them into BigQuery. Also materializes the full analytics DAG after loading.

**Input:** S2 Datasets API (bulk files hosted on S3).

**Output:** BigQuery tables (`s2_data.*`) + materialized analytics tables (`statistics.*`).

## What it does

1. **Check for new releases:** Queries the S2 Datasets API for available releases; compares against `s2_data.release_log` to determine if a new release is available
2. **Download:** Streams dataset files from S2's S3 bucket to GCS (`s2_datasets/` prefix) using 4-8 parallel workers
3. **Load into BigQuery:** Bulk-loads papers, citations, and authors into `s2_data` dataset tables
4. **Build derived tables:** Creates `author_paper_bridge`, `author_paper_stats`, and `paper_citations_by_year` from the loaded data
5. **Apply diffs:** For incremental updates, applies DELETE+MERGE operations from S2 diff releases
6. **Materialize analytics DAG:** Runs `materialize_tables.py` to materialize 15 tables across 6 levels — app-facing tables, dist tables, and substitution targets
7. **Log completion:** Records the release ID and status in `s2_data.release_log`

## Modes

| Mode | Description |
|------|-------------|
| `full` | Download and load all datasets from a release (initial setup or forced rebuild) |
| `diff` | Apply incremental updates from the last loaded release to the latest |
| `auto` | Check `release_log` and decide between full and diff (default) |

## Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | S2 Datasets API (S3 bulk files) | |
| **Writes** | | GCS (`s2_datasets/` prefix) |
| | | BigQuery (`s2_data.*` tables) |
| | | BigQuery (`statistics.*` materialized tables) |
| | | BigQuery (`s2_data.release_log` for tracking) |

## S2 Data Tables

| Table | Description | Approximate Size |
|-------|-------------|-----------------|
| `s2_data.papers` | Paper metadata: corpusid, title, year, citationcount, authors, externalids, venue | 233M rows |
| `s2_data.citations` | Citation edges: citingcorpusid, citedcorpusid, isinfluential | 5.6B rows |
| `s2_data.authors` | Author profiles: authorid, name, affiliations, papercount, citationcount, hindex | 102M rows |
| `s2_data.author_paper_bridge` | Derived: authorid → corpusid mapping | Derived |
| `s2_data.author_paper_stats` | Derived: total_publications, i10_index, year_of_first_pub per author | Derived |
| `s2_data.paper_citations_by_year` | Derived: per-paper citation counts by citing year | Derived |

## Schedule

- **Monthly:** Cloud Scheduler `s2-monthly-ingestion` triggers on the 1st of each month at 02:00 UTC
- **Fallback:** GitHub Actions workflow `bigquery-materialize-all.yml` runs at 08:00 UTC on the 1st (safety net if the Cloud Run Job's materialization fails)

## Implementation

| File | Role |
|---|---|
| `dataset_ingestion/main.py` | Cloud Run Job entry point (full/diff/auto modes) |
| `dataset_ingestion/s2_api_client.py` | S2 Datasets API client (releases, file URLs, diffs) |
| `dataset_ingestion/downloader.py` | Parallel S3→GCS streaming (4-8 workers) |
| `dataset_ingestion/loader.py` | BigQuery bulk load + derived table building |
| `dataset_ingestion/diff_updater.py` | Incremental diff application (DELETE+MERGE) |
| `dataset_ingestion/materialize_tables.py` | Selective analytics DAG materialization (6 levels, 15 tables) |
| `dataset_ingestion/config.py` | Config with env var overrides |

## Infrastructure

- **Cloud Run Job:** `s2-dataset-ingestion`, us-central1
- **Cloud Scheduler:** `s2-monthly-ingestion` (monthly, 1st of month, 02:00 UTC)
- **GCS Bucket:** `scholar_data_share` (prefix: `s2_datasets/`)
- **S2 API Key:** Stored in Secret Manager (`projects/875626982900/secrets/s2-api-key`)

## Legacy

The `ingestion/` directory contains the original GCS → BigQuery batch load pipeline for Google Scholar data (`scholar_raw_data` dataset). It is still actively deployed: `deploy-ingestion.yml` deploys the `v3_batch_load_gcs_to_bq` Cloud Function and an hourly Cloud Scheduler job (`batch-load-gcs-to-bq`). This handles any remaining Google Scholar JSON files landing in GCS.
