# CLAUDE.md — ipeirotis-org/scholar_v2

## Purpose

PiP Score is a distributed system for analyzing research impact using percentile-based, age-aware metrics. It implements the **PiP-AUC (Percentile-in-Percentile Area Under Curve)** scoring methodology using **Semantic Scholar** bulk datasets (200M papers, 2.4B citations, 102M authors) and serves an interactive Flask web app at [pip-score.org](https://www.pip-score.org/).

### What is PiP-AUC?

PiP-AUC combines citation quality with publication productivity into a single 0-to-1 score:

1. **Citation percentiles**: Each paper is ranked against all papers published in the same year. These percentiles stabilize within ~3 years of publication, enabling early impact assessment.
2. **Publication volume percentiles**: An author's paper count is compared against all S2 authors *at the same career stage* (years since first publication), with an `active_authors` benchmark (hindex≥3, pubs≥3) for meaningful differentiation.
3. **The PiP chart**: Papers are sorted by citation percentile (Y-axis, descending) and plotted against the author's publication count percentile (X-axis). Both axes range 0–1.
4. **AUC score**: The area under this curve is computed via trapezoidal integration. A PiP-AUC of 0.6 ~ top 25%; 0.8 ~ top 10%.

All author metrics (h-index, citations, i10-index) are also percentile-ranked by cohort (year of first publication).

See the [blog post](https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html) for the full methodology.

## Task Tracking

All current and planned tasks are tracked in [`TASKS.md`](TASKS.md). Check there for planned features and next steps.

## Architecture

```
scholar_v2/
├── frontend/                     # Flask web application (Cloud Run) — reads Firestore only
│   ├── main.py                   # App entry point
│   ├── app.py                    # Flask app factory with security headers
│   ├── routes.py                 # Routes: /, /results, /publication, /download, /data, /help, /api/*
│   ├── cache.py                  # Read-only Firestore cache client
│   ├── queue_client.py           # Enqueue cache-miss tasks to priority queue
│   ├── visualization.py          # Matplotlib plot generation (base64 PNG)
│   ├── config.py                 # Config with env var overrides
│   ├── templates/                # Jinja2 HTML templates
│   ├── static/                   # CSS, JS assets
│   ├── Dockerfile                # Python 3.12-slim, Flask on port 8080
│   └── tests/
├── cache_layer/                  # Cloud Run service: BigQuery → Firestore cache population
│   ├── main.py                   # HTTP entry points for priority + batch queues + admin
│   ├── cache_service.py          # Orchestration: dispatch by request type
│   ├── bigquery_client.py        # Read-only BigQuery queries (S2-backed views)
│   ├── cache_writer.py           # Write-only Firestore client
│   ├── config.py                 # Config with env var overrides
│   ├── Dockerfile                # Python 3.12-slim
│   └── tests/
├── dataset_ingestion/            # Cloud Run Job: monthly S2 dataset ingestion + materialization
│   ├── main.py                   # Job entry point (full/diff/auto modes)
│   ├── s2_api_client.py          # S2 Datasets API client (releases, file URLs, diffs)
│   ├── downloader.py             # Parallel S3→GCS streaming (4-8 workers)
│   ├── loader.py                 # BigQuery bulk load + derived table materialization
│   ├── materialize_tables.py     # Full analytics DAG materialization (7 levels)
│   ├── diff_updater.py           # Incremental diff application (DELETE+MERGE)
│   ├── config.py                 # Config with env var overrides
│   └── tests/
├── ingestion/                    # Cloud Function: GCS → BigQuery batch load (legacy)
│   ├── batch_load.py             # Streaming, chunking, dead-letter handling
│   ├── dedup_views.sql           # author_latest + pub_latest dedup views
│   ├── config.py                 # Config with env var overrides
│   └── tests/
├── author_search/                # Author search library (used by frontend, not a standalone service)
│   ├── search_service.py         # In-memory index search (loaded from BQ, refreshed every 6h)
│   ├── bigquery_client.py        # Loads active S2 authors for the in-memory index
│   ├── cache.py                  # Firestore cache (24h TTL for search results, chunked index)
│   ├── config.py                 # Config with env var overrides
│   └── tests/
├── bigquery/                     # SQL view definitions (8-level DAG)
│   ├── statistics/               # Stats, dist tables, ranked views (Levels 1–7)
│   └── coauthor_network/         # Co-author graph views
├── scripts/                      # One-off utility scripts
├── docs/                         # Architecture docs, analytics docs
└── .github/workflows/            # CI/CD (Cloud Run + Cloud Functions)
```

## Data Flow

```
1. INGEST: Monthly Cloud Scheduler (1st of month, 02:00 UTC)
             → dataset_ingestion Cloud Run Job
             → Download S2 dataset diffs from S2 S3 → GCS
             → BigQuery bulk load (papers, citations, authors)
             → Materialize derived tables (author_paper_bridge, author_paper_stats,
               paper_citations_by_year)
             → Materialize full analytics DAG (7 levels: dist tables, ranked tables,
               temporal tables — all views replaced with pre-computed tables)

2. ANALYZE: BigQuery SQL views compute:
             → Publication citation percentiles (by pub_year cohort)
             → Author metric percentiles (by year_of_first_pub cohort, two benchmarks:
               all_authors + active_authors)
             → PiP-AUC inputs (interpolated num_papers_percentile per pub)
             → PiP-AUC score (trapezoidal AUC) + its percentile
             → Temporal metrics (h-index, citations over time) [materialized daily]

3. CACHE:  Cache Layer (Cloud Run) populates Firestore from BigQuery
             → Triggered by: priority queue (frontend cache miss)
                             batch queue (scheduled warming, full rebuild)
             → Writes structured data to Firestore cache collections
             → Cache is fully disposable — can be rebuilt from BigQuery

4. SERVE:  Flask app reads from Firestore cache only (no direct BigQuery)
             → On cache miss: enqueues priority task → returns loading page
             → matplotlib generates percentile rank + PiP scatter plots from cached data
             → HTML templates render with base64-encoded PNG images

5. SEARCH: Author search runs in the frontend Cloud Run service (in-memory):
             → In-memory index of ~360K prominent S2 authors (refreshed every 6h from BQ)
             → Filtered to hindex >= 20, citedby > 5000
             → Instant substring matching, sorted by citation count
             → S2 API fallback for less-known researchers (102M authors)
             → Results cached in Firestore (24h TTL)
```

## BigQuery Analytics Framework

> Full details: [`docs/architecture-analytics-details.md`](docs/architecture-analytics-details.md)

### Data sources

All analytics views read from the `s2_data` dataset (Semantic Scholar bulk datasets):
- `s2_data.papers` (233M rows): corpusid, title, year, citationcount, authors, externalids, venue
- `s2_data.citations` (5.6B rows): citationid, citingcorpusid, citedcorpusid, isinfluential
- `s2_data.authors` (102M rows): authorid, name, affiliations, papercount, citationcount, hindex
- `s2_data.author_paper_bridge` (derived): authorid → corpusid mapping
- `s2_data.author_paper_stats` (derived): authorid, total_publications, i10_index, year_of_first_pub
- `s2_data.paper_citations_by_year` (derived): per-paper citation counts by citing year

### Key design pattern: Stats → Distributions → Ranked (per metric family)

Every metric family follows: **stats** (raw values) → **dist** (PERCENT_RANK) → **ranked** (cheap JOIN). The full DAG has **7 topological levels** (1–7). All levels are materialized into tables monthly after ingestion by `materialize_tables.py`. Views are kept for development/debugging but app queries hit materialized tables.

### Benchmark populations

Author-level distribution tables include two benchmarks:
- `all_authors` — full S2 population (~99.5M)
- `active_authors` — hindex≥3 AND total_publications≥3 (meaningful differentiation)

Ranked views default to `active_authors` for user-facing percentiles.

### View DAG (topological levels)

| Level | Views/Tables | Purpose |
|-------|-------------|---------|
| 1 | `base_author_publications`, `stats_publication_current`, `stats_publication_citations_temporal`, `coauthor_network`, **`dist_publication_citations`** ᵀ, **`dist_author_metrics`** ᵀ | Foundation: raw data only |
| 2 | `stats_author_current`, `intermediate_author_publication_state_temporal`, `coauthors_to_add`, `ranked_publication_current`, **`dist_publication_citations_temporal`** ᵀ | Derived stats + first ranked + first temporal dist |
| 3 | `stats_author_metrics_temporal_view`, `stats_author_publication_pip_inputs_current`, `ranked_author_current`, `ranked_publication_citations_temporal` | Temporal stats + PiP inputs + more ranked |
| 4 | `stats_author_pip_scores_current`, **`dist_pip_auc_scores`** ᵀ, **`dist_author_metrics_temporal`** ᵀ | PiP scores + PiP dist + temporal author dist |
| 5 | `ranked_author_pip_scores_current`, `ranked_author_metrics_temporal`, `stats_author_pip_scores_temporal_view` | PiP ranked + temporal ranked + temporal PiP stats |
| 6 | **`dist_pip_auc_scores_temporal`** ᵀ | Temporal PiP distribution |
| 7 | `ranked_author_pip_scores_temporal` | Temporal PiP ranked |

ᵀ = Materialized TABLE (quarterly). All others are live VIEWs.

### Materialization schedule

| What | Schedule | Rationale |
|------|----------|-----------|
| S2 dataset ingestion + full DAG materialization | **Monthly** (1st of month, 02:00 UTC) | S2 releases weekly diffs; monthly is sufficient for citation percentiles |
| All tables (dist + stats + ranked + temporal) | **Monthly** (during ingestion) | Materialized by `materialize_tables.py` in topological DAG order (7 levels) |
| Views (all non-materialized) | **Live** (kept for dev/debugging) | App queries use materialized `_table` versions via `USE_MATERIALIZED_TABLES` flag |

## Tech Stack

- **Flask** on Google Cloud Run
- **Semantic Scholar** bulk datasets (200M papers, 2.4B citations, 102M authors)
- **GCP**: Cloud Run (frontend + cache layer + dataset ingestion), Cloud Functions (ingestion), Firestore, BigQuery, Cloud Storage, Cloud Tasks
- **matplotlib** for visualization (server-side PNG)
- **pandas / numpy** for data manipulation
- **Docker** (Python 3.12-slim)

## Key Infrastructure

- **GCP Project**: `scholar-version2`
- **BigQuery Datasets**: `s2_data` (S2 raw tables + derived), `statistics` (analytics views), `scholar_raw_data` (legacy)
- **GCS Bucket**: `scholar_data_share` (prefixes: `s2_datasets/`, `bq_load_temp/`)
- **Cloud Tasks Queues**: `cache-priority`, `cache-batch` (location: `northamerica-northeast1`)
- **Cloud Scheduler**: `s2-monthly-ingestion` (monthly 1st of month, 02:00 UTC), `v3-populate-recent-authors` (every 5 min)

## CI/CD

- **deploy-frontend.yml**: Push to `main` → Docker build → deploy Cloud Run frontend (`us-central1`)
- **deploy-dataset-ingestion.yml**: Monthly S2 dataset ingestion + full DAG materialization (Cloud Run Job)
- **deploy-ingestion.yml**: Deploy GCS→BQ ingestion function
- **deploy-infrastructure.yml**: Cloud Tasks queues + Cloud Scheduler jobs
- **bigquery-views.yml**: Deploy analytics SQL views in topological DAG order (views kept for dev/debugging)
- **bigquery-materialize-all.yml**: Fallback monthly materialization (safety net if ingestion job's materialization fails)

## Development Notes

- **Config** in each component's `config.py` — supports env var overrides for all settings
- **Firestore caching** in the Flask app uses timestamp comparison: cache is invalidated when the author's latest data is newer than the cache entry
- **Task idempotency**: Cloud Tasks deduplicates naturally (AlreadyExists is caught gracefully)
- **BigQuery schema**: S2 data stored in native tables; legacy `scholar_raw_data` stored as `{document_id, timestamp, data}` where `data` is a JSON string
- **Author IDs**: S2 numeric author IDs (e.g., `2942126`). Legacy Google Scholar IDs (alphanumeric) no longer accepted
- **S2 API key**: Stored in Secret Manager (`projects/875626982900/secrets/s2-api-key`), used by dataset_ingestion for bulk downloads

## GCP

- **Project ID**: `scholar-version2`
- **Service Account**: `claude-agent@scholar-version2.iam.gserviceaccount.com`

### Roles Granted

| Role | Reason |
|------|--------|
| `roles/bigquery.dataEditor` | Read/write/update BigQuery tables and views |
| `roles/bigquery.jobUser` | Execute BigQuery queries and load jobs |
| `roles/cloudfunctions.developer` | Deploy/manage Cloud Functions |
| `roles/storage.objectAdmin` | Full read/write/delete on GCS objects |
| `roles/datastore.user` | Read/write Firestore documents (cache + repositories) |
| `roles/cloudtasks.admin` | Full task queue management for debugging |
| `roles/run.developer` | Deploy/manage Cloud Run services and jobs |
| `roles/iam.serviceAccountUser` | Act as service accounts for Cloud Run/Functions deploys |
| `roles/secretmanager.admin` | Create, update, and read secrets |
| `roles/logging.viewer` | Read Cloud Logging for debugging |
| `roles/monitoring.viewer` | Read Cloud Monitoring metrics |
| `roles/errorreporting.viewer` | View error reports |

### How to Authenticate

```bash
# 1. Decrypt the service account key
openssl enc -d -aes-256-cbc -pbkdf2 \
  -pass env:GCP_CREDENTIALS_KEY \
  -in .gcp-sa-key.enc -out /tmp/sa-key.json

# 2. Activate the service account
gcloud auth activate-service-account --key-file=/tmp/sa-key.json

# 3. Set the project
gcloud config set project $(jq -r .project_id .gcp-config.json)

# 4. Delete the plaintext key immediately
rm /tmp/sa-key.json
```

### Permission Escalation

If you hit a 403 error:
1. Stop and report the exact error, the role needed, and why
2. Ask the user for a new bootstrap token (`gcloud auth print-access-token`)
3. Use the token to update IAM bindings (never modify IAM policies without approval)
