# CLAUDE.md — ipeirotis-org/scholar_v2

## Purpose

Scholar Analytics is a distributed system for analyzing Google Scholar data using percentile-based, age-aware research metrics. It implements the **PiP-AUC (Paper-in-Percentile Area Under Curve)** scoring methodology and serves an interactive Flask web app at [scholar-analytics.org](https://www.scholar-analytics.org/).

### What is PiP-AUC?

PiP-AUC combines citation quality with publication productivity into a single 0-to-1 score:

1. **Citation percentiles**: Each paper is ranked against all papers published in the same year. These percentiles stabilize within ~3 years of publication, enabling early impact assessment.
2. **Publication volume percentiles**: An author's paper count is compared to ~15,000 faculty at top US universities *at the same career stage* (years since first publication). This age-aware normalization prevents bias toward senior researchers.
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
│   └── tests/                    # 32 tests
├── cache_layer/                  # Cloud Run service: BigQuery → Firestore cache population
│   ├── main.py                   # HTTP entry points for priority + batch queues
│   ├── cache_service.py          # Orchestration: dispatch by request type
│   ├── bigquery_client.py        # Read-only BigQuery queries
│   ├── cache_writer.py           # Write-only Firestore client
│   ├── config.py                 # Config with env var overrides
│   ├── Dockerfile                # Python 3.12-slim
│   └── tests/
├── crawler/                      # Cloud Functions: fetch author/publication data (Gen2, 9 regions)
│   ├── fetch_author.py           # Scholar → JSON → GCS → enqueue pubs (timeout: 1h)
│   ├── fetch_publication.py      # Scholar → JSON → GCS (timeout: 60s)
│   ├── scholarly_client.py       # scholarly wrapper with timeout, retry, error classification
│   ├── gcs_writer.py             # GCS upload with retry
│   ├── task_enqueuer.py          # Cloud Tasks enqueue with stagger delay
│   ├── config.py                 # Config with env var overrides
│   └── tests/                    # 53 tests
├── ingestion/                    # Cloud Function: GCS → BigQuery batch load
│   ├── batch_load.py             # Streaming, chunking, dead-letter handling
│   ├── dedup_views.sql           # author_latest + pub_latest dedup views
│   ├── config.py                 # Config with env var overrides
│   └── tests/                    # 34 tests
├── refresh/                      # Cloud Run service: refresh & expand orchestration
│   ├── main.py                   # HTTP entry points for stale/error/coauthor refresh
│   ├── refresh_service.py        # Orchestration logic
│   ├── bigquery_client.py        # Queries for stale/error authors, coauthors
│   ├── task_enqueuer.py          # Enqueue author tasks to Cloud Tasks
│   ├── config.py                 # Config with env var overrides
│   └── tests/                    # 76 tests
├── author_search/                # Cloud Function: author search (9 regions)
│   ├── main.py                   # Cloud Function entry point
│   ├── search_service.py         # BigQuery-first search with Scholar fallback
│   ├── bigquery_client.py        # Author name search queries
│   ├── scholar_client.py         # scholarly fallback search
│   ├── cache.py                  # Firestore cache (24h TTL)
│   ├── config.py                 # Config with env var overrides
│   └── tests/                    # 23 tests
├── bigquery/                     # SQL view definitions (8-level DAG)
│   ├── statistics/               # Stats, dist tables, ranked views (Levels 1–7)
│   └── coauthor_network/         # Co-author graph views
├── scripts/                      # One-off utility scripts
├── docs/                         # Architecture docs, analytics docs
└── .github/workflows/            # CI/CD (Cloud Run + multi-region Functions)
```

## Data Flow

```
1. FETCH:  User enters Scholar ID
             → Cloud Tasks queue
             → fetch_author Cloud Function (scholarly.search_author_id + fill)
             → JSON saved to GCS (authors_json/YYYY/MM/DD/{scholar_id}.json)
             → enqueue fetch_publication for each pub (0.1s delay between)

2. LOAD:   batch_load_gcs_to_bq (triggered manually or scheduled)
             → list GCS files by date folder
             → wrap JSON under {"data": ...} key, create NDJSON
             → BigQuery WRITE_APPEND load
             → archive source files (authors_json/ → authors_archive/)

3. ANALYZE: BigQuery SQL views compute:
             → Publication citation percentiles (by pub_year cohort)
             → Author metric percentiles (by year_of_first_pub cohort)
             → PiP-AUC inputs (interpolated num_papers_percentile per pub)
             → PiP-AUC score (trapezoidal AUC) + its percentile
             → Temporal metrics (h-index, citations over time) [materialized, daily refresh]

4. CACHE:  Cache Layer (Cloud Run) populates Firestore from BigQuery
             → Triggered by: priority queue (frontend cache miss, ingestion events)
                             batch queue (scheduled warming, full rebuild)
             → Writes structured data to Firestore cache collections
             → Cache is fully disposable — can be rebuilt from BigQuery

5. SERVE:  Flask app reads from Firestore cache only (no direct BigQuery)
             → On cache miss: enqueues priority task → returns loading page
             → matplotlib generates percentile rank + PiP scatter plots from cached data
             → HTML templates render with base64-encoded PNG images
```

## BigQuery Analytics Framework

> Full details: [`docs/architecture-analytics-details.md`](docs/architecture-analytics-details.md)

### Key design pattern: Stats → Distributions → Ranked (per metric family)

Every metric family follows: **stats** (raw values) → **dist** (PERCENT_RANK, materialized quarterly) → **ranked** (cheap JOIN). But the PiP pipeline creates cross-role dependencies, so the full DAG has **8 topological levels** (0–7), not 3 tiers.

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
| Distribution tables (`dist_*`, 6 tables) | **Quarterly** | Population percentiles shift slowly; expensive to compute |
| Snapshot tables (`ranked_*_table`, 4 tables) | **Daily** | Needed only for all-authors ranking/export; per-author pages use views directly |
| Views (all non-dist, non-snapshot) | **Live** | Cheap per-author queries via dist table lookups; cached in Firestore |

**Cost note:** Individual author data changes at most monthly (90-day re-crawl threshold). Daily snapshot materialization recomputes ~15,000 rows when typically <200 have changed. Per-author profile pages are unaffected — they query views directly and cache in Firestore.

## Tech Stack

- **Flask** on Google Cloud Run
- **scholarly** for Google Scholar scraping
- **GCP**: Cloud Run (frontend + refresh), Cloud Functions (crawler + ingestion + search, 9 regions), Firestore, BigQuery, Cloud Storage, Cloud Tasks, Secret Manager
- **matplotlib** for visualization (server-side PNG)
- **pandas / numpy** for data manipulation
- **Docker** (Python 3.12-slim)

## Key Infrastructure

- **GCP Project**: `scholar-version2`
- **BigQuery Dataset**: `scholar_raw_data` (tables: `author`, `pub`; views in `statistics/`, `coauthor_network/`)
- **GCS Bucket**: `scholar_data_share` (prefixes: `authors_json/`, `publications_json/`, `authors_archive/`, `publications_archive/`, `bq_load_temp/`)
- **Cloud Tasks Queues**: `process-authors`, `process-pubs` (location: `northamerica-northeast1`)
- **Region Rotation**: Functions deploy across 9 US regions; requests rotate **daily** (`(hours_since_epoch // 24) % 9`) to distribute Scholar API load and avoid rate limiting

## CI/CD

- **main.yml**: Push to `main` → Cloud Build → Docker image to Artifact Registry → deploy Cloud Run (frontend + refresh service, `us-central1`) + Cloud Scheduler for refresh tasks
- **function.yml**: Deploy crawler (2 functions x 9 regions), ingestion (1 function + dedup views), author search (1 function x 9 regions), batch load scheduler
- **bigquery-views.yml**: Deploy analytics SQL views in topological DAG order
- **bigquery-materialize.yml**: Daily snapshot materialization (4 ranked tables)
- **bigquery-materialize-distributions.yml**: Quarterly distribution materialization (6 dist tables)
- Tests run per-component: 218 tests total across all 5 components

## Development Notes

- **Config** in each component's `config.py` — supports env var overrides for all settings
- **Region rotation** is set at module import time (`get_rotating_region()`), so it's fixed for the lifetime of a Cloud Run instance
- **Firestore caching** in the Flask app uses timestamp comparison: cache is invalidated when the author's latest data (max of author timestamp and latest publication timestamp) is newer than the cache entry
- **Task idempotency**: Task names include the scholar/pub ID, so Cloud Tasks deduplicates naturally (AlreadyExists is caught gracefully)
- **BigQuery schema**: Raw data stored as `{document_id, timestamp, data}` where `data` is a JSON string — views parse this with JSON functions
- **Dedup views**: `author_latest` and `pub_latest` views deduplicate raw data using `ROW_NUMBER()` partitioned by document_id

## GCP

- **Project ID**: `scholar-version2`
- **Service Account**: `claude-agent@scholar-version2.iam.gserviceaccount.com`

### Roles Granted

| Role | Reason |
|------|--------|
| `roles/bigquery.dataEditor` | Read/write/update BigQuery tables and views |
| `roles/bigquery.jobUser` | Execute BigQuery queries and load jobs |
| `roles/cloudfunctions.developer` | Deploy/manage Cloud Functions across 9 regions |
| `roles/storage.objectAdmin` | Full read/write/delete on GCS objects |
| `roles/datastore.user` | Read/write Firestore documents (cache + repositories) |
| `roles/cloudtasks.admin` | Full task queue management for debugging |
| `roles/run.developer` | Deploy/manage Cloud Run Flask app |
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
