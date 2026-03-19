# System Architecture: Scholar Analytics v2

## Overview

Scholar Analytics is a system for analyzing Google Scholar data using percentile-based, age-aware research metrics (PiP-AUC). It consists of five components with strict boundaries around what each reads and writes.

```
                    Google Scholar
                         |
                    [1. CRAWLER]
                         |
                    GCS (raw JSON)
                         |
                 [2. INGESTION PIPELINE]
                         |
                    BigQuery (raw tables)
                         |
                   [3. ANALYTICS]
                         |
                    BigQuery (views + materialized tables)
                        / \
                       /   \
              [4. FRONTEND]  [5. REFRESH & EXPAND]
              (read-only)     (orchestration)
                                    |
                              Cloud Tasks → back to Crawler
```

---

## Component 1: Crawler

**Purpose:** Fetch author and publication data from Google Scholar and write raw JSON to GCS.

**Input:** A Google Scholar author ID (via Cloud Tasks queue).

**Output:** Raw JSON files in GCS.

### What it does

1. Receives an author ID from the `process-authors` Cloud Tasks queue
2. Calls `scholarly.search_author_id()` + `scholarly.fill()` to get the full author profile
3. Serializes the author profile to JSON and uploads to GCS: `authors_json/YYYY/MM/DD/{scholar_id}.json`
4. For each publication in the author's profile, enqueues a publication fetch task to the `process-pubs` queue (with 0.1s stagger delay)
5. Each publication task calls `scholarly.fill()` on the publication and uploads to GCS: `publications_json/YYYY/MM/DD/{author_pub_id}.json`

### Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | Google Scholar (via `scholarly`) | |
| **Writes** | | GCS (`authors_json/`, `publications_json/`) |
| **Receives work from** | Cloud Tasks (`process-authors`, `process-pubs`) | |

### Current implementation

| File | Role |
|---|---|
| `functions/fetch_author/main.py` | Cloud Function: fetch author profile → GCS |
| `functions/fetch_publication/main.py` | Cloud Function: fetch publication details → GCS |
| `shared/services/storage_service.py` | GCS upload client |
| `shared/services/task_queue_service.py` | Enqueue publication tasks |
| `shared/config.py` | Region rotation, queue config |

### Infrastructure

- **Cloud Functions (Gen2):** Deployed across 9 US regions with daily rotation to distribute Scholar API load and avoid rate limiting
- **Cloud Tasks queues:** `process-authors` (author fetches), `process-pubs` (publication fetches), both in `northamerica-northeast1`
- **Timeouts:** 1 hour for author fetch, 60 seconds for publication fetch
- **Idempotency:** Task names include the scholar/pub ID; Cloud Tasks deduplicates (AlreadyExists caught gracefully)

### Queue management needs

- Query queue depth (number of pending tasks)
- Check if a specific author/publication fetch is pending or complete
- Purge/cancel tasks if needed
- Monitor for stuck or failed tasks

### Archival

After ingestion, raw JSON files are archived:
- `authors_json/` → `authors_archive/`
- `publications_json/` → `publications_archive/`

Archives are kept indefinitely in GCS for debugging and historical analysis.

---

## Component 2: Ingestion Pipeline

**Purpose:** Read raw JSON from GCS and load it into BigQuery tables.

**Input:** JSON files in GCS (`authors_json/`, `publications_json/`).

**Output:** Rows appended to BigQuery tables (`scholar_raw_data.author`, `scholar_raw_data.pub`).

### What it does

1. Lists JSON files in GCS by date-prefix folders
2. Validates and wraps each JSON file under a `{"data": ...}` key
3. Creates NDJSON (newline-delimited JSON) in a temp GCS location
4. Loads NDJSON into BigQuery via `WRITE_APPEND`
5. On success: moves source files to archive prefixes
6. On failure: moves bad files to `dead_letter/` prefix

### Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | GCS (`authors_json/`, `publications_json/`) | |
| **Writes** | | BigQuery (`scholar_raw_data.author`, `scholar_raw_data.pub`) |
| | | GCS (archive moves, temp NDJSON, dead letter) |

### Cadence

- **Default: daily batch.** This is a slow-moving field; daily is sufficient for routine operation.
- **On-demand: event-driven.** When a user searches for an author not yet in the database, the frontend can trigger an immediate ingestion cycle for that specific author after the crawler completes. This avoids the cost of continuous event-driven loading while providing responsive UX for interactive users.

### Current implementation

| File | Role |
|---|---|
| `functions/batch_load_gcs_to_bq/main.py` | Cloud Function: GCS → NDJSON → BigQuery batch load |
| `shared/services/storage_service.py` | GCS list/read/move operations |

### Infrastructure

- **Cloud Function (Gen2):** `batch_load_gcs_to_bq`, 1-hour timeout, 512MB memory
- **Cloud Scheduler:** Triggers the function (currently hourly; can move to daily)
- **BigQuery schema:** Raw data stored as `{document_id, timestamp, data}` where `data` is a JSON string parsed by downstream views

### Migration note: Eliminate Firestore Export path

**History:** The system originally used Firestore as the primary data store with Google's Firestore-to-BigQuery streaming extension. This was very expensive (continuous per-record streaming costs).

**Current state:** The BigQuery analytics views still reference `firestore_export.scholar_raw_author_raw_latest` and `firestore_export.scholar_raw_pub_raw_latest` — remnants of the old Firestore Export path.

**Target state:** All analytics views must be rewritten to read from the GCS-batch-loaded tables (`scholar_raw_data.author`, `scholar_raw_data.pub`) instead of the Firestore Export tables. The Firestore Export extension should be disabled. This is the single most important migration step.

---

## Component 3: Analytics

**Purpose:** Compute all metrics, percentiles, and scores from raw BigQuery data.

**Input:** BigQuery raw tables (`scholar_raw_data.author`, `scholar_raw_data.pub`).

**Output:** BigQuery views and materialized tables with computed metrics.

### What it computes

1. **Publication metrics:** Citation counts and citation percentiles per publication, partitioned by publication year
2. **Author metrics:** h-index, citations, i10-index (and 5-year variants), total publications — with percentiles by career-stage cohort (year of first publication)
3. **PiP-AUC scores:** Paper-in-Percentile Area Under Curve via trapezoidal integration, with percentile ranking
4. **Temporal metrics:** Historical evolution of author h-index, citations, i10-index over time
5. **Coauthor network:** Coauthor graph extracted from author profiles

### View dependency tiers

```
Tier 0 (distribution tables, materialized daily):
  dist_publication_citations          — (pub_year, num_citations) → percentile
  dist_author_metrics                 — (cohort, metric, value) → percentile
  dist_pip_auc_scores                 — (cohort, pip_auc_score) → percentile

Tier 1 (no view dependencies):
  base_author_publications            — author → publication list
  stats_publication_current           — publication citation percentiles (uses dist_publication_citations)
  stats_publication_citations_temporal — citation timeline per publication
  coauthor_network                    — coauthor graph

Tier 2 (depends on Tier 1):
  stats_author_current                — author summary metrics + percentiles
  coauthors_to_add                    — coauthors not yet in database
  intermediate_author_publication_state_temporal — author-pub state per year

Tier 3 (depends on Tier 2):
  stats_author_publication_pip_inputs_current — PiP chart X/Y coordinates
  stats_author_metrics_temporal_view          — temporal h-index, citations, i10

Tier 4 (depends on Tier 3):
  stats_author_pip_scores_current     — PiP-AUC score + percentile
```

### Materialization strategy

Expensive views are materialized daily into `_table` suffixed tables via the `bigquery-materialize.yml` workflow (06:00 UTC):

| Materialized table | Source |
|---|---|
| `dist_publication_citations` | PERCENT_RANK by pub_year |
| `dist_author_metrics` | PERCENT_RANK by cohort for 8 metrics |
| `dist_pip_auc_scores` | PiP-AUC percentiles |
| `stats_author_current_table` | `stats_author_current` view |
| `stats_author_pip_scores_current_table` | `stats_author_pip_scores_current` view |
| `stats_author_metrics_temporal` | `stats_author_metrics_temporal_view` |

**Per-author queries** use the live views (cheap with distribution table lookups). **Bulk queries** (all-authors ranking export) use the materialized tables.

### Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery raw tables (`scholar_raw_data.author`, `scholar_raw_data.pub`) | |
| **Writes** | | BigQuery views and materialized tables |

### Current implementation

| File | Role |
|---|---|
| `bigquery/statistics/*.sql` | All view and table definitions |
| `bigquery/coauthor_network/*.sql` | Coauthor graph views |
| `.github/workflows/bigquery-views.yml` | CI/CD: deploy views in dependency order |
| `.github/workflows/bigquery-materialize.yml` | CI/CD: daily materialization |

---

## Component 4: Frontend

**Purpose:** Search for authors and display precomputed analytics with visualizations. **Read-only** — does not modify any data or trigger any crawling.

**Input:** User queries (author search, author ID).

**Output:** HTML pages with embedded charts.

### What it does

1. **Author search:** User enters an author name → search Google Scholar via `scholarly` → display matching profiles
2. **Author profile:** User selects an author → query BigQuery views for metrics, percentiles, PiP-AUC → render charts
3. **Publication detail:** User clicks a publication → show citation timeline
4. **Download:** Export author publications as CSV
5. **All-authors ranking:** Export full ranking table as CSV (from materialized tables)

### Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery (analytics views and materialized tables) | |
| | Firestore (query result cache) | |
| | Google Scholar (author name search via `scholarly`) | |
| **Writes** | | Firestore (query result cache only) |
| | | GCS (CSV export: `all_authors_stats.csv`) |

**The frontend does NOT:**
- Write to BigQuery
- Enqueue crawl tasks
- Modify raw data
- Trigger refresh or expansion (that is Component 5's job)

### Caching

The frontend caches BigQuery query results in Firestore to avoid repeated expensive queries:
- **Cache key:** collection + document ID (e.g., `author_pub_stats/{author_id}`)
- **Invalidation:** Cache entry is stale when the author's latest data (max of author timestamp and latest publication timestamp) is newer than the cache timestamp
- **Collections used:** `author_pub_stats`, `author_stats`, `pub_stats`, `queries`

Firestore is used **only** as a cache in the target architecture — not as a raw data store.

### Visualization

All charts are generated server-side with matplotlib and embedded as base64 PNG:
- Percentile rank plot (paper rank vs citation percentile)
- PiP-AUC scatter plot (num_papers_percentile vs num_citations_percentile)
- Publication citation timeline (yearly + cumulative percentile, dual axis)
- Temporal author metrics (h-index, citations, i10 over time)

### Current implementation

| File | Role |
|---|---|
| `app/main.py` | Flask routes: `/`, `/results`, `/download`, `/publication/*` |
| `app/data_analysis.py` | BigQuery queries + Firestore cache layer |
| `app/visualization.py` | Matplotlib chart generation |
| `app/scholar.py` | Google Scholar author search (with Firestore cache) |
| `app/templates/` | Jinja2 HTML templates |
| `app/static/` | CSS, JS assets |

### Infrastructure

- **Cloud Run:** `scholar-service`, us-central1, port 8080
- **Docker:** Python 3.12-slim

---

## Component 5: Refresh & Expand

**Purpose:** Orchestrate data freshness and database growth by instructing the Crawler what to fetch. This is the only component that enqueues crawl tasks.

**Input:** Schedules, user requests, staleness analysis, coauthor graph.

**Output:** Tasks in Cloud Tasks queues (which feed the Crawler).

### What it does

1. **Stale author refresh:** Identify authors not updated in N days → enqueue for re-crawl
2. **User-triggered refresh:** When a user views an author and requests a refresh → enqueue for re-crawl
3. **New author fetch:** When a user searches for an author not in the database → enqueue for initial crawl
4. **Coauthor expansion:** Analyze the coauthor graph → identify high-value authors not yet in the database → enqueue for crawl
5. **On-demand ingestion trigger:** After enqueuing a crawl for a user-requested author, optionally trigger an immediate ingestion cycle so results appear faster than the daily batch

### Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery (`coauthors_to_add` view) | |
| | Firestore (author staleness timestamps) | |
| **Writes** | | Cloud Tasks (`process-authors` queue) |

### Refresh policies

| Trigger | Condition | Action |
|---|---|---|
| Scheduled | Author not updated in 90+ days | Enqueue for re-crawl |
| Scheduled | Coauthor not in database, high coauthor frequency | Enqueue for initial crawl |
| User-driven | User clicks "refresh" on author profile | Enqueue for re-crawl |
| User-driven | User searches for unknown author ID | Enqueue for initial crawl + trigger ingestion |

### Current implementation

| File | Role |
|---|---|
| `app/refresh.py` | Stale author detection, coauthor discovery, fetch enqueueing |
| `app/coauthor_service.py` | BigQuery query for coauthors not in DB |
| `app/queue_handler.py` | Cloud Tasks wrapper (enqueue, check pending) |
| `shared/services/task_queue_service.py` | Cloud Tasks client |
| `shared/repositories/author_repository.py` | Firestore staleness queries |

### Migration: Extract from Flask app

Currently, Refresh & Expand logic lives inside the Flask app as API routes (`/api/refresh_stale_authors`, `/api/add_coauthors`, `/api/fetch_authors`). These should be extracted into a **separate Cloud Run service** with:

- **Cloud Scheduler triggers** for periodic stale refresh and coauthor expansion
- **HTTP endpoints** for user-triggered refreshes (called by the frontend)
- Its own deployment pipeline, independent of the frontend

This separation means the frontend becomes truly read-only, and refresh/expand logic can be scaled, tested, and debugged independently.

---

## Data Stores Summary

| Store | Role | Written by | Read by |
|---|---|---|---|
| **GCS `authors_json/`** | Raw author JSON (staging) | Crawler | Ingestion |
| **GCS `publications_json/`** | Raw publication JSON (staging) | Crawler | Ingestion |
| **GCS `authors_archive/`** | Archived author JSON | Ingestion | (debugging only) |
| **GCS `publications_archive/`** | Archived publication JSON | Ingestion | (debugging only) |
| **GCS `dead_letter/`** | Failed files | Ingestion | (debugging only) |
| **GCS `all_authors_stats.csv`** | Author rankings export | Frontend | Frontend (signed URL download) |
| **BigQuery `scholar_raw_data.author`** | Raw author records | Ingestion | Analytics |
| **BigQuery `scholar_raw_data.pub`** | Raw publication records | Ingestion | Analytics |
| **BigQuery views** | Computed metrics/percentiles | Analytics | Frontend, Refresh & Expand |
| **BigQuery materialized tables** | Daily snapshots of views | Analytics | Frontend (bulk exports) |
| **Firestore (cache collections)** | Query result cache | Frontend | Frontend |
| **Cloud Tasks `process-authors`** | Author fetch queue | Refresh & Expand | Crawler |
| **Cloud Tasks `process-pubs`** | Publication fetch queue | Crawler | Crawler |

---

## Code to Delete or Consolidate

These are identified redundancies and dead code based on the target architecture:

### Duplicate author search

Both `app/scholar.py` and `functions/find_scholar_id_from_name/main.py` implement Google Scholar author search with Firestore caching. **Keep one.** The Cloud Function is the better home since it runs in rotating regions (Scholar rate-limit avoidance). The Flask app should call the Cloud Function instead of using `scholarly` directly.

### Firestore as raw data store

`shared/repositories/author_repository.py` and `shared/repositories/publication_repository.py` provide CRUD for raw scholar data in Firestore. In the target architecture, raw data lives only in GCS and BigQuery. These repositories should be:
- **Kept** if still needed for cache-related queries (e.g., staleness timestamps for Refresh & Expand)
- **Deleted** if staleness tracking moves to BigQuery (which already has timestamps on raw records)

### Firestore Export references in BigQuery views

All SQL views referencing `firestore_export.scholar_raw_author_raw_latest` or `firestore_export.scholar_raw_pub_raw_latest` must be rewritten to use `scholar_raw_data.author` and `scholar_raw_data.pub`.

### Refresh & Expand routes in Flask app

`app/main.py` routes `/api/add_coauthors`, `/api/refresh_stale_authors`, `/api/fetch_authors` should move to the separate Refresh & Expand service. The Flask app should only have a thin endpoint that forwards user-triggered refresh requests.

---

## Migration Plan

### Phase 1: Rewrite BigQuery views to use GCS-loaded tables

**Goal:** Eliminate dependency on Firestore Export.

1. Rewrite all views that reference `firestore_export.*` to read from `scholar_raw_data.author` and `scholar_raw_data.pub`
2. Verify analytics results match (compare output of old vs new views)
3. Disable the Firestore-to-BigQuery export extension
4. Remove Firestore raw data writes from crawler functions (if any remain)

### Phase 2: Consolidate author search

1. Make the Flask app call the `find_scholar_id_from_name` Cloud Function instead of using `scholarly` directly
2. Delete `app/scholar.py` (or reduce it to a thin client for the Cloud Function)

### Phase 3: Extract Refresh & Expand service

1. Create a new Cloud Run service (or Cloud Function) for Refresh & Expand
2. Move `app/refresh.py`, `app/coauthor_service.py`, `app/queue_handler.py` logic into it
3. Set up Cloud Scheduler triggers for periodic stale refresh and coauthor expansion
4. Add an HTTP endpoint for user-triggered refreshes
5. Update the Flask frontend to call the new service for refresh requests
6. Remove `/api/` routes from Flask `main.py`

### Phase 4: Clean up Firestore usage

1. Determine if `shared/repositories/` is still needed (staleness tracking)
2. If staleness can be tracked via BigQuery timestamps, delete the repositories
3. Firestore remains as cache-only for frontend query results

### Phase 5: Reorganize code structure

Target directory layout reflecting the 5 components:

```
scholar_v2/
├── crawler/                       # Component 1
│   ├── fetch_author/              # Cloud Function
│   ├── fetch_publication/         # Cloud Function
│   └── shared/                    # Copied at deploy time
├── ingestion/                     # Component 2
│   ├── batch_load/                # Cloud Function
│   └── shared/
├── analytics/                     # Component 3
│   ├── views/                     # SQL view definitions
│   │   ├── statistics/
│   │   └── coauthor_network/
│   └── materialization/           # Materialization SQL + workflow
├── frontend/                      # Component 4
│   ├── app/                       # Flask application
│   │   ├── main.py
│   │   ├── data_analysis.py
│   │   ├── visualization.py
│   │   ├── templates/
│   │   └── static/
│   ├── shared/
│   └── Dockerfile
├── refresh/                       # Component 5
│   ├── service/                   # Refresh & Expand logic
│   │   ├── stale_refresh.py
│   │   ├── coauthor_expansion.py
│   │   └── queue_manager.py
│   └── shared/
├── shared/                        # Common code (copied into each component at deploy)
│   ├── config.py
│   ├── services/
│   │   ├── bigquery_service.py
│   │   ├── firestore_service.py
│   │   ├── storage_service.py
│   │   └── task_queue_service.py
│   └── utils.py
├── scripts/                       # One-off utilities
└── .github/workflows/             # CI/CD
    ├── deploy-crawler.yml
    ├── deploy-ingestion.yml
    ├── deploy-analytics.yml       # View deployment + materialization
    ├── deploy-frontend.yml
    └── deploy-refresh.yml
```

---

## Open Questions

1. **Staleness tracking:** Currently uses Firestore document timestamps. Should this move to BigQuery (query `MAX(timestamp)` from raw tables), or is Firestore cheaper for these small lookups?

2. **`find_scholar_id_from_name`:** Is this part of the Crawler or the Frontend? It searches Scholar but doesn't fetch full profiles. Recommendation: keep as a standalone Cloud Function called by the Frontend.

3. **On-demand ingestion trigger:** When a user searches for a new author, how quickly should results appear? Options:
   - Wait for daily batch (simplest, cheapest)
   - Frontend triggers an immediate ingestion run for just that author's files
   - Crawler writes a "summary" directly queryable by the frontend while full ingestion is pending

4. **Shared code deployment:** The `shared/` directory is currently copied into each Cloud Function at deploy time. Should we package it as a proper Python package, or keep the copy approach?

---

_Last updated: 2026-03-19_
