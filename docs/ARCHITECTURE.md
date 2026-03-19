# System Architecture: Scholar Analytics v2

## Overview

Scholar Analytics is a system for analyzing Google Scholar data using percentile-based, age-aware research metrics (PiP-AUC). It consists of six components with strict boundaries around what each reads and writes.

```
                         Google Scholar
                          |         |
                   [1. CRAWLER]  [6. AUTHOR SEARCH]
                          |         |
                   GCS (raw JSON)  BigQuery + Scholar (fallback)
                          |         |
                  [2. INGESTION]   |
                          |        |
                   BigQuery (raw)  |
                          |        |
                    [3. ANALYTICS]  |
                          |        |
                   BigQuery (views + materialized tables)
                         /|\
                        / | \
                       /  |  \
          [4. FRONTEND]   |   [5. REFRESH & EXPAND]
          (read-only)     |     (orchestration)
               |          |           |
               +--- calls ---+  Cloud Tasks → back to Crawler
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

**Purpose:** Display precomputed analytics with visualizations. **Read-only** — does not modify any data, trigger any crawling, or search Google Scholar directly.

**Input:** User queries (author search via Author Search Service, author ID for profile display).

**Output:** HTML pages with embedded charts.

### What it does

1. **Author search:** User enters an author name → calls Author Search Service (Component 6) → display matching profiles
2. **Author profile:** User selects an author → query BigQuery views for metrics, percentiles, PiP-AUC → render charts
3. **Publication detail:** User clicks a publication → show citation timeline
4. **Download:** Export author publications as CSV
5. **All-authors ranking:** Export full ranking table as CSV (from materialized tables)
6. **Refresh request:** User clicks "refresh" on a stale author → forwards request to Refresh & Expand service (Component 5)

### Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery (analytics views and materialized tables) | |
| | Firestore (query result cache) | |
| | Author Search Service (Component 6) | |
| **Writes** | | Firestore (query result cache only) |
| | | GCS (CSV export: `all_authors_stats.csv`) |
| **Calls** | | Refresh & Expand (Component 5) for user-triggered refreshes |

**The frontend does NOT:**
- Write to BigQuery
- Enqueue crawl tasks directly
- Modify raw data
- Call Google Scholar directly (that goes through Component 6)
- Run scheduled refresh or expansion (that is Component 5's job)

### Caching

The frontend caches BigQuery query results in Firestore to avoid repeated expensive queries:
- **Cache key:** collection + document ID (e.g., `author_pub_stats/{author_id}`)
- **Invalidation:** Cache entry is stale when the author's latest data (max of author timestamp and latest publication timestamp) is newer than the cache timestamp
- **Collections used:** `author_pub_stats`, `author_stats`, `pub_stats`

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
| `app/scholar.py` | Google Scholar author search — **TO DELETE** (replaced by Component 6) |
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
| **Reads** | BigQuery (`coauthors_to_add` view, raw table timestamps for staleness) | |
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

## Component 6: Author Search Service

**Purpose:** Help users find authors by name. Searches local data first (cheap, fast), falls back to Google Scholar only when needed.

**Input:** Author name query string.

**Output:** List of matching authors with name, affiliation, scholar_id, citation count.

### What it does

1. **Local search (fast, free):** Query BigQuery for authors already in the database whose name matches the query. This covers ~15,000+ faculty profiles plus all their coauthors (names and IDs available from the coauthor network).
2. **Google Scholar fallback (slow, rate-limited):** If local search returns too few results, call `scholarly.search_author()` to find authors not yet in the database.
3. **Cache results:** Cache Google Scholar search results to avoid repeated API calls for the same query.

### Data sources for local search

The system already has rich author identity data:

| Source | What it contains | Coverage |
|---|---|---|
| `stats_author_current` view | name, affiliation, email_domain, scholar_id, metrics | All crawled authors (~15k+) |
| `coauthor_network` view | coauthor_name, coauthor_affiliation, coauthor_scholar_id | All coauthors of crawled authors (much larger set) |
| `coauthors_to_add` view | name, affiliation, scholar_id of authors not yet crawled | Discovery candidates |

This means most academic searches can be answered locally. A professor searching for their colleague will almost certainly find them in the coauthor graph without hitting Google Scholar at all.

### Search strategy

```
1. Query BigQuery: SELECT name, affiliation, scholar_id
   FROM stats_author_current WHERE LOWER(name) LIKE @pattern
   → Return if sufficient results

2. Query BigQuery: SELECT coauthor_name, coauthor_affiliation, coauthor_scholar_id
   FROM coauthor_network WHERE LOWER(coauthor_name) LIKE @pattern
   → Return if sufficient results

3. Check Firestore cache for this query string
   → Return if cached and fresh

4. Fall back to scholarly.search_author(name)
   → Cache results in Firestore
   → Return
```

### Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery (author + coauthor views) | |
| | Firestore (search result cache) | |
| | Google Scholar (fallback via `scholarly`) | |
| **Writes** | | Firestore (search result cache) |

### Why a separate component

- **Rate limiting:** Google Scholar aggressively rate-limits. Running `scholarly` from a Cloud Function with 9-region rotation is safer than running it from the single-region Flask app.
- **Testability:** Search logic can be tested independently — mock BigQuery results, verify fallback behavior.
- **Scalability:** Can add more sophisticated matching (fuzzy name matching, affiliation search, autocomplete) without touching the frontend.
- **Separation of concerns:** The frontend should not import `scholarly` at all.

### Current implementation

| File | Role | Target state |
|---|---|---|
| `functions/find_scholar_id_from_name/main.py` | Cloud Function: Scholar search only | Extend with local BigQuery search |
| `app/scholar.py` | Flask app: duplicate Scholar search | **Delete** — replaced by this component |

### Frontend integration

The frontend autocomplete should call this service as the user types:
- Debounced input (300ms) → HTTP call to Author Search Service
- Display results as dropdown: name, affiliation, scholar_id
- Clicking a result navigates to `/results?author_id=X`
- If no results found: show "Search Google Scholar?" button that triggers the Scholar fallback explicitly

### Infrastructure

- **Cloud Function (Gen2):** Deployed across 9 US regions (same rotation as Crawler)
- **OR Cloud Run service:** If we need persistent connections or more complex logic
- Benefits from region rotation for Google Scholar fallback calls

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
| **Firestore (cache collections)** | Query result cache | Frontend, Author Search | Frontend, Author Search |
| **Cloud Tasks `process-authors`** | Author fetch queue | Refresh & Expand | Crawler |
| **Cloud Tasks `process-pubs`** | Publication fetch queue | Crawler | Crawler |

---

## Code to Delete or Consolidate

These are identified redundancies and dead code based on the target architecture:

### Duplicate author search

Both `app/scholar.py` and `functions/find_scholar_id_from_name/main.py` implement Google Scholar author search with Firestore caching. **Delete `app/scholar.py`.** The Cloud Function becomes the Author Search Service (Component 6), extended with local BigQuery search before Scholar fallback. The Flask app calls this service instead of using `scholarly` directly.

### Firestore as raw data store

`shared/repositories/author_repository.py` and `shared/repositories/publication_repository.py` provide CRUD for raw scholar data in Firestore. In the target architecture, raw data lives only in GCS and BigQuery. **Delete both repositories.** Staleness tracking moves to BigQuery (`SELECT MAX(timestamp) FROM scholar_raw_data.author WHERE document_id = @id`).

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

### Phase 2: Build Author Search Service (Component 6)

1. Extend `find_scholar_id_from_name` Cloud Function with local BigQuery search (query `stats_author_current` and `coauthor_network` by name before falling back to `scholarly`)
2. Make the Flask app call this service instead of using `scholarly` directly
3. Delete `app/scholar.py`
4. Update the frontend autocomplete to call the Author Search Service

### Phase 3: Extract Refresh & Expand service

1. Create a new Cloud Run service (or Cloud Function) for Refresh & Expand
2. Move `app/refresh.py`, `app/coauthor_service.py`, `app/queue_handler.py` logic into it
3. Set up Cloud Scheduler triggers for periodic stale refresh and coauthor expansion
4. Add an HTTP endpoint for user-triggered refreshes
5. Update the Flask frontend to call the new service for refresh requests
6. Remove `/api/` routes from Flask `main.py`

### Phase 4: Clean up Firestore usage

1. Delete `shared/repositories/author_repository.py` and `publication_repository.py`
2. Move staleness tracking to BigQuery: `SELECT MAX(timestamp) FROM scholar_raw_data.author WHERE document_id = @id`
3. Firestore remains as cache-only for frontend query results and author search results

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
├── author_search/                 # Component 6
│   ├── main.py                    # Cloud Function: local search + Scholar fallback
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
    ├── deploy-refresh.yml
    └── deploy-author-search.yml
```

---

## Design Decisions

1. **Staleness tracking: BigQuery.** Use `SELECT MAX(timestamp) FROM scholar_raw_data.author WHERE document_id = @id`. This eliminates Firestore as a dependency for Refresh & Expand and lets us delete `shared/repositories/`. BigQuery latency (~500ms) is acceptable since staleness checks run on schedules, not in user-facing request paths.

2. **Author search: Separate component (Component 6).** The existing `find_scholar_id_from_name` Cloud Function becomes the Author Search Service, extended with local BigQuery search before Scholar fallback. `app/scholar.py` is deleted. The service benefits from 9-region rotation for Scholar API calls.

3. **On-demand latency: Show "queued" status.** The crawler is not real-time. When a user searches for an unknown author, Refresh & Expand enqueues the crawl and the frontend shows a message like "Author queued for analysis, results available within 24 hours." No event-driven ingestion needed.

4. **Shared code: Keep copy approach.** The `shared/` directory is copied into each component at deploy time (already done in CI/CD). Add a `shared/VERSION` file so deployed components can report which version of shared code they're running.

---

## Cost and Performance Analysis

### Current cost hotspots

| Issue | Component | Impact | Fix |
|---|---|---|---|
| **Firestore Export streaming** | Ingestion | Continuous per-record cost to BigQuery | Phase 1: rewrite views to use batch-loaded tables, disable export |
| **Live BigQuery view queries** | Frontend | Full table scans on every author page view | **Already fixed:** distribution table lookups for per-author queries; materialized tables for bulk |
| **Server-side matplotlib** | Frontend | ~200-500ms CPU per page load to generate 4-6 PNG charts | Future: move to client-side charting (Chart.js/Plotly) |
| **9-region x 4 function deployment** | Crawler | 36 function deployments, most idle | Acceptable: minimal cost when idle, needed for rate-limit avoidance |
| **Firestore cache reads** | Frontend | 5-7 reads per page view (cache check + data) | Reduce: consolidate into fewer documents, accept stale data for longer |

### What's already cheap

- **GCS storage:** Raw JSON + archives are small (<1GB total). Negligible cost.
- **Cloud Tasks:** Per-task pricing is near-zero at our volume.
- **BigQuery storage:** Raw tables + materialized tables are small. Storage cost is minimal.
- **Daily batch ingestion:** One BigQuery load job per day is essentially free.

### Performance bottlenecks

| Bottleneck | Latency | Component | Mitigation |
|---|---|---|---|
| **BigQuery per-author query** | 1-3 seconds | Frontend | Firestore cache (cache hit: ~50ms). Distribution table lookups reduced view cost significantly. |
| **matplotlib chart generation** | 200-500ms | Frontend | Could move to client-side JS charting. Or pre-render and cache chart images. |
| **scholarly.fill() for author** | 10-60 seconds | Crawler | Unavoidable — Scholar is slow. 1-hour timeout is appropriate. |
| **scholarly.search_author()** | 2-5 seconds | Author Search | Local BigQuery search first (Component 6) avoids this for most queries. |
| **Bulk export (all authors)** | 5-15 seconds | Frontend | Uses materialized tables. Could pre-generate CSV daily and serve from GCS (already partially done). |

### Recommendations for cost reduction

1. **Priority 1: Kill Firestore Export** (Phase 1). This is the single largest unnecessary cost. Rewriting views to use batch-loaded tables eliminates continuous streaming charges.

2. **Priority 2: Move Cloud Scheduler from hourly to daily.** The batch load runs hourly but daily is sufficient. Reduces BigQuery load job costs (minimal) and function invocations.

3. **Priority 3: Cache more aggressively.** The Firestore cache invalidation currently checks if author data is newer than cache. For most authors (unchanged for months), extend cache TTL significantly — check for freshness at most once per day, not on every request.

4. **Future: Client-side charting.** Eliminates matplotlib CPU cost on Cloud Run. The data is already in JSON format from BigQuery; sending it to the browser for Chart.js/Plotly rendering would be faster and enable interactive plots.

---

_Last updated: 2026-03-19_
