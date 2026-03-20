# System Architecture: Scholar Analytics

## Overview

Scholar Analytics is a system for analyzing Google Scholar data using percentile-based, age-aware research metrics (PiP-AUC). It consists of seven components with strict boundaries around what each reads and writes.

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
     [7. CACHE LAYER]    |   [5. REFRESH & EXPAND]
      (BQ → Firestore)   |     (orchestration)
            |             |           |
      Firestore cache     |     Cloud Tasks → back to Crawler
            |             |
     [4. FRONTEND]        |
     (Firestore-only)     |
            |             |
            +--- calls ---+
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

### Implementation

| File | Role |
|---|---|
| `crawler/fetch_author.py` | Cloud Function: fetch author profile → GCS |
| `crawler/fetch_publication.py` | Cloud Function: fetch publication details → GCS |
| `crawler/gcs_writer.py` | GCS upload client with retry |
| `crawler/scholarly_client.py` | scholarly wrapper with timeout, retry, error classification |
| `crawler/task_enqueuer.py` | Enqueue publication tasks |
| `crawler/config.py` | Region rotation, queue config |

### Infrastructure

- **Cloud Functions (Gen2):** Deployed across 9 US regions with daily rotation to distribute Scholar API load and avoid rate limiting
- **Cloud Tasks queues:** `process-authors` (author fetches), `process-pubs` (publication fetches), both in `northamerica-northeast1`
- **Timeouts:** 1 hour for author fetch, 60 seconds for publication fetch
- **Idempotency:** Task names include the scholar/pub ID; Cloud Tasks deduplicates (AlreadyExists caught gracefully)

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

### Deduplication

The raw tables use `WRITE_APPEND`, so they accumulate every historical version of each document. `_latest` views deduplicate to the most recent record per document using `ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY timestamp DESC)`. All downstream analytics read from these `_latest` views.

### Cadence

- **Default: daily batch.** This is a slow-moving field; daily is sufficient for routine operation.
- **On-demand:** When a user searches for an author not yet in the database, the frontend can trigger an immediate ingestion cycle after the crawler completes.

### Implementation

| File | Role |
|---|---|
| `ingestion/batch_load.py` | Cloud Function: GCS → NDJSON → BigQuery batch load |
| `ingestion/config.py` | Config with env var overrides |

### Infrastructure

- **Cloud Function (Gen2):** `batch_load_gcs_to_bq`, 1-hour timeout, 512MB memory
- **Cloud Scheduler:** Triggers the function daily
- **BigQuery schema:** Raw data stored as `{document_id, timestamp, data}` where `data` is a JSON string parsed by downstream views

---

## Component 3: Analytics

**Purpose:** Compute all metrics, percentiles, and scores from raw BigQuery data.

**Input:** BigQuery raw tables via `_latest` deduplication views.

**Output:** BigQuery views and materialized tables with computed metrics.

### What it computes

1. **Publication metrics:** Citation counts and citation percentiles per publication, partitioned by publication year
2. **Author metrics:** h-index, citations, i10-index (and 5-year variants), total publications — with percentiles by career-stage cohort (year of first publication)
3. **PiP-AUC scores:** Paper-in-Percentile Area Under Curve via trapezoidal integration, with percentile ranking
4. **Temporal metrics:** Historical evolution of author h-index, citations, i10-index over time
5. **Coauthor network:** Coauthor graph extracted from author profiles

### Three-tier architecture

```
Tier 1 — Raw Statistics (no percentiles, no PERCENT_RANK):
  base_author_publications            — author → publication list
  stats_publication_current           — num_citations, metadata
  stats_publication_citations_temporal — yearly/cumulative citations
  stats_author_current                — hindex, citedby, i10index, total_publications
  stats_author_metrics_temporal_view  — per-author per-year metrics
  stats_author_pip_scores_current     — pip_auc_score (no percentile)
  stats_author_pip_scores_temporal_view — temporal pip_auc_score (no percentile)
  coauthor_network, coauthors_to_add
  intermediate_author_publication_state_temporal
  stats_author_publication_pip_inputs_current — PiP chart X/Y coordinates

Tier 2 — Distribution Tables (materialized quarterly, ONLY place PERCENT_RANK runs):
  dist_publication_citations          — (pub_year, num_citations) → percentile
  dist_publication_citations_temporal — (pub_year, citation_year, age) → 4 percentiles
  dist_author_metrics                 — (cohort, metric, value) → percentile (8 metrics)
  dist_author_metrics_temporal        — (cohort, state_year, metric, value) → percentile (7 metrics)
  dist_pip_auc_scores                 — (cohort, pip_auc_score) → percentile
  dist_pip_auc_scores_temporal        — (cohort, state_year, pip_auc_score) → percentile

Tier 3 — Ranked Views (cheap JOINs of Tier 1 + Tier 2):
  ranked_publication_current            — adds num_citations_percentile
  ranked_publication_citations_temporal  — adds 4 percentile columns
  ranked_author_current                 — adds 8 percentile columns
  ranked_author_metrics_temporal        — adds 7 percentile columns
  ranked_author_pip_scores_current      — adds pip_auc_score_percentile
  ranked_author_pip_scores_temporal     — adds pip_auc_score_percentile
```

### Materialization strategy

> Full details: [ANALYTICS.md](ANALYTICS.md)

There are two materialization schedules with different cost profiles:

**Quarterly — distribution tables** (`bigquery-materialize-distributions.yml`, 04:00 UTC, Jan/Apr/Jul/Oct):

| Table | What it computes |
|---|---|
| `dist_publication_citations` | PERCENT_RANK by pub_year |
| `dist_publication_citations_temporal` | PERCENT_RANK for 4 temporal citation metrics |
| `dist_author_metrics` | PERCENT_RANK by cohort for 8 metrics |
| `dist_author_metrics_temporal` | PERCENT_RANK by cohort+year for 7 metrics |
| `dist_pip_auc_scores` | PiP-AUC percentiles (depends on dist 1+3) |
| `dist_pip_auc_scores_temporal` | Temporal PiP-AUC percentiles (depends on dist 2+4) |

These are the **only** place `PERCENT_RANK()` runs. Output is small (DISTINCT values only) and the population shape changes slowly — recomputing quarterly introduces negligible error.

**Daily — snapshot tables** (`bigquery-materialize.yml`, 06:00 UTC):

| Table | Source |
|---|---|
| `ranked_author_current_table` | `ranked_author_current` view |
| `ranked_author_pip_scores_current_table` | `ranked_author_pip_scores_current` view |
| `ranked_author_metrics_temporal_table` | `ranked_author_metrics_temporal` view |
| `ranked_author_pip_scores_temporal_table` | `ranked_author_pip_scores_temporal` view |

These exist only for the all-authors ranking page and CSV export. **Per-author profile pages query the ranked views directly** (cheap via distribution table lookups, cached in Firestore).

**Cost note:** Individual author data changes at most monthly (90-day re-crawl threshold). Daily snapshot materialization recomputes ~15,000 rows when typically fewer than 200 have changed. This is an area where event-driven or weekly materialization could reduce cost without meaningful staleness impact.

### Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery raw tables (`scholar_raw_data.author`, `scholar_raw_data.pub`) via `_latest` views | |
| **Writes** | | BigQuery views and materialized tables |

### Implementation

| File | Role |
|---|---|
| `bigquery/statistics/*.sql` | All view and table definitions |
| `bigquery/coauthor_network/*.sql` | Coauthor graph views |
| `.github/workflows/bigquery-views.yml` | CI/CD: deploy views in dependency order |
| `.github/workflows/bigquery-materialize.yml` | CI/CD: daily materialization |

---

## Component 4: Frontend

**Purpose:** Display precomputed analytics with visualizations. **Read-only** — reads only from Firestore cache (populated by the Cache Layer). Does not query BigQuery directly.

**Input:** User queries (author search via Author Search Service, author ID for profile display).

**Output:** HTML pages with embedded charts.

### What it does

1. **Home page:** Recently analyzed authors (from Firestore cache) and search bar
2. **Author search:** User enters an author name → calls Author Search Service (Component 6) → display matching profiles
3. **Author profile:** User selects an author → read metrics from Firestore → render charts with visualization module
4. **Publication detail:** User clicks a publication → read citation timeline data from Firestore → render chart
5. **Download:** Export author publications as CSV
6. **Refresh request:** User clicks "refresh" on a stale author → forwards request to Refresh & Expand service (Component 5)

### Cache miss handling

When the frontend reads from Firestore and the data is not present (cache miss):
1. Enqueue a `populate` task to the Cache Layer's **priority queue** (Cloud Tasks)
2. Return a "loading" page to the user (this pattern already exists for uncrawled authors)
3. The user's page auto-refreshes; when the Cache Layer has populated the data, the next request returns the full page

### Page structure

All pages share a consistent template with:
- **Header:** Permanent links to Home, Help, and other key pages
- **Footer:** Attribution, contact, methodology link
- **Navigation:** Consistent across all pages via a base template

### Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | Firestore (cache — sole data source for display) | |
| | Author Search Service (Component 6) | |
| **Writes** | | Cloud Tasks (`cache-priority` queue, on cache miss) |
| **Calls** | | Refresh & Expand (Component 5) for user-triggered refreshes |

**The frontend does NOT:**
- Query BigQuery directly (all data comes from Firestore, populated by the Cache Layer)
- Write to Firestore (the Cache Layer owns all cache writes)
- Write to BigQuery
- Enqueue crawl tasks directly
- Modify raw data
- Call Google Scholar directly (that goes through Component 6)
- Run scheduled refresh or expansion (that is Component 5's job)

### Visualization

All charts are generated server-side with matplotlib and embedded as base64 PNG:
- Percentile rank plot (paper rank vs citation percentile)
- PiP-AUC scatter plot (num_papers_percentile vs num_citations_percentile)
- Publication citation timeline (yearly + cumulative percentile, dual axis)
- Temporal author metrics (h-index, citations, i10 over time)

The Cache Layer provides the structured data; the frontend owns how to visualize it.

### Implementation

| File | Role |
|---|---|
| `frontend/main.py` | App entry point |
| `frontend/app.py` | Flask app factory with security headers |
| `frontend/routes.py` | Routes: `/`, `/results`, `/publication`, `/download`, `/data`, `/help`, `/api/*` |
| `frontend/cache.py` | Read-only Firestore cache client |
| `frontend/queue_client.py` | Thin client to enqueue cache-miss tasks to priority queue |
| `frontend/visualization.py` | Matplotlib chart generation (base64 PNG) |
| `frontend/config.py` | Config with env var overrides |
| `frontend/templates/` | Jinja2 HTML templates |
| `frontend/static/` | CSS, JS assets |

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
2. **Error author re-crawl:** Find authors with highest fetch errors and re-crawl them, with a **24-hour cooldown** to avoid retry loops
3. **User-triggered refresh:** When a user views an author and requests a refresh → enqueue for re-crawl
4. **New author fetch:** When a user searches for an author not in the database → enqueue for initial crawl
5. **Coauthor expansion:** Analyze the coauthor graph → identify high-value authors not yet in the database → enqueue for crawl (~1 per 10 min = ~4K new authors/month)
6. **On-demand ingestion trigger:** After enqueuing a crawl for a user-requested author, optionally trigger an immediate ingestion cycle so results appear faster than the daily batch

### Scheduled tasks

| Task | Schedule | Description |
|---|---|---|
| **Refresh stale** | Periodic | Find oldest entries by timestamp → enqueue for re-crawl |
| **Fix errors** | Periodic | Find authors with highest error counts → re-crawl (skip if processed within 24h) |
| **Add coauthors** | ~1 per 10 min | Pick from `coauthors_to_add` view → enqueue for initial crawl |

### Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery (`coauthors_to_add` view, raw table timestamps for staleness, error counts) | |
| **Writes** | | Cloud Tasks (`process-authors` queue) |

### Refresh policies

| Trigger | Condition | Action |
|---|---|---|
| Scheduled | Author not updated in 90+ days | Enqueue for re-crawl |
| Scheduled | Author has high error count + last attempt > 24h ago | Enqueue for re-crawl |
| Scheduled | Coauthor not in database, high coauthor frequency | Enqueue for initial crawl |
| User-driven | User clicks "refresh" on author profile | Enqueue for re-crawl |
| User-driven | User searches for unknown author ID | Enqueue for initial crawl + trigger ingestion |

### Implementation

Refresh & Expand runs as a **separate Cloud Run service** with:
- **Cloud Scheduler triggers** for periodic stale refresh and coauthor expansion
- **HTTP endpoints** for user-triggered refreshes (called by the frontend)
- Its own deployment pipeline, independent of the frontend

This separation keeps the frontend truly read-only.

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

| Source | What it contains | Coverage |
|---|---|---|
| `stats_author_current` view | name, affiliation, email_domain, scholar_id, metrics | All crawled authors (~15k+) |
| `coauthor_network` view | coauthor_name, coauthor_affiliation, coauthor_scholar_id | All coauthors of crawled authors (much larger set) |
| `coauthors_to_add` view | name, affiliation, scholar_id of authors not yet crawled | Discovery candidates |

Most academic searches can be answered locally — a professor searching for their colleague will almost certainly find them in the coauthor graph without hitting Google Scholar.

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

### Frontend integration

The frontend calls this service for author search:
- Debounced input (300ms) → HTTP call to Author Search Service
- Display results as dropdown: name, affiliation, scholar_id
- Clicking a result navigates to `/results?author_id=X`
- If no results found: show "Search Google Scholar?" button that triggers the Scholar fallback explicitly

### Infrastructure

- **Cloud Function (Gen2):** Deployed across 9 US regions (same rotation as Crawler)
- Benefits from region rotation for Google Scholar fallback calls

---

## Component 7: Cache Layer

**Purpose:** Populate and maintain the Firestore cache from BigQuery. This is the **only** component that writes to Firestore cache collections. The cache is fully disposable — it can be wiped and rebuilt from BigQuery at any time.

**Input:** Tasks from two Cloud Tasks queues (priority and batch).

**Output:** Firestore cache documents.

### What it does

1. **Priority queue tasks** (interactive, user-waiting — target <2s):
   - `populate_author_profile` — query BigQuery for author_stats, pub_stats, temporal_stats; write all to Firestore
   - `populate_publication_detail` — query BigQuery for publication citation temporal data; write to Firestore
   - `invalidate_author` — re-populate all caches for an author whose data has changed

2. **Batch queue tasks** (background, scheduled):
   - `populate_recent_authors` — query BigQuery for recently analyzed authors; write to Firestore (scheduled every 5 min)
   - `warm_author` — pre-populate cache for a newly crawled author before a user visits
   - `rebuild_all` — full cache rebuild from BigQuery (enqueues individual `populate_author_profile` tasks to batch queue)

### Data flow

```
Cloud Tasks (priority or batch queue)
    → Cache Layer (Cloud Run)
        → BigQuery (read analytics views)
        → Firestore (write cache documents)
```

The Cache Layer is the **single writer** to Firestore cache collections. This one-way flow makes the cache fully disposable: delete all Firestore cache documents, trigger `rebuild_all`, and the cache is restored from BigQuery.

### Cache collections

| Collection | Document ID | Data | Populated by |
|---|---|---|---|
| `v3_author_stats/{id}` | scholar_id | Author metrics + percentiles + PiP-AUC | `populate_author_profile` |
| `v3_author_pub_stats/{id}` | scholar_id | Per-publication PiP inputs and metadata | `populate_author_profile` |
| `v3_author_temporal/{id}` | scholar_id | H-index, citations, i10 over time | `populate_author_profile` |
| `v3_pub_stats/{id}` | author_pub_id | Temporal citation data for one publication | `populate_publication_detail` |
| `v3_author_freshness/{id}` | scholar_id | Existence + last_updated timestamp | `populate_author_profile` |
| `v3_recent_authors` | `recent` | List of recently analyzed authors | `populate_recent_authors` |

### Cache invalidation

The Cache Layer owns invalidation logic:
- **On data change:** The Ingestion Pipeline (Component 2) enqueues `invalidate_author` to the priority queue after loading new data for an author. The Cache Layer checks the author's latest BigQuery timestamp and re-populates all caches.
- **On cache miss:** The Frontend (Component 4) enqueues a `populate` task to the priority queue. The Cache Layer runs the queries and writes fresh data to Firestore.
- **Scheduled:** `populate_recent_authors` runs every 5 minutes via Cloud Scheduler → batch queue.
- **Manual rebuild:** `rebuild_all` can reconstruct the entire cache from BigQuery.

No timestamp-comparison logic is needed in the frontend — it simply reads whatever is in Firestore.

### Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery (analytics views and materialized tables) | |
| **Writes** | | Firestore (cache collections) |
| **Receives work from** | Cloud Tasks (`cache-priority`, `cache-batch`) | |
| **Enqueues work to** | Cloud Tasks (`cache-batch`, for `rebuild_all` fan-out) | |

### Implementation

| File | Role |
|---|---|
| `cache_layer/main.py` | Cloud Run HTTP entry points (one handler per queue) |
| `cache_layer/cache_service.py` | Orchestration: dispatch by request type, coordinate queries |
| `cache_layer/bigquery_client.py` | Read-only BigQuery queries (moved from frontend) |
| `cache_layer/cache_writer.py` | Write-only Firestore client |
| `cache_layer/config.py` | Config with env var overrides |
| `cache_layer/Dockerfile` | Python 3.12-slim |

### Infrastructure

- **Cloud Run:** `cache-layer-service`, us-central1
- **Cloud Tasks queues:**
  - `cache-priority` — high concurrency, short timeout (~30s), for interactive cache population
  - `cache-batch` — rate-limited, longer timeout (~5min), for warming and rebuilds
- **Cloud Scheduler:** Triggers `populate_recent_authors` every 5 minutes via batch queue

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
| **BigQuery `scholar_raw_data.author`** | Raw author records | Ingestion | Analytics (via `_latest` views) |
| **BigQuery `scholar_raw_data.pub`** | Raw publication records | Ingestion | Analytics (via `_latest` views) |
| **BigQuery views** | Computed metrics/percentiles | Analytics | Cache Layer, Refresh & Expand |
| **BigQuery materialized tables** | Daily snapshots of views | Analytics | Cache Layer (bulk exports) |
| **Firestore (cache collections)** | Query result cache | Cache Layer | Frontend, Author Search |
| **Cloud Tasks `process-authors`** | Author fetch queue | Refresh & Expand | Crawler |
| **Cloud Tasks `process-pubs`** | Publication fetch queue | Crawler | Crawler |
| **Cloud Tasks `cache-priority`** | Interactive cache population | Frontend (on miss), Ingestion (on load) | Cache Layer |
| **Cloud Tasks `cache-batch`** | Background cache warming/rebuild | Cloud Scheduler, Refresh, Cache Layer | Cache Layer |

---

## Design Decisions

1. **Staleness tracking: BigQuery.** Use `SELECT MAX(timestamp) FROM scholar_raw_data.author WHERE document_id = @id`. This eliminates Firestore as a dependency for Refresh & Expand. BigQuery latency (~500ms) is acceptable since staleness checks run on schedules, not in user-facing request paths.

2. **Author search: Separate component (Component 6).** A dedicated service with local BigQuery search before Scholar fallback. Benefits from 9-region rotation for Scholar API calls. The frontend does not import `scholarly`.

3. **On-demand latency: Show "queued" status.** The crawler is not real-time. When a user searches for an unknown author, Refresh & Expand enqueues the crawl and the frontend shows "Author queued for analysis." No event-driven ingestion needed for most cases.

4. **Self-contained components.** Each component (`frontend/`, `crawler/`, `ingestion/`, `refresh/`, `author_search/`, `cache_layer/`) has its own `config.py`, service modules, `requirements.txt`, and tests. No shared code directory — components are fully independent.

5. **Cache Layer separation (Component 7).** The frontend does not query BigQuery directly. A dedicated Cache Layer service owns all BigQuery reads and Firestore writes. Benefits: (a) frontend latency is bounded by Firestore read time, not BigQuery query time; (b) BigQuery costs are controlled by the cache layer, not by user traffic; (c) BigQuery outages don't take down the frontend (it serves whatever is in cache); (d) the cache is fully disposable and can be rebuilt from BigQuery at any time. Two Cloud Tasks queues (priority + batch) separate interactive requests from background warming.

---

## Cost and Performance

### Cost profile

| Area | Cost | Notes |
|---|---|---|
| **GCS storage** | Negligible | Raw JSON + archives < 1GB |
| **Cloud Tasks** | Near-zero | Low volume |
| **BigQuery storage** | Minimal | Small raw + materialized tables |
| **Daily batch ingestion** | Free tier | One load job per day |
| **9-region function deployment** | Low | Minimal cost when idle, needed for rate-limit avoidance |
| **Firestore cache** | Low | 5-7 reads per page view |
| **Server-side matplotlib** | Moderate | ~200-500ms CPU per page load for 4-6 charts |

### Performance characteristics

| Operation | Latency | Mitigation |
|---|---|---|
| Frontend page load (cache hit) | ~50-100ms | Firestore read only — no BigQuery in request path |
| Frontend page load (cache miss) | 2-5s | Priority queue → Cache Layer → BigQuery → Firestore; user sees loading page then auto-refresh |
| BigQuery per-author query (Cache Layer) | 1-3s | Runs in Cache Layer, not frontend. Distribution table lookups keep view cost low. |
| matplotlib chart generation | 200-500ms | Could move to client-side JS charting in the future. |
| scholarly.fill() for author | 10-60s | Unavoidable — Scholar is slow. 1-hour timeout is appropriate. |
| scholarly.search_author() | 2-5s | Local BigQuery search first (Component 6) avoids this for most queries. |
| Bulk export (all authors) | 5-15s | Uses materialized tables. Pre-generated CSV served from GCS. |
| Full cache rebuild | Minutes | Batch queue fan-out; runs in background, no user impact. |

---

## Future Directions

| Feature | Description |
|---|---|
| **REST API** | Expose authors, publications, and stats as JSON API endpoints for third-party integrations. Cache Layer already provides the data; API would read from Firestore. |
| **Client-side charting** | Replace server-side matplotlib with Chart.js/Plotly for interactive plots. Data served via API from Firestore cache. |
| **Field-specific benchmarks** | Compare against specific fields (business, CS, biology) with per-field percentiles |
| **Crossref integration** | Enrich publications with DOIs and metadata from Crossref |
| **Author comparison** | Side-by-side PiP-AUC and percentile plots for multiple authors |

---

_Last updated: 2026-03-20_
