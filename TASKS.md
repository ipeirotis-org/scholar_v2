# TASKS: Scholar Analytics

> Maintain and improve the Scholar Analytics platform — percentile-based, age-aware research metrics with PiP-AUC scoring.
>
> Live at [scholar-analytics.org](https://www.scholar-analytics.org/)
>
> **System architecture:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

---

## Collaboration

- [ ] **Reply to Florian Ederer re Shapley value citation attribution**
  - Email thread started by John Horton intro (April 2025)
  - Florian (economist at BU) wants per-author citation adjustment for tenure committees
  - Proposed Shapley values for citations on scholar-analytics.org
  - Florian shared Nature paper: https://www.nature.com/articles/s41467-019-13130-4
  - Draft reply mentions PNAS paper on co-citation analysis + Google Scholar blocking crawlers
  - Next step: send draft reply, then schedule a call to discuss collaboration

- [ ] **Read and analyze papers on citation credit allocation**
  - [ ] Shen & Barabasi (2014) "Collective credit allocation in science" -- https://www.pnas.org/doi/10.1073/pnas.1401992111
    - Uses co-citation patterns to infer per-author contribution to a paper's impact
  - [ ] Hasan & King (2019) "Spillover effects of intellectual property protection in the inter-war period" -- https://www.nature.com/articles/s41467-019-13130-4
    - Shared by Florian Ederer; relevant to citation attribution methodology

---

## BigQuery Cache Layer (Architecture Change)

Decouple the frontend from BigQuery by introducing a dedicated **cache_layer** component. The frontend becomes a pure Firestore reader; all BigQuery queries and cache writes are owned by the cache layer, triggered via Cloud Tasks queues.

**Design principles:**
- One-way data flow: BigQuery → cache_layer → Firestore. Frontend only reads Firestore.
- Cache is fully disposable — can be rebuilt from BigQuery at any time.
- Two Cloud Tasks queues: **priority** (interactive, user-waiting) and **batch** (warming, rebuilds, scheduled).
- Visualization stays in the frontend — cache serves structured data only.

### Phase 1: Build the `cache_layer/` component

Create the new component that owns all BigQuery reads and Firestore writes.

- [ ] **Scaffold `cache_layer/` directory structure**
  - `main.py` — Cloud Run HTTP entry points (one handler per queue)
  - `cache_service.py` — Orchestration: dispatch by request type, coordinate queries
  - `bigquery_client.py` — All BigQuery read queries (copy from frontend, refactor)
  - `cache_writer.py` — Firestore write-only client (adapted from frontend `cache.py`)
  - `config.py` — Config with env var overrides
  - `Dockerfile` — Python 3.12-slim, same base as frontend
  - `requirements.txt`
  - `tests/`

- [ ] **Implement request type handlers in `cache_service.py`**
  - `populate_author_profile(scholar_id)` — queries author_stats, pub_stats, temporal_stats; writes all three to Firestore
  - `populate_publication_detail(author_pub_id)` — queries pub temporal citation stats; writes to Firestore
  - `populate_recent_authors()` — queries recently analyzed authors list; writes to Firestore
  - `invalidate_author(scholar_id)` — checks freshness, re-populates all caches for that author
  - `rebuild_all()` — iterates all known authors, enqueues `populate_author_profile` for each to batch queue

- [ ] **Implement `bigquery_client.py`**
  - Move these methods from `frontend/bigquery_client.py`:
    - `get_author_pub_stats(scholar_id)`
    - `get_author_stats(scholar_id)`
    - `get_publication_stats(author_pub_id)`
    - `get_author_temporal_stats(scholar_id)`
    - `get_author_last_updated(scholar_id)` / `get_author_freshness(scholar_id)`
    - `get_recently_analyzed_authors(limit)`
  - Keep `get_all_authors_stats()` in frontend for now (CSV export, reads materialized tables)

- [ ] **Implement `cache_writer.py`**
  - Write-only Firestore client
  - Same collection names as frontend (`v3_author_stats`, `v3_author_pub_stats`, `v3_author_temporal`, `v3_pub_stats`, `v3_author_freshness`, `v3_recent_authors`)
  - Each write includes timestamp for staleness tracking
  - Batch write support for `rebuild_all`

- [ ] **Implement `main.py` with HTTP handlers**
  - `POST /tasks/priority` — dispatches priority queue tasks (author_profile, publication_detail, invalidate_author)
  - `POST /tasks/batch` — dispatches batch queue tasks (recent_authors, warm_author, rebuild_all)
  - `POST /admin/rebuild` — trigger full cache rebuild (enqueues to batch queue)
  - Request format: `{ "type": "author_profile", "scholar_id": "XYZ" }` (or similar per type)
  - Validate OIDC token from Cloud Tasks

- [ ] **Write tests for cache_layer**
  - Unit tests for each cache_service handler (mock BQ + Firestore)
  - Unit tests for bigquery_client (mock BQ client)
  - Unit tests for cache_writer (mock Firestore client)
  - Integration test: priority task → BQ query → Firestore write

### Phase 2: Cloud Tasks queues + wiring

Set up the queue infrastructure and connect the cache layer to event sources.

- [ ] **Create Cloud Tasks queues**
  - `cache-priority` — high max dispatches/sec, short task timeout (~30s), high concurrency
  - `cache-batch` — rate-limited dispatches/sec, longer timeout (~5min), lower concurrency
  - Region: `northamerica-northeast1` (same as existing queues)

- [ ] **Wire ingestion to publish cache invalidation events**
  - After `batch_load_gcs_to_bq` successfully loads author data, enqueue `invalidate_author` to priority queue for each affected author
  - This ensures cache stays fresh without the frontend needing to check freshness

- [ ] **Wire refresh service to publish warm events**
  - After triggering a crawl for an author, enqueue `warm_author` to batch queue
  - Pre-populates cache before user returns to check results

- [ ] **Add scheduled task for `populate_recent_authors`**
  - Cloud Scheduler → batch queue, every 5 minutes
  - Replaces the frontend's 5-minute TTL cache for homepage

- [ ] **CI/CD: deploy cache_layer as Cloud Run service**
  - Add to `.github/workflows/main.yml` or create dedicated workflow
  - Deploy alongside frontend and refresh services

### Phase 3: Migrate frontend to cache-read-only

Make the frontend a pure cache consumer.

- [ ] **Add queue client to frontend**
  - Thin client to enqueue tasks to `cache-priority` queue
  - Used on cache miss to request cache population

- [ ] **Modify frontend routes to be cache-read-only**
  - `/results`: read Firestore → if miss, enqueue `author_profile` to priority queue → return loading page
  - `/publications/<id>`: read Firestore → if miss, enqueue → return loading page
  - `/publication/<id>/<pub_id>`: read Firestore → if miss, enqueue `publication_detail` → return loading page
  - `/` (homepage): read Firestore only (populated by scheduled batch task) → if miss, show empty/fallback
  - `/download/<id>`: keep direct BigQuery for now (on-demand CSV export, low frequency)

- [ ] **Remove BigQuery dependency from frontend**
  - Delete `frontend/bigquery_client.py` (except `get_all_authors_stats` for CSV export, or move CSV to cache_layer too)
  - Remove `google-cloud-bigquery` from frontend `requirements.txt` (if CSV moves)
  - Simplify `frontend/cache.py` to read-only (remove `set()` method)
  - Remove freshness-checking logic from routes (cache layer owns freshness)

- [ ] **Update frontend loading/polling UX**
  - Existing loading page pattern already handles "data not ready" state
  - Add client-side polling or short auto-refresh for cache miss → enqueued → populated flow
  - Target: user sees data within 2-5s of cache miss (priority queue latency)

- [ ] **Update frontend tests**
  - Remove all BigQuery mock tests from frontend
  - Add tests for enqueue-on-miss behavior
  - Add tests for cache-read-only routes

---

## Future Features

- [ ] **REST API for authors, publications, and stats** _(from #28)_
  - Expose data as JSON API endpoints (separate from the HTML frontend)
  - Enables third-party integrations and programmatic access
  - Could be a separate Cloud Run service or part of the frontend with `/api/` routes

- [ ] **Migrate frontend to API + client-side JS** _(from #6)_
  - Replace Jinja server-rendered templates with API calls + JavaScript
  - Aligns with client-side charting direction (Chart.js/Plotly)
  - Added benefit: exposes a usable API for external consumers

- [ ] **Field-specific benchmarks** _(from #12)_
  - Allow users to compare against their field (business, CS, biology, etc.)
  - Show number of people included per field, field-specific percentiles

- [ ] **Crossref integration for publication metadata** _(from #20)_
  - Query Crossref to find DOIs and enriched metadata for publications in our dataset
  - Colab notebook exists: https://colab.research.google.com/drive/14WhIOthRkVMWp0r3O86PYVHTQ0CuipeT
  - Consider bulk importing the 120M entries dataset (publications until 2020)

- [ ] **Author comparison feature**
  - Side-by-side PiP-AUC and percentile plots for multiple authors

- [ ] **Dynamic region rotation per-request** instead of fixed at import time
  - Currently `Config.FUNCTION_LOCATION` is set once when the module loads
  - Long-running Cloud Run instances may use the same region for days

---

_Last updated: 2026-03-20_
