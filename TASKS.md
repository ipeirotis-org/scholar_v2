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

### Phase 1: Build the `cache_layer/` component ✓

Create the new component that owns all BigQuery reads and Firestore writes.

- [x] **Scaffold `cache_layer/` directory structure**
- [x] **Implement request type handlers in `cache_service.py`**
- [x] **Implement `bigquery_client.py`** (moved from frontend + `get_all_author_ids`)
- [x] **Implement `cache_writer.py`** (write-only Firestore client with batch support)
- [x] **Implement `main.py` with HTTP handlers** (`/tasks/priority`, `/tasks/batch`, `/admin/rebuild`, `/admin/populate`, `/health`)
- [x] **Write tests for cache_layer** (50 tests)

### Phase 2: Cloud Tasks queues + wiring ✓ (code complete, infra pending)

- [x] **Wire ingestion to publish cache invalidation events**
  - `ingestion/cache_enqueuer.py` — extracts scholar IDs from NDJSON, enqueues `invalidate_author` to priority queue
  - `ingestion/batch_load.py` — calls cache enqueuer after successful BQ load (non-fatal on failure)
  - 8 new tests for cache_enqueuer
- [ ] **Create Cloud Tasks queues** (infra — needs `gcloud` commands or Terraform)
  - `cache-priority` — high concurrency, short timeout (~30s)
  - `cache-batch` — rate-limited, longer timeout (~5min)
  - Region: `northamerica-northeast1`
- [ ] **Wire refresh service to publish warm events**
  - After triggering a crawl for an author, enqueue `warm_author` to batch queue
- [ ] **Add scheduled task for `populate_recent_authors`**
  - Cloud Scheduler → batch queue, every 5 minutes
- [ ] **CI/CD: deploy cache_layer as Cloud Run service**
  - Add to `.github/workflows/main.yml` or create dedicated workflow

### Phase 3: Migrate frontend to cache-read-only ✓

- [x] **Add queue client to frontend** (`frontend/queue_client.py`)
- [x] **Modify frontend routes to be cache-read-only**
  - All routes read from Firestore only; enqueue on cache miss; return loading page
  - `/download` reads pub_stats from cache (no direct BQ)
- [x] **Remove BigQuery dependency from frontend**
  - Deleted `frontend/bigquery_client.py`
  - Replaced `google-cloud-bigquery` with `google-cloud-tasks` in requirements.txt
  - Simplified `frontend/cache.py` (removed `valid_after` parameter, documented cache-layer ownership)
  - `cache.set()` retained only for plot caching (frontend-generated)
- [x] **Update frontend tests** (27 tests, all passing — no BQ mocks needed)

### Remaining deployment tasks

- [ ] **Create Cloud Tasks queues** (`cache-priority`, `cache-batch`)
- [ ] **Deploy cache_layer Cloud Run service**
- [ ] **Set `CACHE_LAYER_URL` env var** on frontend, ingestion, and cache_layer services
- [ ] **Run initial cache rebuild** (`POST /admin/rebuild`) to populate Firestore from BigQuery
- [ ] **Add Cloud Scheduler** for `populate_recent_authors` (every 5 min)
- [ ] **Wire refresh service** to enqueue `warm_author` after crawls
- [ ] **Update frontend loading/polling UX**
  - Existing loading page pattern already handles "data not ready" state
  - Consider client-side polling or short auto-refresh for smoother cache-miss UX

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
