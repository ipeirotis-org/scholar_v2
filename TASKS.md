# TASKS: Scholar Analytics v2

> Maintain and improve the Scholar Analytics platform — percentile-based, age-aware research metrics with PiP-AUC scoring.
>
> Live at [scholar.ipeirotis.org](https://scholar.ipeirotis.org/)
>
> **System architecture:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

---

## v3 Migration — Build New Codebase

We are rebuilding the system from scratch in `v3/` with clean component boundaries, tests first, and no legacy baggage. The old code at the repo root continues to serve production until each component is replaced. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full architecture and build plan.

### Step 1: Crawler (Component 1)

- [ ] **Build the crawler from scratch in `v3/crawler/`**
  - [ ] `scholarly_client.py` — wrapper around `scholarly` with timeout handling (`ThreadPoolExecutor`), retry logic, clean error classification (transient vs permanent)
  - [ ] `gcs_writer.py` — upload JSON to GCS with date-prefix path, retry on failure
  - [ ] `task_enqueuer.py` — enqueue publication fetch tasks to Cloud Tasks with stagger delay
  - [ ] `fetch_author.py` — Cloud Function entry point: receive author_id → fetch → serialize → upload → enqueue pubs
  - [ ] `fetch_publication.py` — Cloud Function entry point: receive pub_data → fetch → upload
  - [ ] `config.py` — crawler config with env var overrides (project, bucket, queues, regions)
  - [ ] Tests for all modules (mocked scholarly, mocked GCS, mocked Cloud Tasks)
  - [ ] `deploy-crawler.yml` — CI/CD for 9-region deployment
  - [ ] Validate: deploy, crawl a test author, verify JSON lands in GCS correctly

### Step 2: Ingestion (Component 2)

- [ ] **Build the ingestion pipeline in `v3/ingestion/`**
  - [ ] `batch_load.py` — GCS → NDJSON → BigQuery batch load with chunking, dead-letter handling
  - [ ] `dedup_views.sql` — `author_latest` and `pub_latest` views with `ROW_NUMBER()` deduplication
  - [ ] Config with env var overrides
  - [ ] Tests and CI/CD
  - [ ] Validate: load crawled JSON, query `_latest` views, verify data

### Step 3: Analytics (Component 3)

- [ ] **Rewrite all SQL views to read from `scholar_raw_data.*_latest`**
  - [ ] Remove all references to `firestore_export.*` (currently ~15 references across 10 SQL files)
  - [ ] Distribution tables (`dist_publication_citations`, `dist_author_metrics`, `dist_pip_auc_scores`)
  - [ ] Core views (tiers 1–4): publication stats, author stats, PiP-AUC, temporal metrics, coauthor network
  - [ ] Materialization workflow
  - [ ] View output validation tests (compare against known-good data for sample authors)
  - [ ] Backfill: re-crawl all authors to populate `scholar_raw_data` with fresh data

### Step 4: Frontend (Component 4)

- [ ] **Build the frontend in `v3/frontend/`**
  - [ ] Flask app: read-only, queries analytics views + materialized tables
  - [ ] Firestore cache for query results only (not raw data)
  - [ ] Calls Author Search Service (Component 6) — no `scholarly` dependency
  - [ ] Calls Refresh & Expand (Component 5) for user-triggered refreshes
  - [ ] Visualization (decide: keep matplotlib or move to client-side charting)
  - [ ] Show recently analyzed authors on the home page _(from #26)_
  - [ ] Input validation, security headers, CSRF protection from the start
  - [ ] Accessibility: keyboard support, ARIA, no deprecated HTML attributes
  - [ ] Overall page template: consistent header, footer, navigation across all pages _(from #5)_
  - [ ] Pin dependency versions in `requirements.txt`
  - [ ] Tests and Dockerfile

### Step 5: Refresh & Expand (Component 5)

- [ ] **Build as a separate service in `v3/refresh/`**
  - [ ] Stale author detection via BigQuery timestamps (`MAX(timestamp)` on raw tables)
  - [ ] Error author detection: find authors with highest fetch errors and re-crawl (with 24h cooldown to avoid loops) _(from #32)_
  - [ ] Coauthor expansion via BigQuery `coauthors_to_add` view (evaluate oversample_factor — currently 100x may be excessive)
  - [ ] Queue manager: enqueue to Cloud Tasks, check pending status
  - [ ] Three scheduled tasks _(from #19)_: refresh stale authors, fix error authors, add coauthors (~1 per 10 min = ~4K/month)
  - [ ] HTTP endpoint for user-triggered refresh (called by frontend)
  - [ ] Tests and CI/CD

### Step 6: Author Search Service (Component 6)

- [ ] **Build in `v3/author_search/`**
  - [ ] Local-first search: query BigQuery `stats_author_current` + `coauthor_network` by name
  - [ ] Google Scholar fallback via `scholarly` (only when local search insufficient)
  - [ ] Firestore cache for Scholar search results
  - [ ] Cloud Function with 9-region rotation (for Scholar rate-limit avoidance)
  - [ ] Frontend autocomplete integration
  - [ ] Tests and CI/CD

### Cutover

- [ ] **Switch production to v3**
  - [ ] Point production traffic to v3 services
  - [ ] Verify all components working end-to-end
  - [ ] Drop `firestore_export` BigQuery dataset
  - [ ] Delete old code at repo root (keep in git history)
  - [ ] Move `v3/` contents to repo root

---

## Immediate (do now, applies to whole repo)

- [ ] **Improve .gitignore coverage**
  - Currently only 2 entries (`.ipynb_checkpoints`, `__pycache__`)
  - Missing: `.env`, `*.pyc`, `venv/`, `.pytest_cache/`, `downloads/`, `.DS_Store`, `*.egg-info/`

- [ ] **Pin dependency versions in `requirements.txt`** (old codebase)
  - All 13 packages have no `==` version constraints — builds not reproducible

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

## Future Features (for v3)

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

- [ ] **Document the region rotation strategy in README**
  - 9-region deployment with daily rotation is a key architectural decision
  - Documented in CLAUDE.md and `shared/config.py`, but not in README

---

## Utility Scripts

- [ ] **Fix broken `resolve_authors.py` script**
  - `scripts/resolve_authors.py:68`: after setting `sid` via coauthor graph, function falls through to "unresolved" return
  - Fix: add return statement after line 68

---

## Completed (old codebase)

<details>
<summary>Click to expand completed tasks</summary>

### Data Correctness
- [x] Fix hardcoded year 2025 in temporal citations view — losing 2026 data
- [x] Fix hardcoded year 2024 in results template
- [x] Fix first publication excluded from PiP-AUC score
- [x] Fix `base_author_publications` view deploy failure

### Security
- [x] Fix SQL injection pattern in coauthor query
- [x] Fix insecure default SECRET_KEY

### Infrastructure
- [x] Complete the GCS → BigQuery batch loading pipeline (chunking, scheduling, dead-letter)
- [x] Materialize percentile tables to reduce BigQuery costs (distribution lookups, daily materialization)
- [x] Add BigQuery view deployment to CI/CD

</details>

---

## Obsolete (superseded by v3 migration)

<details>
<summary>Click to expand — these bugs/tasks exist in the old codebase and will not be fixed there. The v3 rewrite addresses them by design.</summary>

- ~~Add input validation on URL parameters~~ → v3 frontend built correctly from scratch
- ~~Add security headers to Flask responses~~ → v3 frontend includes from start
- ~~Add retry logic for GCS upload failures~~ → v3 crawler has retry by design
- ~~Fix broken task queue status tracking~~ → v3 queue_manager redesigned
- ~~Add timeout handling for `scholarly.fill()`~~ → v3 `scholarly_client.py` handles this
- ~~Differentiate transient vs permanent failures~~ → v3 crawler error classification
- ~~Fix matplotlib memory leak~~ → v3 frontend may use client-side charting; if matplotlib kept, will close figures properly
- ~~Fix unsafe chained `.get().get()` in Firestore service~~ → v3 shared code rewritten
- ~~Fix `get_rotating_region()` docstring~~ → v3 config rewritten
- ~~Add environment variable overrides for hardcoded config~~ → v3 config has env vars from start
- ~~Add CI/CD tests~~ → v3 has tests-first approach per component
- ~~Improve error handling across services~~ → v3 services rewritten with proper error propagation
- ~~Reduce redundant Firestore reads~~ → v3 frontend cache redesigned
- ~~Consolidate duplicate service instances~~ → v3 clean architecture, no duplication
- ~~Evaluate coauthor query oversample_factor~~ → addressed in v3 refresh component
- ~~Consolidate logging configuration~~ → v3 has unified logging per component
- ~~Clean up empty stub templates~~ → v3 frontend rewritten
- ~~Re-enable temporal stats visualization~~ → v3 frontend includes temporal plots
- ~~Add OIDC authentication for Cloud Function endpoints~~ → v3 deployment config
- ~~Update frontend dependencies (jQuery, Bootstrap)~~ → v3 uses current versions
- ~~Switch from matplotlib to client-side charting~~ → decided during v3 frontend build
- ~~Improve accessibility~~ → v3 frontend built with accessibility from start

</details>

---

_Last updated: 2026-03-19_
