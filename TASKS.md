# TASKS: Scholar Analytics v2

> Maintain and improve the Scholar Analytics platform — percentile-based, age-aware research metrics with PiP-AUC scoring.
>
> Live at [scholar-analytics.org](https://www.scholar-analytics.org/)
>
> **System architecture:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

---

## v3 Migration — Build New Codebase

We are rebuilding the system from scratch in `v3/` with clean component boundaries, tests first, and no legacy baggage. The old code at the repo root continues to serve production until each component is replaced. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full architecture and build plan.

### Step 1: Crawler (Component 1)

- [ ] **Build the crawler from scratch in `v3/crawler/`**
  - [x] `scholarly_client.py` — wrapper around `scholarly` with timeout handling (`ThreadPoolExecutor`), retry logic, clean error classification (transient vs permanent)
  - [x] `gcs_writer.py` — upload JSON to GCS with date-prefix path, retry on failure
  - [x] `task_enqueuer.py` — enqueue publication fetch tasks to Cloud Tasks with stagger delay
  - [x] `fetch_author.py` — Cloud Function entry point: receive author_id → fetch → serialize → upload → enqueue pubs
  - [x] `fetch_publication.py` — Cloud Function entry point: receive pub_data → fetch → upload
  - [x] `config.py` — crawler config with env var overrides (project, bucket, queues, regions)
  - [x] Tests for all modules (53 tests, mocked scholarly, mocked GCS, mocked Cloud Tasks)
  - [x] `deploy-crawler.yml` — CI/CD for 9-region deployment (updated `function.yml`)
  - [x] Validate: deployed to us-central1 + us-east4, tested with real author IDs, verified error classification and logging

### Step 2: Ingestion (Component 2)

- [ ] **Build the ingestion pipeline in `v3/ingestion/`**
  - [x] `batch_load.py` — GCS → NDJSON → BigQuery batch load with streaming, chunking, dead-letter handling (OOM-safe: streams GCS listing, processes in bounded batches, clears NDJSON after each batch)
  - [x] `dedup_views.sql` — `author_latest` and `pub_latest` views with `ROW_NUMBER()` deduplication in `scholar_raw_data` dataset
  - [x] `config.py` — Config with env var overrides (project, bucket, dataset, batch size, max files)
  - [x] Tests (34 tests: config, streaming, NDJSON prep, dead letter, archival, BQ load, batching, entry point)
  - [x] CI/CD — Added to `function.yml`: deploys `v3_batch_load_gcs_to_bq` function + dedup views
  - [x] Validate: deployed dedup views, loaded 100 real publications from GCS, verified data integrity and downstream analytics compatibility (v3 views are a superset of legacy: 16,468 vs 16,080 authors, 3,438,072 vs 3,408,566 pubs)

### Step 3: Analytics (Component 3)

- [x] **Rewrite all SQL views to read from `scholar_raw_data.*_latest`**
  - [x] Remove all references to `firestore_export.*` (migrated 15 references across 11 files)
  - [x] Distribution tables (`dist_publication_citations`, `dist_author_metrics`, `dist_pip_auc_scores`)
  - [x] Core views (tiers 1–4): publication stats, author stats, PiP-AUC, temporal metrics, coauthor network
  - [x] Materialization workflow (`materialize_stats.sql`)
  - [x] Deploy all 24 views/tables to BigQuery (3 materialized tables were missing: `dist_publication_citations_temporal`, `dist_author_metrics_temporal`, `dist_pip_auc_scores_temporal`; plus 5 views)
  - [x] Fix floor-lookup performance: rewrite all 6 ranked views from range-join pattern to scalar subquery pattern (query time: timeout → 4s for per-author queries)
  - [x] View output validation: tested `stats_author_current`, `ranked_author_current`, `ranked_publication_current`, `stats_author_pip_scores_current`, `ranked_author_pip_scores_current`, `stats_author_publication_pip_inputs_current`, `ranked_author_metrics_temporal`, `ranked_author_pip_scores_temporal`, `coauthor_network` across 3 authors (Ipeirotis, Hinton, Bengio) — all passed
  - [ ] Backfill: re-crawl all authors to populate `scholar_raw_data` with fresh data

### Step 4: Frontend (Component 4)

- [ ] **Build the frontend in `v3/frontend/`**
  - [x] Flask app factory with security headers (X-Content-Type-Options, X-Frame-Options, X-XSS-Protection, Referrer-Policy, Permissions-Policy)
  - [x] BigQuery client: parameterized queries for author stats, publication stats, PiP inputs, temporal metrics, CSV export
  - [x] Firestore cache for query results only (not raw data), with timestamp-based invalidation
  - [x] Visualization: server-side matplotlib (percentile rank, PiP scatter, pub citations, temporal dual-axis plots), explicit figure cleanup to prevent memory leaks
  - [x] Route handlers: /, /results, /publication, /download, /data, /help, /api/fetch_authors, /api/refresh_stale_authors, /api/add_coauthors, /get_similar_authors
  - [x] Input validation: regex-based scholar_id and author_pub_id validation on all routes
  - [x] Accessibility: ARIA landmarks, roles, labels, keyboard-navigable autocomplete, semantic HTML
  - [x] Overall page template: consistent header, footer, navigation, flash messages across all pages _(from #5)_
  - [x] Pin dependency versions in `requirements.txt`
  - [x] Tests (30 tests: config, routes, input validation, security headers, visualizations) and Dockerfile
  - [x] Calls Author Search Service (Component 6) — runs in-process via direct import; also deployable as 9-region Cloud Function
  - [x] Calls Refresh & Expand (Component 5) — delegates via HTTP when REFRESH_SERVICE_URL configured, falls back gracefully
  - [ ] Show recently analyzed authors on the home page _(from #26)_

### Step 5: Refresh & Expand (Component 5)

- [ ] **Build as a separate service in `v3/refresh/`**
  - [x] `config.py` — Config with env var overrides (project, bucket, queues, regions, refresh policies)
  - [x] `bigquery_client.py` — Queries for stale authors (90-day threshold), error authors (24h cooldown), coauthors to add (oversample factor 10x)
  - [x] `task_enqueuer.py` — Enqueue author tasks to Cloud Tasks with idempotent task names, duplicate handling
  - [x] `refresh_service.py` — Orchestration: refresh_stale_authors, refresh_error_authors, expand_coauthors, fetch_author(s)
  - [x] `main.py` — HTTP entry points: refresh_stale, refresh_errors, expand_coauthors, fetch_author, fetch_authors (Cloud Run / Cloud Function)
  - [x] Frontend wired: routes.py calls Component 5 via HTTP when REFRESH_SERVICE_URL is configured, falls back to stubs when not
  - [x] Tests (76 tests: config, bigquery_client, task_enqueuer, refresh_service, main entry points, input validation)
  - [x] `requirements.txt` — functions-framework, google-cloud-bigquery, google-cloud-tasks
  - [x] CI/CD deployment pipeline — Cloud Run service in `main.yml` with source deploy
  - [x] Cloud Scheduler triggers — refresh stale (daily 02:00), refresh errors (daily 03:00), expand coauthors (every 10 min)

### Step 6: Author Search Service (Component 6)

- [ ] **Build in `v3/author_search/`**
  - [x] Local-first search: query BigQuery `stats_author_current` + `coauthor_network` by name
  - [x] Google Scholar fallback via `scholarly` (only when local search insufficient)
  - [x] Firestore cache for Scholar search results (24h TTL)
  - [x] `main.py` — Cloud Function entry point with `functions_framework.http`
  - [x] `requirements.txt` — scholarly, google-cloud-bigquery, google-cloud-firestore, functions-framework
  - [x] CI/CD — 9-region Cloud Function deployment in `function.yml`
  - [x] Frontend autocomplete integration — in-process import in `routes.py:get_similar_authors`
  - [x] Tests (20 tests: bigquery_client, search_service, cache)
  - [ ] End-to-end validation with live BigQuery data

### Cutover

- [ ] **Switch production to v3**
  - [ ] Point production traffic to v3 services
  - [ ] Verify all components working end-to-end
  - [ ] Drop `firestore_export` BigQuery dataset
  - [ ] Delete old code at repo root (keep in git history)
  - [ ] Move `v3/` contents to repo root

---

## Immediate (do now, applies to whole repo)

- [x] **Improve .gitignore coverage**
  - Added `.env`, `*.pyc`, `*.pyo`, `*.egg-info/`, `venv/`, `.venv/`, `.pytest_cache/`, `downloads/`, `.DS_Store`, `*.log`, `sa-key.json`

- [x] **Pin dependency versions in `requirements.txt`** (old codebase)
  - All 12 packages now pinned with `==` version constraints

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

- [x] **Fix broken `resolve_authors.py` script**
  - Added missing `return` statement after coauthor graph resolution (line 68)

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

_Last updated: 2026-03-20 (CI/CD deployment pipelines for all 6 components)_
