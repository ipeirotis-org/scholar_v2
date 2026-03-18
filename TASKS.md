# TASKS: Scholar Analytics v2

> Maintain and improve the Scholar Analytics platform — percentile-based, age-aware research metrics with PiP-AUC scoring.
>
> Live at [scholar.ipeirotis.org](https://scholar.ipeirotis.org/)

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

## Architecture — Key Context

### Storage migration (Firestore → GCS + BigQuery batch)

**History:** The system originally used Firestore as the primary data store, with Google's automatic Firestore-to-BigQuery streaming extension syncing data for analytics. This was very expensive due to continuous per-record streaming costs.

**Current state (incomplete migration):** We stopped the Firestore→BigQuery streaming and switched to a GCS-based pipeline:
1. Cloud Functions (`fetch_author`, `fetch_publication`) write JSON to GCS buckets (`authors_json/`, `publications_json/`)
2. `batch_load_gcs_to_bq` Cloud Function reads GCS files, converts to NDJSON, and loads into BigQuery in batch mode
3. After successful load, source files are archived (`authors_json/` → `authors_archive/`)

**What's broken:** The batch load function (`functions/batch_load_gcs_to_bq/main.py`) is triggered manually (no scheduling), and it times out for authors with very long publication lists. There is no systematic, automated way to move data from GCS to BigQuery. The old Firestore saves in the Cloud Functions are commented out (lines 111-117 in `fetch_author`, lines 92-100 in `fetch_publication`).

**What we need:** A reliable, automated GCS→BigQuery batch pipeline that handles large authors without timeout, runs on a schedule (e.g., daily or hourly), and doesn't require manual intervention.

### Percentile computation cost

**History:** All percentile calculations (publication citations, author metrics, PiP-AUC scores) are computed as live BigQuery SQL views using `PERCENT_RANK()` window functions. Every time a user views an author profile, the app queries these views, triggering expensive full-table scans and window computations across the entire dataset.

**Current state:** The view dependency chain is:
```
stats_publication_current (pub citation percentiles — MODERATE-HIGH cost)
  → stats_author_current (author metric percentiles — HIGH cost)
    → stats_author_publication_pip_inputs_current (PiP interpolation — VERY HIGH cost, 6-CTE pipeline)
      → stats_author_pip_scores_current (PiP-AUC score — MODERATE cost)
```
Only `stats_author_metrics_temporal` is materialized (daily refresh). All other views recompute on every query.

**What we need:** Materialize the expensive percentile tables (publication percentiles, author percentiles, PiP scores) on a schedule (daily or monthly). The live app would then do simple lookups against pre-computed tables instead of running expensive window functions. For publications not yet in the materialized table (newly fetched), use an approximate lookup ("find the closest percentile number" from the same pub_year cohort).

---

## Critical — Data Correctness (actively wrong in production)

- [x] **Fix hardcoded year 2025 in temporal citations view — losing 2026 data NOW**
  - `bigquery/statistics/stats_publication_citations_temporal.sql:45`: replaced `2025` with `EXTRACT(YEAR FROM CURRENT_DATE())`

- [x] **Fix hardcoded year 2024 in results template**
  - `app/templates/results.html:21`: replaced hardcoded `2024` with `current_year` injected via Flask context processor

- [x] **Fix first publication excluded from PiP-AUC score**
  - `bigquery/statistics/stats_author_pip_scores_current.sql`: used `COALESCE` on `LAG()` results to include first publication in AUC

- [x] **Fix `base_author_publications` view deploy failure**
  - `bigquery/statistics/base_author_publications.sql:1`: changed to `CREATE OR REPLACE VIEW` with full project prefix

---

## Critical — Security

- [ ] **Fix SQL injection pattern in coauthor query**
  - `app/coauthor_service.py:50`: `LIMIT {rows_needed}` uses f-string interpolation instead of parameterized query
  - Fix: use `LIMIT @rows_needed` with `ScalarQueryParameter("rows_needed", "INT64", rows_needed)`

- [ ] **Fix insecure default SECRET_KEY**
  - `shared/config.py:21`: `SECRET_KEY = os.getenv("SECRET_KEY", "default-secret-key")` allows Flask session forgery
  - Fix: raise error if env var not set, or generate random key at startup with `os.urandom(32).hex()`

- [ ] **Add input validation on URL parameters**
  - `app/main.py:98, 106`: `int()` conversion without try/except — crashes on bad input
  - `app/main.py:113, 121`: no format validation on scholar IDs, no length limits
  - Fix: try/except around int() casts, validate scholar_id with regex `^[a-zA-Z0-9_-]+$`, cap `num_authors`

- [ ] **Add security headers to Flask responses**
  - No CSP, X-Frame-Options, X-Content-Type-Options, or HSTS headers
  - Fix: add `@app.after_request` handler in `app/main.py`

---

## Bugs / Reliability

- [ ] **Re-enable temporal stats visualization**
  - `app/main.py:157-173`: temporal stats block is commented out
  - Plot functions exist in `app/visualization.py` (`generate_author_h_index_plot`, `generate_author_total_citations_plot`, `generate_author_i10_index_plot`, `generate_author_h_index_5y_plot`)
  - `bigquery_service.get_author_temporal_stats()` is implemented and working
  - `stats_author_metrics_temporal` materialized view refreshes daily
  - Decision needed: re-enable as-is, or redesign the temporal UI

- [ ] **Add retry logic for GCS upload failures**
  - `functions/fetch_author/main.py:130-133`: GCS upload returns None on failure, no retry
  - `functions/fetch_publication/main.py:114-117`: same issue
  - Risk: silent data loss for fetched Scholar data

- [ ] **Fix broken task queue status tracking**
  - `shared/services/task_queue_service.py`: `get_number_of_tasks_in_queue()` always returns None; `check_pending_tasks()` always returns False
  - `app/queue_handler.py` wraps these but can't provide meaningful status
  - `app/main.py:132-134`: queue count display is commented out as a result
  - Source comments suggest Firestore-based tracking as alternative

- [ ] **Add timeout handling for `scholarly.fill()` in Cloud Functions**
  - `functions/fetch_author/main.py:159`: `scholarly.fill()` can hang for the entire 1h function timeout
  - `functions/fetch_publication/main.py`: same issue with 60s timeout
  - Fix: wrap in `concurrent.futures.ThreadPoolExecutor` with timeout (300s for authors, 30s for publications)

- [ ] **Differentiate transient vs permanent failures in Cloud Functions**
  - `functions/fetch_publication/main.py:56-59`: all exceptions return 500, causing Cloud Tasks to retry permanent failures
  - Fix: return 503 for transient errors (network/timeout), 200/400 for permanent errors (bad data)

- [ ] **Fix matplotlib memory leak in visualization.py**
  - `app/visualization.py`: all plot functions create `Figure` and `BytesIO` objects without closing them
  - Affects: lines 12, 49, 93, 154, 184, 227
  - Fix: add `plt.close(fig)` and `buf.close()` after base64 encoding

- [ ] **Fix unsafe chained `.get().get()` in Firestore service**
  - `shared/services/firestore_service.py:88-90`: `doc.to_dict().get("data").get(key_attr)` — AttributeError if "data" is None
  - Fix: use `(doc.to_dict().get("data") or {}).get(key_attr)`

- [ ] **Fix broken `resolve_authors.py` script**
  - `scripts/resolve_authors.py:68`: after setting `sid` via coauthor graph, function falls through to "unresolved" return
  - Fix: add return statement after line 68

- [ ] **Fix `get_rotating_region()` docstring**
  - `shared/config.py:40`: docstring says "based on the current UTC hour" but line 59 divides by 24 (daily rotation)
  - Fix: update docstring to say "daily"

---

## High Priority

- [ ] **Complete the GCS → BigQuery batch loading pipeline**
  - **Problem:** `functions/batch_load_gcs_to_bq/main.py` is triggered manually and times out for authors with long publication lists. No automated scheduling exists.
  - **Current flow:** fetch functions write JSON to GCS → batch_load reads GCS → wraps in NDJSON → loads to BQ tables (`scholar_raw_data.author`, `scholar_raw_data.pub`) → archives source files
  - Sub-tasks:
  - [ ] Fix timeout for large authors: process files individually or in small batches instead of all-at-once per date folder. Consider streaming inserts or breaking NDJSON into chunks.
  - [ ] Add automated scheduling: use Cloud Scheduler to trigger the batch load function on a regular cadence (e.g., hourly or daily)
  - [ ] Add per-file error handling: a single bad JSON file should not block the entire batch. Move bad files to a dead-letter prefix instead of failing the whole folder.
  - [ ] Add idempotency: track which files have been loaded (e.g., via a manifest in GCS or a BQ metadata table) so retries don't duplicate data
  - [ ] Remove commented-out Firestore saves from Cloud Functions (`fetch_author/main.py:111-117`, `fetch_publication/main.py:92-100`) — the migration to GCS-only is the intended direction
  - Related: "Improve batch_load_gcs_to_bq robustness" in Enhancements (merge with this task)

- [ ] **Materialize percentile tables to reduce BigQuery costs**
  - **Problem:** All percentile views (`stats_publication_current`, `stats_author_current`, `stats_author_publication_pip_inputs_current`, `stats_author_pip_scores_current`) are computed live on every query via expensive `PERCENT_RANK()` window functions. This causes high BigQuery costs and slow page loads.
  - **Reference pattern:** `stats_author_metrics_temporal` is already a materialized view with daily refresh — use the same approach
  - Sub-tasks:
  - [ ] Convert `stats_publication_current` to a materialized view (or scheduled query writing to a table) with daily/monthly refresh. This is the foundation — all other views depend on it.
  - [ ] Convert `stats_author_current` to a materialized table (depends on publication percentiles)
  - [ ] Convert `stats_author_publication_pip_inputs_current` to a materialized table — this is the most expensive view (6-CTE interpolation pipeline)
  - [ ] Convert `stats_author_pip_scores_current` to a materialized table (depends on PiP inputs)
  - [ ] Implement approximate percentile lookup for newly fetched publications not yet in the materialized table: given a publication's citation count and pub_year, find the closest percentile from the pre-computed table for that cohort
  - [ ] Update `shared/services/bigquery_service.py` to query the materialized tables instead of the live views
  - [ ] Set up refresh schedule: daily refresh is likely sufficient since citation data changes slowly. Monthly may also be acceptable for cost savings.
  - [ ] Verify: `bigquery_service.py:56` references `stats_author_current_percentiles` which doesn't exist in the codebase — fix this reference as part of the materialization work

- [ ] **Add environment variable overrides for hardcoded config**
  - `shared/config.py`: `PROJECT_ID`, `BUCKET_NAME`, Firestore collection names, queue names all hardcoded
  - `functions/batch_load_gcs_to_bq/main.py` duplicates project/dataset/bucket IDs
  - Blocks: local development, testing, multi-environment deployment

- [x] **Add BigQuery view deployment to CI/CD**
  - **Problem:** BigQuery SQL views (`bigquery/statistics/*.sql`, `bigquery/coauthor_network/*.sql`) are not deployed by any CI/CD workflow. Changes to view definitions (e.g., the fixes in this PR) require manual execution against BigQuery.
  - **Current state:** `main.yml` deploys Cloud Run; `function.yml` deploys Cloud Functions. Neither touches BigQuery.
  - **Solution:** Added `.github/workflows/bigquery-views.yml` — deploys all 10 views in dependency order (5 tiers), triggers on `bigquery/**/*.sql` changes + manual dispatch
  - Sub-tasks:
  - [x] Add a CI/CD step (in `main.yml` or a new workflow) that runs `bq query --use_legacy_sql=false < file.sql` for each view in `bigquery/statistics/` and `bigquery/coauthor_network/`
  - [x] Ensure views are deployed in dependency order (e.g., `base_author_publications` before `stats_author_current`, `stats_publication_current` before `stats_author_publication_pip_inputs_current`)
  - [x] Only trigger on changes to `bigquery/**/*.sql` files (use `paths` filter in workflow)
  - [x] Use the existing service account for authentication (`gcloud auth` in CI)

- [ ] **Add CI/CD tests**
  - Both workflows deploy without running any tests
  - At minimum: unit tests for shared services, integration tests for BigQuery views
  - Consider: linting, type checking
  - Also: `.github/workflows/function.yml:37` uses outdated `actions/checkout@v3` (should be v4)
  - Also: `.github/workflows/function.yml:92` uses `eval $CMD` — replace with direct execution

- [ ] **Improve error handling across services**
  - `bigquery_service.py`: all methods catch exceptions and return empty structures (empty DataFrame, None, []) — failures are invisible
  - `firestore_service.py`: same pattern
  - `storage_service.py`: `upload_string_to_gcs` returns None on failure
  - Decide: add structured error reporting, or at minimum ensure errors surface to the UI

- [ ] **Reduce redundant Firestore reads in data_analysis.py**
  - `app/data_analysis.py:20-26`: `get_author()` fetches author, then `get_author_last_modification()` re-reads the same doc plus queries all publications
  - 5-7 Firestore reads + 2 BigQuery queries per author page view
  - Fix: refactor `get_author_last_modification()` to accept already-fetched timestamp

- [ ] **Consolidate duplicate service instances across app modules**
  - `app/data_analysis.py:12-15`, `app/coauthor_service.py:12`, `app/refresh.py:14-15`, `app/scholar.py:9`, `app/queue_handler.py:8` each create independent `FirestoreService()`, `BigQueryService()` instances
  - Fix: create `app/services.py` with shared singletons; import from there

---

## Enhancements

- [ ] **Evaluate coauthor query oversample_factor**
  - `app/coauthor_service.py:37`: `oversample_factor=100` fetches 100x requested rows from BigQuery
  - Same pattern in `shared/repositories/author_repository.py:47` (hardcoded `* 100` multiplier for stale author selection)
  - May have unnecessary BigQuery cost; assess whether a smaller factor suffices

- [ ] **Document the region rotation strategy in README**
  - 9-region deployment with daily rotation is a key architectural decision for avoiding Scholar rate-limiting
  - Documented in CLAUDE.md and code comments in `shared/config.py:38-60`, but not in README

- [ ] **Pin dependency versions in requirements.txt**
  - `requirements.txt`: all 13 packages have no `==` version constraints
  - Builds are not reproducible; risk of breaking changes on redeploy

- [ ] **Improve .gitignore coverage**
  - Currently only 2 entries (`.ipynb_checkpoints`, `__pycache__`)
  - Missing: `.env`, `*.pyc`, `venv/`, `.pytest_cache/`, `downloads/`, `.DS_Store`, `*.egg-info/`

- [ ] **Consolidate logging configuration**
  - `app/main.py:42`, `app/data_analysis.py:9`, `app/scholar.py:6`: each call `logging.basicConfig()` independently
  - `shared/config.py:63`: uses `print()` instead of `logging`
  - Fix: single logging config in `main.py`; replace `print()` with `logging.info()`

- [ ] **Clean up empty stub templates**
  - `app/templates/api.html` and `app/templates/help.html` are completely empty (just extend base.html)
  - Either add content or remove the routes

---

## Future / Low Priority

- [ ] **Switch from matplotlib to client-side charting** (e.g., Chart.js, Plotly.js)
  - Would reduce server-side computation
  - Enable interactive plots (hover for paper details, zoom)

- [ ] **Add author comparison feature**
  - Side-by-side PiP-AUC and percentile plots for multiple authors

- [ ] **Make region rotation dynamic per-request** instead of fixed at import time
  - Currently `Config.FUNCTION_LOCATION` is set once when the module loads
  - Long-running Cloud Run instances may use the same region for days

- [ ] **Add OIDC authentication for Cloud Function endpoints**
  - `task_queue_service.py` has OIDC token config commented out
  - Functions currently deploy with `--allow-unauthenticated`

- [ ] **Add security headers and CSRF protection**
  - No Content-Security-Policy, X-Frame-Options, X-Content-Type-Options, HSTS headers
  - No CSRF tokens on forms

- [ ] **Update frontend dependencies**
  - jQuery 3.3.1 (2018) -> 3.7.x
  - Bootstrap 4.3.1 (2018) -> 5.x
  - Add `&display=swap` to Google Fonts URL for better loading

- [ ] **Improve accessibility**
  - Table sorting headers lack keyboard support (`role="button"`, `tabindex`)
  - Autocomplete dropdown not ARIA-compliant
  - Deprecated `align` attributes in templates — use CSS instead

---

_Last updated: 2026-03-18_
