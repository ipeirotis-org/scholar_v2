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

## Critical — Data Correctness (actively wrong in production)

- [ ] **Fix hardcoded year 2025 in temporal citations view — losing 2026 data NOW**
  - `bigquery/statistics/stats_publication_citations_temporal.sql:45`: `GENERATE_ARRAY(pub_year, 2025)` excludes all 2026+ citation data
  - Fix: replace `2025` with `EXTRACT(YEAR FROM CURRENT_DATE())`

- [ ] **Fix hardcoded year 2024 in results template**
  - `app/templates/results.html:21`: `{{ 2024 - author.stats.year_of_first_pub + 1 }}` — wrong since Jan 2025
  - Fix: pass current year from route handler or use Jinja2 `now()` function

- [ ] **Fix first publication excluded from PiP-AUC score**
  - `bigquery/statistics/stats_author_pip_scores_current.sql:9-22`: `LAG()` produces NULL for first publication; `WHERE` on line 22 drops it
  - The area from x=0 to the first publication's `num_papers_percentile` is missing from AUC
  - Fix: use `COALESCE(prev_num_papers_percentile, 0)` and `COALESCE(prev_num_citations_percentile, num_citations_percentile)` to treat first pub as origin point

- [ ] **Fix `base_author_publications` view deploy failure**
  - `bigquery/statistics/base_author_publications.sql:1`: uses `CREATE VIEW` instead of `CREATE OR REPLACE VIEW` with project prefix
  - Fix: change to `` CREATE OR REPLACE VIEW `scholar-version2.statistics.base_author_publications` AS ``

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

- [ ] **Fix or remove disabled Firestore saves in Cloud Functions**
  - `functions/fetch_author/main.py:111-117`: `save_author()` commented out
  - `functions/fetch_publication/main.py:92-100`: `save_publication()` / Firestore writes commented out
  - Current flow: data goes only to GCS -> async BigQuery ETL via `batch_load_gcs_to_bq`
  - Impact: newly fetched authors/pubs don't appear in Firestore cache until batch ETL runs
  - Decision needed: is real-time Firestore still needed, or is async-only acceptable?

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

- [ ] **Add environment variable overrides for hardcoded config**
  - `shared/config.py`: `PROJECT_ID`, `BUCKET_NAME`, Firestore collection names, queue names all hardcoded
  - `functions/batch_load_gcs_to_bq/main.py` duplicates project/dataset/bucket IDs
  - Blocks: local development, testing, multi-environment deployment

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

- [ ] **Improve batch_load_gcs_to_bq robustness**
  - No retry on BigQuery load failures
  - Archive happens per-folder; a single bad file in a folder blocks the whole batch
  - Consider: per-file processing, dead-letter handling for malformed JSON

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

- [ ] **Explore BigQuery scheduled queries** to replace manual `batch_load_gcs_to_bq` triggers

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

_Last updated: 2026-03-16_
