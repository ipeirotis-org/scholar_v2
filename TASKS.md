# TASKS: Scholar Analytics v2

> Maintain and improve the Scholar Analytics platform — percentile-based, age-aware research metrics with PiP-AUC scoring.
>
> Live at [scholar.ipeirotis.org](https://scholar.ipeirotis.org/)

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
  - Current flow: data goes only to GCS → async BigQuery ETL via `batch_load_gcs_to_bq`
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

- [ ] **Improve error handling across services**
  - `bigquery_service.py`: all methods catch exceptions and return empty structures (empty DataFrame, None, []) — failures are invisible
  - `firestore_service.py`: same pattern
  - `storage_service.py`: `upload_string_to_gcs` returns None on failure
  - Decide: add structured error reporting, or at minimum ensure errors surface to the UI

---

## Enhancements

- [ ] **Evaluate coauthor query oversample_factor**
  - `app/coauthor_service.py:37`: `oversample_factor=100` fetches 100x requested rows from BigQuery
  - Same pattern in `shared/repositories/author_repository.py:47` (hardcoded `* 100` multiplier for stale author selection)
  - May have unnecessary BigQuery cost; assess whether a smaller factor suffices

- [ ] **Document the region rotation strategy in README**
  - 9-region deployment with daily rotation is a key architectural decision for avoiding Scholar rate-limiting
  - Documented in CLAUDE.md and code comments in `shared/config.py:38-60`, but not in README
  - Note: `get_rotating_region()` docstring says "hourly" but code actually rotates daily — fix the docstring

- [ ] **Improve batch_load_gcs_to_bq robustness**
  - No retry on BigQuery load failures
  - Archive happens per-folder; a single bad file in a folder blocks the whole batch
  - Consider: per-file processing, dead-letter handling for malformed JSON

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

---

_Last updated: 2026-03-05_
