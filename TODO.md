# TODO: Scholar Analytics v2

> **Goal**: Maintain and improve the Scholar Analytics platform — percentile-based, age-aware research metrics with PiP-AUC scoring.
>
> Last reviewed: 2026-02-15.

---

## Bugs / Fixes

- [ ] Re-enable temporal stats visualization
  - Code in `app/main.py:157-173` is commented out
  - Temporal plot functions exist in `visualization.py` but are unreachable from web UI
  - Decide: re-enable with current schema or redesign temporal data pipeline

- [ ] Fix or remove disabled Firestore saves in Cloud Functions
  - `fetch_author/main.py` and `fetch_publication/main.py` have `save_author()` / Firestore writes commented out
  - Currently data only goes to GCS → async BigQuery ETL
  - Decide: is Firestore cache still needed for real-time display, or is async-only acceptable?

- [ ] Implement retry logic for GCS upload failures
  - `fetch_author/main.py:130-133` — GCS upload failure returns None with no retry
  - Publication enqueuing logs errors but continues silently

---

## High Priority

- [ ] Implement Firestore-based task status tracking
  - `task_queue_service.py` — `get_number_of_tasks_in_queue()` returns None
  - Code comments suggest tracking task status in Firestore instead of querying Cloud Tasks API
  - Would enable progress indicators in the web UI

- [ ] Add environment variable overrides for hardcoded config
  - `shared/config.py` hardcodes `PROJECT_ID = "scholar-version2"`, Firestore collection names, bucket name
  - `batch_load_gcs_to_bq` duplicates project/dataset IDs
  - Enable env var override for all constants

---

## Enhancements

- [ ] Evaluate coauthor query oversample_factor
  - `coauthor_service.py:37` defaults to `oversample_factor=100`
  - May retrieve excessive rows from BigQuery; assess cost impact

- [ ] Add CI/CD tests for Cloud Functions
  - Currently deploys without running any tests
  - At minimum: unit tests for shared services, integration tests for data flow

- [ ] Document the region rotation strategy
  - 9-region deployment with hourly rotation is a key architectural decision
  - Add explanation to README for maintainability

---

## Future / Low Priority

- [ ] Consider switching from matplotlib to client-side charting (e.g., Chart.js)
  - Would reduce server-side computation
  - Enable interactive plots (hover, zoom)

- [ ] Add author comparison feature
  - Side-by-side PiP-AUC and percentile plots for multiple authors

- [ ] Explore BigQuery scheduled queries to replace manual batch_load triggers

---

_Last updated: 2026-02-15_
