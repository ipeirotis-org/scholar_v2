# Codebase Review — Scholar Analytics v2

**Date:** 2026-03-24
**Commit:** 3eedf0dda7666156424cb191fa9be86eb491e734
**Scope:** Full codebase audit across all runtime components (frontend, cache_layer, crawler, ingestion, refresh, author_search) + shared modules + CI/CD + SQL analytics

---

## 1. Architecture & Organization

### 1.1 No Shared Utility Module — Duplicated Code Across Components

**Risk: Medium | Maintainability**

There is no `shared/` or `common/` directory. Several utilities are duplicated across components:

| Duplicated Pattern | Files | ~Lines Duplicated |
|---|---|---|
| `bq_view()` / `bq_raw()` helpers | `cache_layer/config.py`, `author_search/config.py`, `refresh/config.py`, `frontend/config.py` | ~6 lines × 4 = 24 |
| `_get_client()` singleton + `_sanitize_task_id()` | `crawler/task_enqueuer.py:19-31`, `refresh/task_enqueuer.py:20-32` | ~25 lines × 2 |
| `_trigger_batch_load()` fire-and-forget | `crawler/fetch_author.py:27-35`, `crawler/fetch_publication.py:26-34` | ~9 lines × 2 |
| OIDC token generation block | `crawler/task_enqueuer.py:34-40`, `refresh/task_enqueuer.py:34-48` | ~12 lines × 2 |

**Fix:** Extract `bq_helpers.py` and `task_client.py` into `region_health/` (already a shared module) or a new `shared/` package.

### 1.2 Inconsistent Document ID Normalization

**Risk: High | Data Integrity**

Three different approaches strip the `.json` suffix from legacy document IDs:

- `ingestion/batch_load.py`: `os.path.basename(name).removesuffix(".json")`
- `ingestion/cache_enqueuer.py:52`: `re.sub(r"\.json$", "", doc_id)`
- `bigquery/materialize_dedup_tables.sql:36-42`: `CASE WHEN ENDS_WITH(document_id, '.json') THEN SUBSTR(...)`

If any one of these diverges, data mismatches between BigQuery and Firestore cache become silently possible.

**Fix:** Create a single `normalize_document_id()` utility and use it everywhere.

### 1.3 `region_health` Component Not Documented

**Risk: Low | Developer Experience**

The `region_health/` module (config, router, scorer) is imported by crawler, frontend, and refresh but is absent from `CLAUDE.md`'s architecture diagram and `docs/ARCHITECTURE.md`.

**Fix:** Add `region_health/` to the architecture docs as a shared library.

### 1.4 Region List Inconsistency

**Risk: Medium | Operations**

- CI/CD workflows (`deploy-crawler.yml`, `deploy-author-search.yml`) deploy to **15 regions**
- `scripts/backfill_authors.py:47-57` hardcodes **9 regions**
- `region_health/config.py` defines `AVAILABLE_FUNCTION_REGIONS` (15 regions)

No single source of truth is used by both CI/CD and Python code.

**Fix:** CI/CD should read region lists from `region_health/config.py` or a shared config file, not hardcode them.

### 1.5 Stale Script: `scripts/resolve_authors.py`

**Risk: Low | Code Hygiene**

The docstring (line 7) references an `app/` directory with `scholar.py` that no longer exists in the codebase. The script itself runs against Firestore collections (`scholar_raw_pub`, `scholar_raw_author`) that predate the current BigQuery-based architecture. It is functional but operates on a data model the system no longer uses.

**Fix:** Delete if the Firestore collections are no longer populated, or update the docstring and verify it still serves a purpose.

---

## 2. Code Quality

### 2.1 `cache_writer.py` — Batch Failures Silently Inflate Return Count

**File:** `cache_layer/cache_writer.py:60-74`

`write_batch()` returns the total count of items *attempted*, not items *committed*. If `batch.commit()` fails at line 64, the count is not decremented, but a new batch is started. The caller sees `{"writes": 500}` when 0 actually persisted.

```python
if count % 500 == 0:
    try:
        batch.commit()
    except Exception:
        logger.exception("Batch commit failed at count %d", count)
    batch = self.db.batch()  # silently starts new batch
```

**Fix:** Track `committed` separately from `attempted`. On commit failure, either retry or subtract from the return count.

### 2.2 Inconsistent BigQuery Client Patterns

**Risk: Low | Consistency**

| Component | Pattern | Testability |
|---|---|---|
| `cache_layer/bigquery_client.py` | Instance class (`BigQueryClient`) | Good (DI via constructor) |
| `author_search/bigquery_client.py` | Instance class (`BigQuerySearchClient`) | Good |
| `refresh/bigquery_client.py` | Module-level functions + global `_client` | Poor (requires module-level patching) |

**Fix:** Refactor `refresh/bigquery_client.py` to a class for consistency and testability.

### 2.3 Inconsistent Return Types in `cache_layer/bigquery_client.py`

**Risk: Medium | Bugs**

- `get_author_stats()` returns `None` on empty result (line 71)
- `get_publication_stats()` returns `[]` on empty result (line 94)
- `get_author_temporal_stats()` returns `[]` on empty result

Callers must handle both `None` and `[]`, inviting `TypeError` on `for item in result`.

**Fix:** Standardize: always return `[]` for list queries, `None` for single-document queries.

### 2.4 Long Route Handler: `frontend/routes.py` `/results` Endpoint

**Risk: Low | Readability**

The `/results` handler spans ~94 lines (154-248), mixing cache reads, plot generation, data formatting, and template rendering.

**Fix:** Extract `_generate_and_cache_plots()` and `_format_author_data()` helper functions.

### 2.5 In-Memory Deduplication Masks Upstream View Bug

**File:** `cache_layer/bigquery_client.py:55-56`

```python
# Guard against upstream view issues
df = df.drop_duplicates(subset=["author_pub_id"], keep="first")
```

This pandas dedup hides a potential BigQuery view bug. If `stats_author_publication_pip_inputs_current` returns duplicates, the root cause should be fixed in SQL.

**Fix:** Add a monitoring alert when duplicates are detected, then fix the view.

---

## 3. Error Handling & Robustness

### 3.1 Admin Endpoints Have No Authentication

**File:** `cache_layer/main.py:55-80`

`/admin/rebuild` and `/admin/populate` accept unauthenticated POST requests. Any HTTP client that can reach the Cloud Run service can trigger a full cache rebuild.

**Fix:** Require IAM-authenticated requests (Cloud Run's `--ingress internal` or verify OIDC tokens).

### 3.2 Archive Failures Can Cause Duplicate BigQuery Loads

**File:** `ingestion/batch_load.py:160-175`

If `copy_blob` / `delete` fails during archival, the source file remains in the input prefix. The next batch load invocation will re-process and re-load the same data to BigQuery (WRITE_APPEND), creating duplicates.

The `author_latest` / `pub_latest` dedup views mask this at query time, but the raw tables grow unboundedly.

**Fix:** Use a "processed" marker (e.g., metadata flag or separate tracking collection) instead of relying solely on file move for idempotency.

### 3.3 No Retry for Transient BigQuery Failures in Refresh

**File:** `refresh/bigquery_client.py:27-34`

`_query()` has no retry logic. A single 503 from BigQuery aborts the entire refresh cycle (stale authors, error recovery, coauthor expansion).

**Fix:** Wrap with `google.api_core.retry.Retry` or implement exponential backoff for transient errors.

### 3.4 `refresh/refresh_service.py:79` — Unprotected `author_exists()` Call

```python
exists = bq.author_exists(scholar_id)  # Can throw — not wrapped in try/except
try:
    enqueued = task_enqueuer.enqueue_author(scholar_id)
```

If `author_exists()` raises, the entire `/api/fetch_author` endpoint returns 500 with no structured error.

**Fix:** Wrap the existence check in the same try/except block.

### 3.5 Overly Broad Exception Handling Throughout

Multiple components catch bare `Exception` and log it, treating all errors identically:

| File | Line(s) | Impact |
|---|---|---|
| `frontend/cache.py` | 37-39 | Network timeout and permission error handled identically |
| `cache_layer/cache_service.py` | 177-179 | 100k rebuild tasks all fail if queue is misconfigured; no fail-fast |
| `ingestion/cache_enqueuer.py` | 94-95 | Transient vs permanent failure not distinguished |

**Fix:** Catch specific exceptions (e.g., `google.api_core.exceptions.ServiceUnavailable` for retry, `PermissionDenied` for fail-fast). Keep broad `Exception` as a last-resort fallback.

### 3.6 `author_search/scholar_client.py` — No Timeout on Scholar Search

`scholarly.search_author()` has no timeout wrapper, unlike the crawler's `scholarly_client.py` which uses `ThreadPoolExecutor` with a 300s timeout. A slow or hanging Scholar API call blocks the Cloud Function indefinitely (until platform timeout).

**Fix:** Add `concurrent.futures.ThreadPoolExecutor` timeout wrapper matching the crawler pattern.

---

## 4. Security

### 4.1 URL Parameters Not Encoded in Refresh Calls

**File:** `frontend/routes.py:76-78`

```python
qs = "&".join(f"{k}={v}" for k, v in params.items())
url = f"{url}?{qs}"
```

Parameter values are not URL-encoded. While current callers pass safe integers, this is fragile.

**Fix:** Use `urllib.parse.urlencode(params)`.

### 4.2 Hardcoded Service Account in Backfill Script

**File:** `scripts/backfill_authors.py:160-161`

```python
"oidcToken": {
    "serviceAccountEmail": "875626982900-compute@developer.gserviceaccount.com",
```

The backfill script hardcodes the default Compute Engine service account directly in the REST payload. The same email is also hardcoded as the default for `CLOUD_TASKS_SA_EMAIL` in `crawler/config.py:19-21`, `frontend/config.py:48-50`, and `refresh/config.py:24-26` — though those at least support env var overrides.

**Fix:** Read from config or environment variable in the backfill script, matching the pattern used by other components.

### 4.3 No CSRF Protection on State-Changing GET Endpoints

**File:** `frontend/routes.py`

`/api/fetch_authors`, `/api/rebuild_statistics`, `/api/add_coauthors` accept GET requests that trigger state changes (Cloud Tasks enqueue, refresh calls). No CSRF token validation.

**Fix:** Change to POST-only endpoints, or add CSRF token validation.

### 4.4 No Rate Limiting on API Endpoints

**File:** `frontend/routes.py`

`/api/refresh_stale_authors`, `/api/add_coauthors`, `/api/rebuild_statistics` can be called repeatedly without throttling, each triggering expensive BigQuery queries and Cloud Tasks enqueue operations.

**Fix:** Add rate limiting (e.g., Flask-Limiter) or deduplication checks.

---

## 5. Performance

### 5.1 Synchronous Matplotlib Rendering Blocks Request Thread

**File:** `frontend/routes.py:129-152`

Plot generation (two matplotlib renders + DataFrame operations) runs synchronously in the Flask request handler. For authors with 500+ publications, this can take several seconds, blocking the thread.

**Fix:** Pre-generate plots in the cache layer during cache population. The frontend should only read pre-rendered plot data from Firestore.

### 5.2 `get_all_author_ids()` Loads Entire Author Table Into Memory

**File:** `cache_layer/bigquery_client.py:161-178`

No LIMIT, no pagination. Called by `_rebuild_all()` which then loops through all IDs to enqueue tasks. With 100k+ authors, this spikes memory.

**Fix:** Use BigQuery pagination (`page_size` parameter on query results) and process in chunks.

### 5.3 Loading Page Polls Every 10 Seconds Without Backoff

**File:** `frontend/templates/loading.html:29`

```javascript
setTimeout(function() {
    window.location.href = "/results?...";
}, 10000);
```

If many users trigger cache misses simultaneously, the server gets hammered with reload requests.

**Fix:** Exponential backoff (10s → 15s → 22s → ...) or switch to server-sent events.

### 5.4 Health Dashboard Runs Uncached BigQuery Queries

**File:** `frontend/health_service.py`

Every dashboard refresh runs 10+ BigQuery queries with no result caching. Each query scans `author_latest` and `pub_latest` tables.

**Fix:** Cache dashboard results in Firestore with a 5-minute TTL.

### 5.5 Coauthor Sampling Oversamples in SQL, Then Samples in Python

**File:** `refresh/bigquery_client.py:103-135`

Fetches 10× the needed coauthors from BigQuery, then `random.sample()` in Python. This wastes query bandwidth.

**Fix:** Use `ORDER BY RAND() LIMIT @limit` in the SQL query.

---

## 6. Testing Gaps

### 6.1 No Tests for Batch Commit Failures in `cache_writer.py`

**File:** `cache_layer/tests/test_cache_writer.py`

Tests verify the 500-operation batch split but never test what happens when `batch.commit()` raises an exception. Given that failed commits silently inflate the return count (see 2.1), this is a significant gap.

### 6.2 No Tests for Race Conditions in `search_service.py` Index Loading

**File:** `author_search/search_service.py:90-111`

The double-checked locking pattern in `_ensure_index_loaded()` is untested for concurrent access. Thread-safety bugs would only manifest under load.

### 6.3 Mocked BigQuery Clients Don't Verify SQL Correctness

All BigQuery client tests mock the `client.query()` call entirely. Parameterized query binding, SQL syntax, and LIMIT enforcement are never verified. A typo in SQL would pass all tests.

**Fix:** Add at least one integration test per component that runs against a test BigQuery dataset, or use SQL linting (e.g., `sqlfluff`).

### 6.4 No Tests for `region_health` Module

The `region_health/` directory has a `tests/` folder but test coverage was not verified. Given that this module is imported by 3 components and controls routing decisions, it needs comprehensive tests for:
- Weighted random selection
- Cache TTL expiry
- Fallback when Firestore is unavailable

### 6.5 Cache Staleness Boundary Not Tested in `author_search/cache.py`

Tests verify fresh and stale cache states but don't test the exact boundary (TTL = 24 hours). Off-by-one errors in timestamp comparison would go undetected.

### 6.6 `ingestion/batch_load.py` — No Tests for Concurrent Invocations

Two simultaneous Cloud Function invocations could both list and process the same GCS files, leading to duplicate BigQuery loads. No test verifies this race condition.

---

## 7. Developer Experience

### 7.1 Config Defaults Target Production

All `config.py` files default to production values (e.g., `PROJECT_ID = "scholar-version2"`). Developers must set environment variables for local development, with no `.env.example` or documentation of required overrides.

**Fix:** Add a `.env.example` file listing all configurable environment variables with safe defaults for local development.

### 7.2 Missing Type Annotations

Most functions across the codebase lack return type annotations. For example:

- `frontend/routes.py:54`: `def _validate_scholar_id(scholar_id)` → should be `-> Optional[str]`
- `frontend/cache.py:28`: `def get(self, collection, doc_id)` → should be `-> Optional[dict]`
- All `cache_layer/bigquery_client.py` methods

**Fix:** Add type annotations incrementally, starting with public API methods.

### 7.3 No Pre-commit Hooks or Linting Configuration

No `.pre-commit-config.yaml`, `pyproject.toml` with linting config, or `ruff.toml` was found. Code style is consistent by convention, not enforcement.

**Fix:** Add `ruff` or `flake8` configuration and a pre-commit hook.

### 7.4 TASKS.md and CLAUDE.md Reference "9 Regions" but Infrastructure Uses 15

Documentation hasn't been updated to reflect the expansion to 15 regions. New developers will be confused by the mismatch.

---

## Summary by Priority

### Critical (Data Integrity / Security)
1. **Batch write failures silently inflate counts** — `cache_layer/cache_writer.py:60-74`
2. **Archive failures cause duplicate BigQuery loads** — `ingestion/batch_load.py:160-175`
3. **Admin endpoints have no authentication** — `cache_layer/main.py:55-80`
4. **Inconsistent document ID normalization** — 3 different implementations across ingestion + BigQuery

### High (Production Reliability)
5. **No retry for transient BigQuery failures** — `refresh/bigquery_client.py:27-34`
6. **No timeout on Scholar search** — `author_search/scholar_client.py`
7. **Unprotected `author_exists()` call** — `refresh/refresh_service.py:79`
8. **Synchronous matplotlib blocking requests** — `frontend/routes.py:129-152`

### Medium (Maintainability / Performance)
9. **Duplicated utilities across components** — config helpers, task enqueuers, batch load triggers
10. **Inconsistent return types** — `cache_layer/bigquery_client.py` (None vs [])
11. **No rate limiting on API endpoints** — `frontend/routes.py`
12. **Health dashboard queries uncached** — `frontend/health_service.py`
13. **URL parameters not encoded** — `frontend/routes.py:76-78`
14. **Region list not single-sourced** — CI/CD vs scripts vs config

### Low (Code Hygiene / DX)
15. **Stale script** — `scripts/resolve_authors.py`
16. **Missing type annotations** — throughout
17. **No linting/pre-commit config** — project-wide
18. **Config defaults target production** — no `.env.example`
19. **`region_health` not in architecture docs** — `docs/ARCHITECTURE.md`
