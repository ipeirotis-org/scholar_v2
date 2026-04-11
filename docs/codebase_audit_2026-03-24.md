# Codebase Audit Report (2026-03-24)

## Critical

### 1) Unauthenticated internal task endpoints can be invoked without identity checks
- **Location:** `cache_layer/main.py:25-35, 37-53, 55-79`
- **Risk:** `/tasks/*` and `/admin/*` routes accept JSON and trigger expensive cache writes/rebuild fan-out, but there is no request authentication/authorization check in the handler. If Cloud Run ingress/IAM is ever relaxed or misconfigured, these become high-impact abuse endpoints.
- **Concrete fix:** Enforce auth in-app (defense in depth): verify `Authorization` bearer token audience/issuer or an allowlisted task header + signed token; reject unauthenticated calls with 401 before parsing payload.

### 2) Several task producers omit OIDC tokens, creating either insecure or brittle invocation paths
- **Location:** `cache_layer/cache_service.py:163-170, 186-193`; `ingestion/cache_enqueuer.py:82-89`
- **Risk:** Tasks created without `oidc_token` can fail in authenticated environments (silent data staleness) or force services to remain unauthenticated (security regression).
- **Concrete fix:** Add `oidc_token` to every Cloud Tasks HTTP request, using the same service account/audience pattern as `frontend/queue_client.py` and `refresh/task_enqueuer.py`.

### 3) State-changing operations are exposed as GET routes
- **Location:** `frontend/routes.py:339-407`
- **Risk:** `/api/fetch_authors`, `/api/rebuild_statistics`, `/api/refresh_stale_authors`, `/api/add_coauthors`, and `/api/refresh_author_index` mutate system state via GET. This makes CSRF, crawler-triggered actions, and accidental link-prefetch side effects more likely.
- **Concrete fix:** Convert to `POST` with CSRF/session protection for browser-triggered calls, require JSON body, and validate origin/auth where appropriate.

## High

### 4) Batch write success accounting is incorrect on Firestore commit failure
- **Location:** `cache_layer/cache_writer.py:53-73`
- **Risk:** `write_batch` returns `count` even when `batch.commit()` fails, so callers can report success while writes were dropped. This causes stale cache with misleading status.
- **Concrete fix:** Track committed writes separately; if a commit fails, decrement/avoid incrementing committed count and return partial success metadata (`attempted`, `committed`, `failed_batches`).

### 5) Refresh BigQuery client has no query error handling at data-access boundary
- **Location:** `refresh/bigquery_client.py:27-35`
- **Risk:** Network/API exceptions propagate out of `_query`, causing endpoint 500s and scheduler job failures without structured fallback.
- **Concrete fix:** Catch provider exceptions in `_query`, log query context safely, and return a controlled empty result or typed error that service layer can map to retry/non-retry behavior.

### 6) Non-atomic read-modify-write on recent author list can lose updates under concurrency
- **Location:** `frontend/cache.py:105-117`
- **Risk:** `record_recent_author` reads current list, mutates it, then writes back without transaction/compare-and-set. Concurrent requests can overwrite each other, losing recent entries.
- **Concrete fix:** Use Firestore transaction with retry or store recents in per-author docs with server timestamps and query top-N.

### 7) In-memory author index refresh never cleans up stale Firestore chunks
- **Location:** `author_search/search_service.py:50-57`
- **Risk:** If the author count shrinks, old `chunk_N` documents remain. `_load_index_from_firestore` keeps reading until a missing chunk, so stale chunks can be reloaded and surface deleted/old records.
- **Concrete fix:** During save, track new chunk count and delete surplus old chunk docs (or write a manifest doc with exact chunk_count and load by manifest).

### 8) Monitoring/logging dashboard endpoints can become very expensive (hot path overload)
- **Location:** `frontend/health_service.py:267-305, 420-447`
- **Risk:** `get_queue_stats` iterates all tasks (`list_tasks`) per queue; `get_function_error_breakdown` scans logs per function per time window. `/health-dashboard` and `/api/health` can trigger large repeated API scans, causing high latency/cost.
- **Concrete fix:** Pre-aggregate periodically to Firestore/BigQuery, cache dashboard snapshots with TTL, and cap/short-circuit per-request scans.

## Medium

### 9) URL query string is manually concatenated without encoding
- **Location:** `frontend/routes.py:75-79`
- **Risk:** `_call_refresh_function` builds `k=v` pairs without URL encoding. Special chars can break parameter parsing or alter semantics.
- **Concrete fix:** Use `urllib.parse.urlencode(params, doseq=True)`.

### 10) Duplicate enqueue/client scaffolding across modules increases drift risk
- **Location:** `frontend/queue_client.py`, `crawler/task_enqueuer.py`, `refresh/task_enqueuer.py`, `ingestion/cache_enqueuer.py`
- **Risk:** Task construction (queue path, OIDC audience, task-id sanitization, duplicate handling) is reimplemented 4 times; behavior already diverges (e.g., OIDC present vs absent).
- **Concrete fix:** Extract a shared `cloud_tasks_client` helper module with standard task builders and auth defaults; keep component-specific payloads only.

### 11) Inconsistent resilience strategy for optional downstream actions
- **Location:** `refresh/refresh_service.py:26, 46, 65, 106`; `ingestion/batch_load.py:314-322`
- **Risk:** Cache warming/invalidation failures are swallowed as non-fatal in some paths but not reported in returned status, making operational visibility poor.
- **Concrete fix:** Return explicit partial-failure fields (`cache_warm_enqueued`, `cache_warm_errors`) and emit structured metrics counters.

## Testing gaps

### 12) CacheWriter tests do not cover commit-failure accounting semantics
- **Location:** `cache_layer/tests/test_cache_writer.py:37-68`
- **Risk:** Current tests assert commit call counts but not correctness when `batch.commit()` fails. Regressions can silently misreport cache population success.
- **Concrete fix:** Add tests with `batch.commit.side_effect` on first/final commit and assert returned committed count + error metadata.

### 13) Ingestion enqueuer tests do not enforce authenticated task payload requirements
- **Location:** `ingestion/tests/test_cache_enqueuer.py:46-71`
- **Risk:** Tests verify enqueue call count only, so missing `oidc_token` is not caught.
- **Concrete fix:** Assert the `task` argument includes `http_request.oidc_token.service_account_email` and expected `audience`.

### 14) Refresh BigQuery tests mock `_query` and never validate exception behavior
- **Location:** `refresh/tests/test_bigquery_client.py:6-127`
- **Risk:** No tests for transport/query exceptions means scheduler failure modes are untested.
- **Concrete fix:** Add tests where underlying client query raises and assert controlled return values + logging behavior.

