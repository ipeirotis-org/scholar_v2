# TASKS: PiP Score

> Maintain and improve the PiP Score platform — percentile-based, age-aware research metrics with PiP-AUC scoring.
>
> Live at [pip-score.org](https://www.pip-score.org/)
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

### Phase 2: Cloud Tasks queues + wiring ✓

- [x] **Wire ingestion to publish cache invalidation events**
  - `ingestion/cache_enqueuer.py` — extracts scholar IDs from NDJSON, enqueues `invalidate_author` to priority queue
  - `ingestion/batch_load.py` — calls cache enqueuer after successful BQ load (non-fatal on failure)
  - 8 new tests for cache_enqueuer
- [x] **Create Cloud Tasks queues** (via CI/CD `ensure-queues` job)
  - `cache-priority` — 50 dispatches/sec, 20 concurrent, 3 attempts
  - `cache-batch` — 5 dispatches/sec, 3 concurrent, 3 attempts
  - Region: `northamerica-northeast1`
- [x] **Wire refresh service to publish warm events**
  - `refresh/task_enqueuer.py` — `enqueue_cache_warm()` + `enqueue_cache_warm_batch()`
  - All 5 refresh operations now enqueue cache warming after crawl tasks
  - 6 new tests for cache warming
- [x] **Add scheduled task for `populate_recent_authors`**
  - Cloud Scheduler `v3-populate-recent-authors` → cache-batch queue, every 5 minutes
- [x] **CI/CD: deploy cache_layer as Cloud Run service**
  - Updated `.github/workflows/main.yml` with full deployment pipeline
  - Deploy order: queues → cache_layer → frontend + refresh (parallel) → schedulers
  - `CACHE_LAYER_URL` env var propagated to frontend, refresh, and cache_layer services

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

### Remaining post-merge tasks

- [ ] **Run initial cache rebuild** — after first deploy, `POST /admin/rebuild` to populate Firestore from BigQuery
- [ ] **Verify end-to-end flow** — crawl → ingest → cache invalidation → frontend serves from cache
- [ ] **Update frontend loading/polling UX** (optional)
  - Existing loading page pattern already handles "data not ready" state
  - Consider client-side polling or short auto-refresh for smoother cache-miss UX

---

## Codebase Health — from [codebase review](docs/codebase-review.md) (2026-03-24)

Findings from a full codebase audit, ordered by priority. See `docs/codebase-review.md` for detailed analysis with file/line references.

### P0 — Critical (Data Integrity / Security)

- [x] **Fix `cache_writer.py` batch failure accounting** _(review §2.1)_
  - `write_batch()` now tracks `committed` separately from `pending`
  - Only increments `committed` after successful `batch.commit()`
  - **Files:** `cache_layer/cache_writer.py`
  - **Tests:** 3 new batch commit failure tests in `cache_layer/tests/test_cache_writer.py`

- [x] **Prevent duplicate BigQuery loads from archive failures** _(review §3.2)_
  - Added `bq_loaded` metadata marker on GCS blobs after successful BQ load
  - `iter_gcs_files()` skips blobs already marked, preventing re-processing if archive fails
  - **Files:** `ingestion/batch_load.py`

- [x] **Add authentication to cache_layer admin endpoints** _(review §3.1)_
  - Added `require_admin_auth` decorator with Bearer token verification
  - Configured via `CACHE_LAYER_ADMIN_TOKEN` env var; when empty, allows all (backwards-compatible)
  - **Files:** `cache_layer/main.py`, `cache_layer/config.py`
  - **Tests:** 7 new auth tests in `cache_layer/tests/test_main.py`

- [x] **Unify document ID normalization** _(review §1.2)_
  - Created `ingestion/normalize.py` with single `normalize_document_id()` function
  - Updated `batch_load.py` and `cache_enqueuer.py` to use the shared function
  - Added cross-reference comment in `dedup_views.sql` to keep SQL aligned
  - **Files:** `ingestion/normalize.py`, `ingestion/batch_load.py`, `ingestion/cache_enqueuer.py`, `ingestion/dedup_views.sql`
  - **Tests:** 7 tests in `ingestion/tests/test_normalize.py`

### P1 — High (Production Reliability)

- [x] **Add retry logic for transient BigQuery failures in refresh** _(review §3.3)_
  - `refresh/bigquery_client.py` — `_query()` now uses `google.api_core.retry.Retry` with exponential backoff
  - Retries on `ServerError` (5xx) and `TooManyRequests` (429), up to 120s deadline

- [x] **Add timeout to Scholar search in author_search** _(review §3.6)_
  - Resolved by S2 migration: `author_search/s2_client.py` uses `requests.get(timeout=Config.S2_TIMEOUT_SECONDS)` (default 10s)
  - Old `scholarly` client with no timeout has been removed

- [x] **Wrap `author_exists()` call in try/except** _(review §3.4)_
  - `refresh/refresh_service.py` — `author_exists()` now wrapped in try/except; returns `None` on failure
  - Enqueue proceeds regardless; structured error response always returned

- [ ] **Move plot generation out of the request thread** _(review §5.1)_
  - `frontend/routes.py:129-152` — synchronous matplotlib rendering blocks Flask worker
  - For authors with 500+ publications, can take several seconds
  - **Fix:** Pre-generate plots in cache_layer during cache population, or generate async

### P2 — Medium (Maintainability / Performance / Security)

- [ ] **Extract shared utilities to reduce cross-component duplication** _(review §1.1)_
  - `bq_view()`/`bq_raw()` duplicated in config files across frontend, cache_layer, author_search
  - **Fix:** Create `shared/` package with `bq_helpers.py`

- [ ] **Standardize return types in `cache_layer/bigquery_client.py`** _(review §2.3)_
  - `get_author_stats()` returns `None` on empty; `get_publication_stats()` returns `[]`
  - Callers must handle both, inviting `TypeError`
  - **Fix:** Return `[]` for list queries, `None` for single-document queries — consistently

- [ ] **Add rate limiting to frontend API endpoints** _(review §4.4)_
  - `/api/refresh_stale_authors`, `/api/add_coauthors`, `/api/rebuild_statistics` have no throttling
  - Each call triggers expensive BigQuery queries and Cloud Tasks enqueue
  - **Fix:** Add Flask-Limiter or deduplication checks

- [x] ~~**Cache health dashboard BigQuery queries**~~ _(resolved: health_service.py removed with crawler cleanup)_

- [x] ~~**URL-encode parameters in refresh function calls**~~ _(resolved: refresh functions removed)_

- [x] ~~**Single-source the region list**~~ _(resolved: crawler + region_health removed)_

- [ ] **Change state-changing API endpoints from GET to POST** _(review §4.3)_
  - `/api/fetch_authors`, `/api/rebuild_statistics`, `/api/add_coauthors` trigger side effects via GET
  - **Fix:** Change to POST-only; consider adding CSRF token validation

- [ ] **Narrow overly broad exception handling** _(review §3.5)_
  - `frontend/cache.py:37-39`, `cache_layer/cache_service.py:177-179`, `ingestion/cache_enqueuer.py:94-95`
  - All catch bare `Exception`, treating transient and permanent failures identically
  - **Fix:** Catch specific exceptions (`ServiceUnavailable` for retry, `PermissionDenied` for fail-fast); keep broad `Exception` as last-resort fallback

- [ ] **Paginate `get_all_author_ids()` in cache_layer** _(review §5.2)_
  - `cache_layer/bigquery_client.py:161-178` — no LIMIT/pagination; loads full author table
  - Called by `_rebuild_all()` which loops to enqueue tasks; spikes memory at scale
  - **Fix:** Use BigQuery `page_size` and process in chunks

### P3 — Low (Code Hygiene / Developer Experience)

- [ ] **Clean up stale `scripts/resolve_authors.py`** _(review §1.5)_
  - Docstring references `app/scholar.py` which no longer exists; script operates on legacy Firestore collections (`scholar_raw_pub`, `scholar_raw_author`) from pre-BigQuery architecture
  - Delete if Firestore collections are no longer populated, or update docstring

- [x] ~~**Add `region_health/` to architecture documentation**~~ _(resolved: region_health removed)_

- [ ] **Add type annotations to public API methods** _(review §7.2)_
  - Most functions lack return type annotations
  - Start with: `frontend/routes.py`, `frontend/cache.py`, `cache_layer/bigquery_client.py`

- [ ] **Add linting configuration and pre-commit hooks** _(review §7.3)_
  - No `.pre-commit-config.yaml` or linting config; style enforced by convention only
  - **Fix:** Add `ruff` config and pre-commit hook

- [ ] **Add `.env.example` for local development** _(review §7.1)_
  - All config.py files default to production values; no documentation of required env var overrides

- [x] ~~**Refactor `refresh/bigquery_client.py` to class-based pattern**~~ _(resolved: refresh component removed)_

- [ ] **Add exponential backoff to loading page polling** _(review §5.3)_
  - `frontend/templates/loading.html:29` — hard-coded 10s reload; hammers server on many simultaneous cache misses
  - **Fix:** 10s → 15s → 22s → ... or switch to server-sent events

- [ ] **Investigate and fix upstream dedup in PiP inputs view** _(review §2.5)_
  - `cache_layer/bigquery_client.py:55-56` applies pandas dedup as "guard against upstream view issues"
  - Root cause should be fixed in `stats_author_publication_pip_inputs_current` SQL view

- [ ] **Extract long `/results` route handler into helpers** _(review §2.4)_
  - `frontend/routes.py:154-248` — 94 lines mixing cache reads, plot generation, formatting
  - **Fix:** Extract `_generate_and_cache_plots()` and `_format_author_data()`

- [x] ~~**Update docs to reflect 15-region deployment**~~ _(resolved: CLAUDE.md rewritten for S2 architecture)_

---

## Semantic Scholar Migration

Replace Google Scholar (scraped via `scholarly`) with Semantic Scholar **bulk datasets** (200M papers, 2.4B citations, 75M authors). This eliminates the entire crawling infrastructure, gives a stable data source (no more scraping/CAPTCHAs), and enables better percentile calculations with the full scholarly graph.

**Key findings from API evaluation:**
- S2 bulk datasets provide all needed data: h-index, citation counts, paper year, paper citations
- i10-index not in S2 directly, but trivially computed in BigQuery (`COUNTIF(citationcount >= 10)`)
- `cites_per_year` not available per-paper, but reconstructable by JOINing citations dataset with papers dataset and grouping by citing paper's year — actually better as a relational table than the current JSON blob
- 5-year metrics (`hindex5y`, `citedby5y`, `i10index5y`) dropped
- Datasets released weekly, ~300GB compressed total, license: ODC-BY (open)
- **API key required** for full dataset download (free, via [partner form](https://www.semanticscholar.org/product/api#Partner-Form))
- Estimated BigQuery cost: **~$2-5/month** (storage + queries)

**Architecture change:**
```
CURRENT: User request → Cloud Tasks → scholarly scrape (per author, 15 regions)
           → GCS → BigQuery → Cache → Frontend

NEW:     Weekly cron → Download S2 dataset diffs → GCS → BigQuery (bulk load)
           → Materialization → Cache → Frontend
         On-demand: S2 Author Search API for search only
```

### Phase 0: Prerequisites

- [x] **Apply for Semantic Scholar API key** via [partner form](https://www.semanticscholar.org/product/api#Partner-Form)
  - Key stored in Secret Manager: `projects/875626982900/secrets/s2-api-key`
  - Verified: datasets API works (papers: 60 files, citations: 358 files, authors: 30 files)
  - Verified: Graph API works (author search, paper lookup, citation pagination)
  - Note: Graph API citation endpoint caps at ~2,000 results per paper (misses older citations). Bulk dataset has no such limit — further validates the bulk approach.
- [x] **Validate S2 data coverage** for 10+ known authors
  - Compared 15 authors across Google Scholar and Semantic Scholar (sorted by GS h-index):

  | Author | GS h | S2 h | Δ | GS cites | S2 cites | Δ% | GS pubs | S2 pubs | Δ% | S2 ID |
  |--------|------|------|---|----------|----------|-----|---------|---------|-----|-------|
  | Ronald C Kessler | 348 | 235 | -113 | 599K | 300K | -50% | 2163 | 1113 | -49% | 2350669 |
  | JoAnn Manson | 326 | 263 | -63 | 498K | 267K | -46% | 2999 | 2218 | -26% | 3988124 |
  | Eric Lander | 314 | 290 | -24 | 671K | 537K | -20% | 1101 | 779 | -29% | 9311320 |
  | Frank B. Hu | 312 | 269 | -43 | 488K | 295K | -40% | 2525 | 1906 | -25% | 2242100447 |
  | Bert Vogelstein | 290 | 268 | -22 | 499K | 366K | -27% | 1307 | 859 | -34% | 1965563 |
  | Christopher Murray | 272 | 195 | -77 | 563K | 279K | -50% | 1427 | 627 | -56% | 145882172 |
  | John P.A. Ioannidis | 254 | 195 | -59 | 548K | 203K | -63% | 2783 | 1237 | -56% | 145441750 |
  | Mark Daly | 249 | 203 | -46 | 472K | 312K | -34% | 1317 | 786 | -40% | 144524355 |
  | Yoshua Bengio | 229 | 212 | -17 | 755K | 565K | -25% | 1346 | 812 | -40% | 1751762 |
  | Geoffrey Hinton | 182 | 162 | -20 | 756K | 578K | -24% | 668 | 467 | -30% | 1695689 |
  | Robert Tibshirani | 180 | 160 | -20 | 493K | 296K | -40% | 1069 | 687 | -36% | 1761784 |
  | Daniel Kahneman | 160 | 124 | -36 | 550K | 236K | -57% | 802 | 291 | -64% | 3683465 |
  | Ilya Sutskever | 101 | 75 | -26 | 627K | 512K | -18% | 181 | 164 | -9% | 1701686 |
  | Ross Girshick | 87 | 79 | -8 | 572K | 404K | -29% | 110 | 112 | +2% | 2983898 |
  | Kaiming He | 76 | 67 | -9 | 806K | 544K | -33% | 95 | 84 | -12% | 39353098 |
  | Panos Ipeirotis | 59 | 48 | -11 | 30K | 22K | -28% | 209 | 125 | -40% | 2942126 |

  **Findings:**
  - S2 consistently has **fewer citations (18-63% less)** and **fewer papers (9-64% less)** than Google Scholar
  - h-index is **10-30% lower** on S2 for most authors; more for medical/social science researchers (up to -113 for Kessler)
  - Medical/biomedical researchers show the **largest gap** (Kessler -50%, Murray -50%, Ioannidis -63%, Kahneman -57%) — GS indexes more clinical/grey literature
  - CS researchers show the **smallest gap** (Girshick +2% pubs, Sutskever -9% pubs, He -12% pubs)
  - **Author disambiguation is a real problem**: S2 fragments high-output authors across multiple profiles (Kahneman had 20+ profiles; Ioannidis had 13+). The correct main profile must be identified carefully.
  - Panos Ipeirotis also has a duplicate profile (id `11143475` with 12 papers vs main id `2942126` with 125)
  - **API rate limits are very aggressive**: ~1 req/30-60s effective rate for search endpoints, even with API key. Batch endpoints work better. This further validates the bulk dataset approach.
  - **Yearly citation reconstruction works**: tested on a 4,667-cite paper, 100% of citing papers had year data (though API caps at ~2000 results)
- [ ] **Validate faculty reference population coverage** _(deferred — clean cut to S2 planned instead of hybrid approach)_
  - Check how many of the ~15K faculty can be found in S2 via name/affiliation or ORCID/DBLP crosswalk

### Phase 1: Dataset ingestion pipeline ✓

Built `dataset_ingestion/` component (Cloud Run job):

- [x] **Download S2 datasets to GCS**
  - `s2_api_client.py`: Datasets API client (releases, file URLs, diffs)
  - `downloader.py`: Parallel streaming download from S2 S3 → GCS (4-8 workers)
  - Files stored at `gs://scholar_data_share/s2_datasets/{release_id}/{dataset}/`
- [x] **Load into BigQuery raw tables** (dataset: `s2_data`)
  - `s2_data.papers` (233M rows): corpusid, title, year, citationcount, authors (JSON), externalids (JSON), venue, publicationdate
  - `s2_data.citations` (5.6B rows): citationid, citingcorpusid, citedcorpusid, isinfluential
  - `s2_data.authors` (102M rows): authorid, name, affiliations (JSON), papercount, citationcount, hindex, externalids (JSON)
  - Nested fields use BigQuery JSON type for schema flexibility
- [x] **Create materialized derived tables**
  - `s2_data.paper_citations_by_year` (700M rows): citedcorpusid, citing_year, citation_count, influential_count — replaces `cites_per_year` JSON
  - `s2_data.author_paper_stats` (99.5M rows): authorid, total_publications, i10_index, total_citations, year_of_first_pub
- [x] **Initial data load completed** (release `2026-03-10`)
  - Authors: 30 files, ~2.5 min BQ load
  - Papers: 60 files, ~13 min BQ load
  - Citations: 358 files, ~38 min download + ~12 min BQ load
  - Derived tables: ~44s total materialization
- [x] **Build incremental update pipeline**
  - `diff_updater.py`: Downloads diff files, loads into temp tables, applies DELETE + MERGE
  - Falls back to full reload on diff failure
  - 11 tests for diff updater
- [x] **CI/CD: deploy as Cloud Run Job with weekly Cloud Scheduler**
  - `.github/workflows/deploy-dataset-ingestion.yml`: test → build → deploy Cloud Run Job → create scheduler
  - Weekly schedule: Mondays at 02:00 UTC
  - 4GB memory, 2 CPU, 4-hour timeout

### Phase 2: Adapt BigQuery statistics views

Rewrite the 8-level analytics DAG to query S2 tables instead of `author_latest`/`pub_latest`:

- [x] **Update Level 1 foundation views** (`base_author_publications`, `stats_publication_current`)
  - Source: `s2_data.papers` with authors array flattening for base_author_publications
  - `stats_publication_current` is now per-paper (no `scholar_id`) — author dimension comes from `base_author_publications`
  - `base_author_publications` uses `LAX_STRING(a.authorId)` as `scholar_id`, `CAST(corpusid AS STRING)` as `author_pub_id`
- [x] **Update temporal citation views** (`stats_publication_citations_temporal`)
  - Source: `s2_data.paper_citations_by_year` joined with `s2_data.papers` for pub_year
  - Year series generated via `GENERATE_ARRAY` for cited papers only
  - `scholar_id` removed (temporal citation data is per-paper, not per-author)
- [x] **Update author stats views** (`stats_author_current`)
  - Source: `s2_data.authors` + `s2_data.author_paper_stats`
  - Dropped: `hindex5y`, `citedby5y`, `i10index5y`, `email_domain` (not in S2)
  - `i10index` computed from paper data via `author_paper_stats`
- [x] **Update distribution tables** (`dist_*`)
  - `dist_publication_citations`: reads from `s2_data.papers` directly
  - `dist_author_metrics`: reads from `s2_data.authors` + `s2_data.author_paper_stats`, 5 metrics (was 8)
  - `dist_publication_citations_temporal`, `dist_author_metrics_temporal`, `dist_pip_auc_scores`, `dist_pip_auc_scores_temporal`: read from updated views (no direct changes needed)
- [x] **Update all downstream views** (ranked_*, PiP inputs, PiP scores, temporal)
  - `ranked_publication_current`: no `scholar_id` (per-paper only)
  - `stats_author_publication_pip_inputs_current`: joins `ranked_publication_current` with `base_author_publications` for author dimension
  - `ranked_author_current`: 5 percentile columns (was 8, dropped 5y metrics)
  - Coauthor network views updated to derive from shared papers in S2
  - Updated `materialize_stats.sql` and CI/CD workflows
- [x] **Validate PiP-AUC scores** — S2 views produce mathematically correct scores
  - Validation script: `scripts/validate_s2_pip_scores.py` (10 checks)
  - Re-materialized `dist_publication_citations` (74K → 135K rows, 76 years), `dist_author_metrics` (72K → 853K rows, 126 years 1901-2026, 5 metrics), and `dist_pip_auc_scores` with S2 data
  - **All 16 benchmark authors** produce valid PiP-AUC scores (range 0.984–0.999)
  - **Cross-view consistency**: manual trapezoidal AUC matches view output with 0.000000 difference for all 16 authors
  - **No duplicates** in PiP inputs, **no NULL/out-of-range** percentiles
  - **Publication count alignment**: PiP input counts match `stats_author_current` within 1-2% (small diffs from papers with 0 citations or pre-1950 publication years)
  - **Monotonicity**: higher PiP scores → higher percentiles confirmed in `dist_pip_auc_scores`
  - **Key finding**: PiP scores are very high (0.984–0.999) because the S2 population (99.5M authors) includes many with 1-2 papers. Any serious researcher scores near 1.0. **Phase 3 (benchmark populations) is essential** to restore meaningful differentiation.
  - **S2 data quality issue**: Frank B. Hu and JoAnn Manson have `year_of_first_pub=1912` due to spurious papers in S2 data; this places them in a tiny cohort. Consider adding a minimum year floor (e.g., 1950) or outlier detection in `author_paper_stats`.

### Phase 3: Multiple benchmark populations

- [x] **Add benchmark populations to distribution tables**
  - Added `benchmark` column to all 4 author-level dist tables:
    - `dist_author_metrics` — `all_authors` (full ~99.5M) + `active_authors` (hindex≥3, total_publications≥3)
    - `dist_pip_auc_scores` — same two benchmarks
    - `dist_author_metrics_temporal` — same, using current author state for membership
    - `dist_pip_auc_scores_temporal` — same
  - Updated all ranked views to filter by `benchmark = 'active_authors'`:
    - `ranked_author_current`, `ranked_author_pip_scores_current`
    - `ranked_author_metrics_temporal`, `ranked_author_pip_scores_temporal`
    - `stats_author_publication_pip_inputs_current` (PiP X-axis interpolation)
  - Updated `materialize_stats.sql` with benchmark-aware dist table generation
  - Publication citation dist tables (`dist_publication_citations`, `dist_publication_citations_temporal`) unchanged — per-paper, not per-author
  - CI/CD workflows unchanged — they reference SQL files by path
- [ ] **Create `benchmark_faculty` table**
  - Curated table of known faculty S2 author IDs with institution/department/group
- [ ] **Migrate faculty reference list**
  - Map ~15K faculty Google Scholar IDs → S2 author IDs via name + affiliation or ORCID/DBLP crosswalk

### Phase 4: Adapt author search + frontend

- [x] **Replace author search fallback** (`author_search/scholar_client.py` → `s2_client.py`)
  - Swapped `scholarly` library → S2 Graph API Author Search (`GET /graph/v1/author/search?query={name}`)
  - New `s2_client.py`: authenticated via S2 API key (env var or Secret Manager), returns normalized author dicts
  - Updated `search_service.py` to import `s2_client` instead of `scholar_client`
  - Removed `scholarly` + `httpx` dependencies; added `requests` + `google-cloud-secret-manager`
  - Updated CI/CD workflow (`deploy-author-search.yml`) to deploy `s2_client.py`
  - 11 new tests for S2 client, 4 updated search service tests (35 passing)
- [x] **Update frontend for S2 data**
  - Updated `SCHOLAR_ID_RE` to accept S2 numeric author IDs (1-20 chars)
  - Removed Google Scholar crawler enqueue — all data comes from S2 bulk datasets
  - `api_fetch_authors` now triggers cache population instead of crawling
  - Removed `id_type=s2` parameter handling (all authors are S2)
  - Updated flash messages and validation text
  - Default speed check author changed to S2 ID (`2942126`)
- [x] **Update cache layer for S2 data**
  - `get_author_pub_stats`: queries S2 `papers` table for title/venue/citations instead of GS `pub_latest`
  - `get_author_freshness`: single query to `stats_author_current` (removed GS raw table check)
  - `get_all_author_ids`: queries S2 `author_paper_stats` (active authors only)
  - Removed `author_has_raw_pubs`, `author_pubs_freshly_materialized`, `refresh_author_pubs` (GS-specific)
  - Simplified `_populate_author_profile` in `cache_service.py` (removed pub refresh logic)
- [x] **Remove Google Scholar-specific references**
  - `results.html`: removed "Google Scholar ID" label, "Last Fetched from Google Scholar", Refresh button, 5y metrics
  - `publications.html`: "Google Scholar ID" → "Author ID"
  - `redirect.html`: removed all "fetched from Google Scholar" text, simplified to S2 messaging
  - `help.html`: updated data source to Semantic Scholar dataset, updated refresh schedule
  - `health.html`: "Scholar ID" → "Author ID"
  - `loading.html`/`index.html`: removed `id_type` parameter passing
- [ ] **Add benchmark selector to profile pages**
  - Let users choose which reference population to view percentiles against

### Phase 5: Remove crawler infrastructure ✓

- [x] **Remove `crawler/` component** (Cloud Functions, scholarly, ScraperAPI, proxy rotation)
  - Deleted entire `crawler/` directory (8 source files + 53 tests)
  - Deleted `.github/workflows/deploy-crawler.yml`
- [x] **Remove Cloud Tasks queues** (process-authors, process-pubs, process-pub-priority)
  - Removed queue creation from `deploy-infrastructure.yml`
  - Added cleanup step to pause legacy queues on next deploy
- [x] **Remove `refresh/` component** (replaced by weekly bulk dataset refresh)
  - Deleted entire `refresh/` directory (5 source files + 76 tests)
  - Deleted `.github/workflows/deploy-refresh.yml`
  - Removed refresh scheduler jobs (v3-refresh-stale-authors, v3-refresh-error-authors, v3-expand-coauthors)
  - Added cleanup step to delete legacy scheduler jobs on next deploy
- [x] **Remove region rotation / health scoring** (no longer scraping)
  - Deleted entire `region_health/` directory (4 source files + 3 test files)
  - Removed region health scorer background thread from `frontend/app.py`
  - Removed `frontend/health_service.py` (depended on region_health + GS-specific BQ queries)
- [x] **Consolidate CI/CD** (remove multi-region function deployment)
  - Removed crawler + refresh deployment workflows
  - Simplified `deploy-infrastructure.yml` to only manage cache queues
- [x] **Update CLAUDE.md** architecture docs
  - Rewrote to describe S2-only architecture, data flow, and infrastructure
- [x] **Clean up frontend references**
  - Removed refresh API endpoints (`/api/refresh_stale_authors`, `/api/add_coauthors`)
  - Removed `_call_refresh_function` helper
  - Removed crawler config (CRAWL_FUNCTION_URL, REFRESH_FUNCTIONS_BASE, etc.)
  - Removed `enqueue_author_crawl` from queue_client
  - Removed region_health imports from config.py
  - Updated Dockerfile to not copy crawler/refresh/region_health
- [x] **Replace author search S2 API with local BigQuery search**
  - Removed `s2_client.py` (S2 Graph API dependency)
  - Added `search_s2_universe()` to BigQuery client — searches full `s2_data.authors` table (102M)
  - Updated search service: "search beyond" now queries local BQ instead of external API
  - Removed `requests` and `google-cloud-secret-manager` dependencies from author_search
- [x] **Add cache purge endpoint** for legacy Google Scholar entries
  - Added `POST /admin/purge_legacy` endpoint to cache_layer
  - Scans all author-keyed cache collections, deletes entries with non-numeric (GS) IDs

### Open questions

- **Coverage gap**: S2 has 200M papers but covers 18-63% fewer citations than Google Scholar. Known and accepted.
- **Author disambiguation**: S2 may split/merge authors differently than Google Scholar. Main profile must be identified carefully.
- **URL backwards compatibility**: Legacy Google Scholar ID URLs will return "not found" since we only accept S2 numeric IDs now.
- **S2 data quality**: Some authors have spurious early publication years (e.g., 1912). Consider minimum year floor.

---

## Materialize Full BigQuery DAG

Replace all live views with pre-computed tables, refreshed monthly after S2 ingestion. Data is static between bulk loads — live views waste compute on every query.

### Motivation

**Current state:** Three separate schedules:
- Weekly (Mon 02:00): S2 ingestion → raw + derived tables
- Quarterly (Jan/Apr/Jul/Oct): 6 distribution tables (`dist_*`)
- Daily (06:00): 4 snapshot tables (`ranked_*_table`)

Per-author cache-miss queries hit live views that chain up to 8 levels deep. Each query re-computes joins and lookups that produce the same result until the next load.

**Target state:** One monthly pipeline materializes the entire DAG after ingestion. All app queries hit pre-computed clustered tables. No live views in the query path.

### Ingestion Cadence: Monthly vs Weekly

S2 releases weekly diffs, but we don't need weekly freshness. Citation percentiles shift slowly — most authors' metrics barely change week-to-week. Monthly ingestion + materialization is sufficient and reduces costs.

| Cadence | Ingestion Cost | Materialization Cost | Data Freshness | Recommendation |
|---------|---------------|---------------------|----------------|----------------|
| Weekly | ~$5–15/run, $20–60/mo | ~$10–30/run, $40–120/mo | ≤7 days stale | Overkill for most use cases |
| Monthly | ~$5–15/run, $5–15/mo | ~$10–30/run, $10–30/mo | ≤30 days stale | **Recommended** — good tradeoff |
| Quarterly | Cheapest | Cheapest | ≤90 days stale | Too stale for newly published papers |

**Done:** Changed Cloud Scheduler from `0 2 * * 1` (weekly Monday) to `0 2 1 * *` (1st of each month). Scheduler renamed from `s2-weekly-ingestion` to `s2-monthly-ingestion`.

### Complete DAG — Materialization Order

Each level depends on the previous. Steps within a level can run in parallel.

#### Step 1: Level 1 — Foundation (parallel, independent)

| Table | Clustering | Est. Size | Source |
|-------|-----------|-----------|--------|
| `base_author_publications_table` | `scholar_id` | ~3 GB | `s2_data.author_paper_bridge` |
| `stats_publication_current_table` | `author_pub_id, pub_year` | ~8 GB | `s2_data.papers` |
| `stats_author_current_table` | `scholar_id, year_of_first_pub` | ~4 GB | `s2_data.authors` + `author_paper_stats` |
| `dist_publication_citations` | `pub_year` | ~5 MB | `s2_data.papers` (APPROX_QUANTILES) |
| `dist_author_metrics` | `benchmark, year_of_first_pub, metric_name` | ~30 MB | `s2_data.authors` + `author_paper_stats` (APPROX_QUANTILES) |

#### Step 2: Level 2 — Temporal foundation + first ranked (depends on Step 1)

| Table | Clustering | Partitioning | Est. Size | Source |
|-------|-----------|-------------|-----------|--------|
| `stats_publication_citations_temporal_table` | `author_pub_id, pub_year, citation_year` | — | ~50–100 GB ⚠️ | `GENERATE_ARRAY` cross join, ~30 rows/paper × 100M papers |
| `ranked_publication_current_table` | `author_pub_id, pub_year` | — | ~8 GB | `stats_publication_current` + `dist_publication_citations` |
| `intermediate_author_publication_state_temporal_table` | `scholar_id, author_pub_id, state_year` | INT64 range on `state_year` | ~100–200 GB ⚠️ | Join of bridge × temporal citations |
| `dist_publication_citations_temporal` | `metric_name, pub_year` | — | ~2–5 GB | `stats_publication_citations_temporal` (APPROX_QUANTILES) |

#### Step 3: Level 3 — Metrics + PiP inputs + ranked (depends on Steps 1–2)

| Table | Clustering | Partitioning | Est. Size | Source |
|-------|-----------|-------------|-----------|--------|
| `stats_author_metrics_temporal_table` | `scholar_id, state_year` | INT64 range on `state_year` | ~10–30 GB | `intermediate_author_publication_state_temporal` aggregation |
| `stats_author_publication_pip_inputs_current_table` | `scholar_id` | — | ~3–5 GB | One row per author×pub with percentiles |
| `ranked_author_current_table` | `scholar_id, year_of_first_pub` | — | ~5 GB | `stats_author_current` + `dist_author_metrics` |
| `ranked_publication_citations_temporal_table` | `author_pub_id, pub_year, citation_year` | — | ~60–120 GB ⚠️ | `stats_publication_citations_temporal` + 4 percentile cols |

#### Step 4: Level 4 — PiP scores + distributions (depends on Steps 1–3)

| Table | Clustering | Est. Size | Source |
|-------|-----------|-----------|--------|
| `stats_author_pip_scores_current_table` | `scholar_id, year_of_first_pub` | ~1 GB | Trapezoidal AUC from PiP inputs |
| `dist_pip_auc_scores` | `benchmark, year_of_first_pub` | ~10 MB | APPROX_QUANTILES from `ranked_author_pip_scores_current_table` |
| `dist_author_metrics_temporal` | `benchmark, year_of_first_pub, state_year, metric_name` | ~50–200 MB | APPROX_QUANTILES from `stats_author_metrics_temporal_table` |

#### Step 5: Level 5 — Ranked PiP + temporal ranked (depends on Steps 1–4)

| Table | Clustering | Partitioning | Est. Size | Source |
|-------|-----------|-------------|-----------|--------|
| `ranked_author_pip_scores_current_table` | `scholar_id` | — | ~1 GB | `stats_author_pip_scores_current` + `dist_pip_auc_scores` |
| `ranked_author_metrics_temporal_table` | `scholar_id, state_year` | — | ~10–30 GB | `stats_author_metrics_temporal` + `dist_author_metrics_temporal` |
| `stats_author_pip_scores_temporal_table` | `scholar_id, state_year` | INT64 on `state_year` | ~5–15 GB | Most expensive computation in system |

#### Step 6: Level 6 — Temporal PiP distribution (depends on Step 5)

| Table | Clustering | Est. Size | Source |
|-------|-----------|-----------|--------|
| `dist_pip_auc_scores_temporal` | `benchmark, year_of_first_pub, state_year` | ~50–200 MB | APPROX_QUANTILES from `ranked_author_pip_scores_temporal_table` |

#### Step 7: Level 7 — Temporal PiP ranked (depends on Steps 5–6)

| Table | Clustering | Est. Size | Source |
|-------|-----------|-----------|--------|
| `ranked_author_pip_scores_temporal_table` | `scholar_id, state_year` | ~5–15 GB | `stats_author_pip_scores_temporal` + `dist_pip_auc_scores_temporal` |

### Benchmark Strategy

**Decision:** Materialize ranked tables with `active_authors` benchmark only (the production default). Keep both benchmarks in the 6 small dist tables (~300 MB total).

Rationale:
- Doubling the large temporal tables (50–200 GB) for `all_authors` is wasteful — no user currently needs it
- Adding `benchmark_faculty` later would triple storage
- Users wanting a different benchmark can query the view (fallback path) or we add benchmark-specific tables on demand

**Future:** When `benchmark_faculty` is added, create a small `dist_author_metrics_faculty` table and a `ranked_author_current_faculty_table`. The dist table is tiny; the ranked table is one snapshot. No need to duplicate all temporal tables.

### Unified Monthly Pipeline

Replace three schedules (weekly ingestion + quarterly distributions + daily snapshots) with one monthly pipeline:

**`.github/workflows/bigquery-materialize-all.yml`**
- **Schedule:** 1st of each month, 08:00 UTC (6h after ingestion starts at 02:00)
- **Preflight:** Query `s2_data.release_log` to verify latest ingestion succeeded
- **Abort** if no successful ingestion since last month
- **Steps:** Run Steps 1–7 above in sequence (parallel within each step)
- **Estimated runtime:** 3–5 hours total (temporal tables dominate)

**Trigger options:**
- (A) Cron schedule at 08:00 UTC on the 1st — simple, add preflight check ← recommended
- (B) `repository_dispatch` from ingestion Cloud Run Job — tighter coupling
- (C) `workflow_run` trigger after `deploy-dataset-ingestion.yml` — GitHub-native chaining

**GitHub Actions timeout:** 6h per job. If the pipeline exceeds this, split into two chained workflows (Steps 1–4 and Steps 5–7) using `workflow_run` triggers.

### App Changes

#### `cache_layer/bigquery_client.py` — switch view → table references

| Method | Current (view) | New (table) |
|--------|---------------|-------------|
| `get_author_pub_stats()` | `stats_author_publication_pip_inputs_current` | `stats_author_publication_pip_inputs_current_table` |
| `get_author_stats()` | `ranked_author_current`, `ranked_author_pip_scores_current` | `ranked_author_current_table`, `ranked_author_pip_scores_current_table` |
| `get_publication_stats()` | `ranked_publication_citations_temporal` | `ranked_publication_citations_temporal_table` |
| `get_author_temporal_stats()` | `ranked_author_metrics_temporal` | `ranked_author_metrics_temporal_table` |
| `get_author_freshness()` | `stats_author_current` | `stats_author_current_table` |

**Migration:** Add `USE_MATERIALIZED_TABLES` config flag. Deploy with `False`, materialize all tables, verify, flip to `True`.

#### CI/CD — delete old, create new

- **Delete:** `bigquery-materialize.yml` (daily snapshots)
- **Delete:** `bigquery-materialize-distributions.yml` (quarterly distributions)
- **Create:** `bigquery-materialize-all.yml` (unified weekly pipeline)
- **Keep:** `bigquery-views.yml` (views still useful for development/debugging)

### Storage and Cost Estimates

| Category | Estimated Size | Monthly Storage ($0.02/GB) |
|----------|---------------|---------------------------|
| Foundation tables (Step 1) | ~15 GB | $0.30 |
| Temporal citation tables (Steps 2–3) | ~150–300 GB ⚠️ | $3.00–6.00 |
| Author metric tables (Steps 3–5) | ~30–80 GB | $0.60–1.60 |
| PiP tables (Steps 4–7) | ~10–30 GB | $0.20–0.60 |
| Distribution tables (Steps 1–6) | ~8 GB | $0.16 |
| **Total** | **~215–435 GB** | **$4.30–8.70/mo** |

**Compute cost for monthly materialization:** On-demand BigQuery at $6.25/TB scanned → ~$10–30 per monthly run. Combined with monthly ingestion (~$5–15), total compute: **~$15–45/month**. Much cheaper than the previous weekly+quarterly+daily regime.

### Caveats to Investigate

1. **⚠️ `stats_publication_citations_temporal` size**: `GENERATE_ARRAY(pub_year, current_year)` creates ~30 rows per paper. With 100M+ cited papers → 3B+ rows, 50–100 GB. **Action:** Run `SELECT COUNT(*) FROM statistics.stats_publication_citations_temporal` to get actual count before committing. Consider materializing only papers with >0 citations and capping year range.

2. **⚠️ `intermediate_author_publication_state_temporal` size**: Joins 600M author-paper rows with temporal citation data. Could be 5–10B rows, 100–200 GB. **Action:** Estimate actual size. Consider whether this intermediate table is needed, or if `stats_author_metrics_temporal` can be computed directly from the source tables without materializing the intermediate.

3. **⚠️ `ranked_publication_citations_temporal` duplicates data**: Same rows as `stats_publication_citations_temporal` plus 4 percentile columns. **Optimization:** Merge into a single table that includes both raw metrics and percentiles, avoiding double storage. The SQL would inline the percentile lookups during materialization.

4. **GitHub Actions 6h timeout**: Steps 2–3 (temporal tables) are the bottleneck. If individual steps exceed 6h, move to BigQuery scheduled queries or Cloud Run Jobs instead of GitHub Actions steps.

5. **Atomic table replacement**: `CREATE OR REPLACE TABLE` briefly drops the old table. During this window, queries fail. **Options:**
   - (A) Write to `*_table_staging`, then rename (BigQuery supports `ALTER TABLE RENAME`) ← safest
   - (B) Rely on Firestore cache to absorb the brief unavailability ← simpler
   - **Action:** Test if `ALTER TABLE ... RENAME TO` works across same dataset. If not, use option B.

6. **Coauthor network tables**: `coauthor_network` view does a self-join of all paper authors → potentially billions of rows. `coauthors_to_add` is diagnostic. Neither is queried by the app. **Decision:** Do NOT materialize. Keep as views unless a feature needs them.

7. **Dist tables reading from pre-computed tables**: Currently `dist_pip_auc_scores` and `dist_pip_auc_scores_temporal` read from the daily-snapshot `ranked_*_table`. In the new pipeline, these snapshots are computed in Step 5, and the dist tables in Steps 4/6. **Circular dependency?** No — `dist_pip_auc_scores` reads from the Step 5 output of the *previous* week's run. The first full run needs the existing tables to bootstrap. Document this bootstrap requirement.

8. **View-to-table references during materialization**: The materialization pipeline runs `CREATE TABLE AS SELECT * FROM <view>`. The views reference other views, which cascade through the chain. For Steps 2+, if a view references a table that was just replaced in Step 1, it reads the new data — correct. But views referencing other views still do live computation during materialization. **Optimization:** Modify downstream view SQL to reference `*_table` names instead of view names, so materialization reads from tables (faster). **Tradeoff:** More SQL changes but faster pipeline.

9. **Incremental materialization**: Currently, every table is fully replaced weekly. For tables that are 100+ GB, this is expensive. BigQuery `MERGE` statements could incrementally update only rows affected by the weekly diff (new/changed papers and authors). **Action:** Investigate after the full materialization works. The weekly diff from `dataset_ingestion/diff_updater.py` identifies changed `corpusid`s and `authorid`s — these could drive incremental updates.

10. **Monitoring and staleness alerts**: Add a final pipeline step that writes success/failure to `s2_data.release_log` or a new `materialization_log` table. Add a Cloud Monitoring alert if materialized tables are >8 days old.

### Implementation Phases

**Phase 1: Foundation** ✓
- [x] Created `dataset_ingestion/materialize_tables.py` with all 7 DAG levels
  - Reads existing SQL view files from `bigquery/statistics/`
  - Converts `CREATE VIEW` to `CREATE TABLE` with clustering
  - Executes in strict topological order (Levels 1-7)
  - 21 tables total: 6 dist tables + 15 stats/ranked tables
- [x] Integrated into ingestion pipeline (`main.py`) — runs after derived tables
- [x] 21 new tests in `dataset_ingestion/tests/test_materialize_tables.py`

**Phase 2: App Migration** ✓
- [x] Added `USE_MATERIALIZED_TABLES` config flag to `cache_layer/config.py` (default: `true`)
  - `bq_view()` auto-maps view names to `_table` names when flag is `True`
  - Mapping covers all views referenced by `bigquery_client.py`
- [x] No changes needed to `bigquery_client.py` — the config layer handles the mapping

**Phase 3: CI/CD Consolidation** ✓
- [x] Changed ingestion Cloud Scheduler from weekly (`0 2 * * 1`) to monthly (`0 2 1 * *`)
  - Renamed scheduler to `s2-monthly-ingestion`
  - Added cleanup step to delete legacy `s2-weekly-ingestion`
  - Increased Cloud Run Job timeout from 4h to 8h for materialization
- [x] Created `.github/workflows/bigquery-materialize-all.yml` (safety net workflow)
  - Runs monthly at 08:00 UTC as fallback
  - Preflight check verifies recent successful ingestion
  - Can be triggered manually via workflow_dispatch
- [x] Deleted `bigquery-materialize.yml` (daily snapshots) and `bigquery-materialize-distributions.yml` (quarterly distributions)
- [x] Updated CLAUDE.md with new schedule and architecture

**Phase 4: Optimization (ongoing)**
- [ ] Merge redundant temporal tables (stats + ranked into single table)
- [ ] Add INT64 range partitioning to large temporal tables
- [ ] Investigate incremental materialization via `MERGE`
- [ ] Monitor costs and adjust clustering
- [ ] Run first full materialization and verify row counts/storage

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


---

_Last updated: 2026-03-30_
