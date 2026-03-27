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

- [ ] **Fix `cache_writer.py` batch failure accounting** _(review §2.1)_
  - `write_batch()` returns count of items *attempted*, not *committed*
  - If `batch.commit()` fails, count is not decremented — caller sees inflated write count
  - **Files:** `cache_layer/cache_writer.py:60-74`
  - **Fix:** Track `committed` separately; on commit failure, retry or subtract from count
  - **Tests:** Add batch commit failure tests (`cache_layer/tests/test_cache_writer.py`)

- [ ] **Prevent duplicate BigQuery loads from archive failures** _(review §3.2)_
  - If GCS archive (copy+delete) fails, source file stays in input prefix
  - Next batch_load re-processes it → WRITE_APPEND creates duplicates in raw tables
  - Dedup views mask this at query time, but raw tables grow unboundedly
  - **Files:** `ingestion/batch_load.py:160-175`
  - **Fix:** Use a "processed" metadata marker or tracking collection instead of relying on file move

- [ ] **Add authentication to cache_layer admin endpoints** _(review §3.1)_
  - `/admin/rebuild` and `/admin/populate` accept unauthenticated POST requests
  - Anyone who can reach the Cloud Run service can trigger a full cache rebuild
  - **Files:** `cache_layer/main.py:55-80`
  - **Fix:** Require IAM-authenticated requests (Cloud Run `--ingress internal` or verify OIDC tokens)

- [ ] **Unify document ID normalization** _(review §1.2)_
  - Three different approaches strip `.json` suffix from legacy document IDs:
    - `ingestion/batch_load.py` — `removesuffix(".json")`
    - `ingestion/cache_enqueuer.py:52` — `re.sub(r"\.json$", "", doc_id)`
    - `bigquery/materialize_dedup_tables.sql:36-42` — `CASE WHEN ENDS_WITH(...)`
  - Divergence causes silent data mismatches between BigQuery and Firestore
  - **Fix:** Create a single `normalize_document_id()` utility; use it everywhere in Python; keep SQL version aligned

### P1 — High (Production Reliability)

- [ ] **Add retry logic for transient BigQuery failures in refresh** _(review §3.3)_
  - `refresh/bigquery_client.py:27-34` — `_query()` has no retry; a single 503 aborts the entire refresh cycle
  - **Fix:** Wrap with `google.api_core.retry.Retry` or implement exponential backoff

- [ ] **Add timeout to Scholar search in author_search** _(review §3.6)_
  - `author_search/scholar_client.py` — `scholarly.search_author()` has no timeout wrapper
  - Crawler uses `ThreadPoolExecutor` with 300s timeout; author_search does not
  - **Fix:** Add `concurrent.futures.ThreadPoolExecutor` timeout wrapper matching the crawler pattern

- [ ] **Wrap `author_exists()` call in try/except** _(review §3.4)_
  - `refresh/refresh_service.py:79` — unprotected BigQuery call outside try/except
  - If it raises, `/api/fetch_author` returns raw 500 with no structured error
  - **Fix:** Wrap the existence check in the same try/except block as the enqueue call

- [ ] **Move plot generation out of the request thread** _(review §5.1)_
  - `frontend/routes.py:129-152` — synchronous matplotlib rendering blocks Flask worker
  - For authors with 500+ publications, can take several seconds
  - **Fix:** Pre-generate plots in cache_layer during cache population, or generate async

### P2 — Medium (Maintainability / Performance / Security)

- [ ] **Extract shared utilities to reduce cross-component duplication** _(review §1.1)_
  - `bq_view()`/`bq_raw()` duplicated in 4 config files
  - `_get_client()` + `_sanitize_task_id()` duplicated in crawler + refresh task enqueuers
  - `_trigger_batch_load()` duplicated in `fetch_author.py` + `fetch_publication.py`
  - **Fix:** Create `shared/` package or extend `region_health/` with `bq_helpers.py` and `task_client.py`

- [ ] **Standardize return types in `cache_layer/bigquery_client.py`** _(review §2.3)_
  - `get_author_stats()` returns `None` on empty; `get_publication_stats()` returns `[]`
  - Callers must handle both, inviting `TypeError`
  - **Fix:** Return `[]` for list queries, `None` for single-document queries — consistently

- [ ] **Add rate limiting to frontend API endpoints** _(review §4.4)_
  - `/api/refresh_stale_authors`, `/api/add_coauthors`, `/api/rebuild_statistics` have no throttling
  - Each call triggers expensive BigQuery queries and Cloud Tasks enqueue
  - **Fix:** Add Flask-Limiter or deduplication checks

- [ ] **Cache health dashboard BigQuery queries** _(review §5.4)_
  - `frontend/health_service.py` runs 10+ BigQuery queries per dashboard refresh with no caching
  - **Fix:** Cache results in Firestore with a 5-minute TTL

- [ ] **URL-encode parameters in refresh function calls** _(review §4.1)_
  - `frontend/routes.py:76-78` — query string built without encoding
  - **Fix:** Use `urllib.parse.urlencode(params)`

- [ ] **Single-source the region list** _(review §1.4)_
  - CI/CD workflows hardcode 15 regions; `scripts/backfill_authors.py` hardcodes 9; `region_health/config.py` defines 15
  - **Fix:** CI/CD should read from `region_health/config.py` or a shared config file

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

- [ ] **Add `region_health/` to architecture documentation** _(review §1.3)_
  - Module is imported by crawler, frontend, and refresh but absent from CLAUDE.md and docs/ARCHITECTURE.md

- [ ] **Add type annotations to public API methods** _(review §7.2)_
  - Most functions lack return type annotations
  - Start with: `frontend/routes.py`, `frontend/cache.py`, `cache_layer/bigquery_client.py`

- [ ] **Add linting configuration and pre-commit hooks** _(review §7.3)_
  - No `.pre-commit-config.yaml` or linting config; style enforced by convention only
  - **Fix:** Add `ruff` config and pre-commit hook

- [ ] **Add `.env.example` for local development** _(review §7.1)_
  - All config.py files default to production values; no documentation of required env var overrides

- [ ] **Refactor `refresh/bigquery_client.py` to class-based pattern** _(review §2.2)_
  - Uses module-level functions + global `_client`; inconsistent with other BQ clients and harder to test

- [ ] **Add exponential backoff to loading page polling** _(review §5.3)_
  - `frontend/templates/loading.html:29` — hard-coded 10s reload; hammers server on many simultaneous cache misses
  - **Fix:** 10s → 15s → 22s → ... or switch to server-sent events

- [ ] **Investigate and fix upstream dedup in PiP inputs view** _(review §2.5)_
  - `cache_layer/bigquery_client.py:55-56` applies pandas dedup as "guard against upstream view issues"
  - Root cause should be fixed in `stats_author_publication_pip_inputs_current` SQL view

- [ ] **Extract long `/results` route handler into helpers** _(review §2.4)_
  - `frontend/routes.py:154-248` — 94 lines mixing cache reads, plot generation, formatting
  - **Fix:** Extract `_generate_and_cache_plots()` and `_format_author_data()`

- [ ] **Update docs to reflect 15-region deployment** _(review §7.4)_
  - TASKS.md and CLAUDE.md still reference "9 regions"

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
- [ ] **Validate faculty reference population coverage**
  - Check how many of the ~15K faculty can be found in S2 via name/affiliation or ORCID/DBLP crosswalk

### Phase 1: Dataset ingestion pipeline

Build `dataset_ingestion/` component (Cloud Run job):

- [ ] **Download S2 datasets to GCS**
  - Call Datasets API (`/datasets/v1/release/`) to find latest release
  - Download papers, citations (stripped of contexts/intents), authors → `gs://scholar_data_share/s2_datasets/{release_id}/`
- [ ] **Load into BigQuery raw tables**
  - `s2_papers` (200M rows): corpusid, title, year, citationcount, authors, externalids, venue, publicationdate
  - `s2_citations` (2.4B rows): citationid, citingcorpusid, citedcorpusid, isinfluential
  - `s2_authors` (75M rows): authorid, name, affiliations, papercount, citationcount, hindex, externalids
- [ ] **Build incremental update pipeline**
  - Use `/diffs/{start}/{end}/{dataset}` endpoint for weekly updates instead of full re-download
- [ ] **Create materialized derived tables**
  - `s2_paper_citations_by_year`: JOIN citations with papers, GROUP BY (citedcorpusid, citing_paper_year) — replaces `cites_per_year` JSON
  - `s2_author_paper_stats`: i10-index, total_publications, total_publications_with_citations per author

### Phase 2: Adapt BigQuery statistics views

Rewrite the 8-level analytics DAG to query S2 tables instead of `author_latest`/`pub_latest`:

- [ ] **Update Level 1 foundation views** (`base_author_publications`, `stats_publication_current`)
  - Source: `s2_papers` (has embedded authors array — no separate author-pub join needed)
- [ ] **Update temporal citation views** (`stats_publication_citations_temporal`)
  - Source: `s2_paper_citations_by_year` table instead of JSON-parsed `cites_per_year`
- [ ] **Update author stats views** (`stats_author_current`)
  - Source: `s2_authors` + `s2_author_paper_stats`
- [ ] **Update distribution tables** (`dist_*`)
  - Same PERCENT_RANK logic, now over full S2 population (200M papers, 75M authors)
- [ ] **Validate PiP-AUC scores** match between old and new views for test authors

### Phase 3: Multiple benchmark populations

- [ ] **Create benchmark population views/tables**
  - `benchmark_all_authors`: all 75M authors (unfiltered)
  - `benchmark_active_authors`: authors with hindex >= 3, papercount >= 3
  - `benchmark_faculty`: curated table of known faculty S2 author IDs with institution/department/group
- [ ] **Compute distribution tables per benchmark**
  - `dist_author_metrics_by_benchmark`: PERCENT_RANK partitioned by (benchmark_name, year_of_first_pub)
- [ ] **Migrate faculty reference list**
  - Map ~15K faculty Google Scholar IDs → S2 author IDs via name + affiliation or ORCID/DBLP crosswalk

### Phase 4: Adapt author search + frontend

- [ ] **Replace author search fallback** (`author_search/scholar_client.py`)
  - Swap scholarly fallback → S2 Author Search API (`GET /author/search?query={name}`)
  - BigQuery-first search now covers 75M authors
- [ ] **Update frontend for S2 author IDs**
  - URL parameter: `scholar_id` → `author_id`
  - Add redirect/lookup for old Google Scholar ID URLs
- [ ] **Add benchmark selector to profile pages**
  - Let users choose which reference population to view percentiles against
- [ ] **Remove Google Scholar-specific references** (links, email_domain field)

### Phase 5: Remove crawler infrastructure

- [ ] **Remove `crawler/` component** (Cloud Functions, scholarly, ScraperAPI, proxy rotation)
- [ ] **Remove Cloud Tasks queues** (process-authors, process-pubs, process-pub-priority)
- [ ] **Remove `refresh/` component** (replaced by weekly bulk dataset refresh)
- [ ] **Remove region rotation / health scoring** (no longer scraping)
- [ ] **Consolidate CI/CD** (remove multi-region function deployment)
- [ ] **Update CLAUDE.md** architecture docs

### Open questions

- **Coverage gap**: S2 has 200M papers but may miss some Google Scholar entries. Validate overlap for key authors.
- **Author disambiguation**: S2 may split/merge authors differently than Google Scholar.
- **URL backwards compatibility**: Strategy for existing bookmarked Google Scholar ID URLs.
- **Schema mapping**: `scholar_id` → `authorid`, `author_pub_id` → `corpusid` (integer vs string).
- **email_domain**: Not available in S2; drop or find alternative source.

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

_Last updated: 2026-03-27_
