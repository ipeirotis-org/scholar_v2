# System Architecture: Scholar Analytics

## Overview

Scholar Analytics is a system for analyzing Google Scholar data using percentile-based, age-aware research metrics (PiP-AUC). It consists of seven components with strict boundaries around what each reads and writes.

```
                         Google Scholar
                          |         |
                   [1. CRAWLER]  [6. AUTHOR SEARCH]
                          |         |
                   GCS (raw JSON)  BigQuery + Scholar (fallback)
                          |         |
                  [2. INGESTION]   |
                          |        |
                   BigQuery (raw)  |
                          |        |
                    [3. ANALYTICS]  |
                          |        |
                   BigQuery (views + materialized tables)
                         /|\
                        / | \
                       /  |  \
     [7. CACHE LAYER]    |   [5. REFRESH & EXPAND]
      (BQ → Firestore)   |     (orchestration)
            |             |           |
      Firestore cache     |     Cloud Tasks → back to Crawler
            |             |
     [4. FRONTEND]        |
     (Firestore-only)     |
            |             |
            +--- calls ---+
```

---

## Components

| # | Component | Purpose | Input | Output | Details |
|---|---|---|---|---|---|
| 1 | **Crawler** | Fetch author/pub data from Google Scholar | Scholar ID (via Cloud Tasks) | Raw JSON files in GCS | [architecture-crawler.md](architecture-crawler.md) |
| 2 | **Ingestion** | Load raw JSON into BigQuery | GCS JSON files (`authors_json/`, `publications_json/`) | BigQuery rows (`scholar_raw_data.author`, `.pub`) | [architecture-ingestion.md](architecture-ingestion.md) |
| 3 | **Analytics** | Compute metrics, percentiles, and PiP-AUC scores | BigQuery raw tables (via `_latest` views) | BigQuery views + materialized tables | [architecture-analytics.md](architecture-analytics.md) |
| 4 | **Frontend** | Display precomputed analytics (read-only from Firestore) | User queries, Firestore cache | HTML pages with embedded charts | [architecture-frontend.md](architecture-frontend.md) |
| 5 | **Refresh & Expand** | Orchestrate data freshness and database growth | Schedules, staleness analysis, coauthor graph | Cloud Tasks for Crawler | [architecture-refresh.md](architecture-refresh.md) |
| 6 | **Author Search** | Find authors by name (local-first, Scholar fallback) | Author name query string | Matching author list | [architecture-author-search.md](architecture-author-search.md) |
| 7 | **Cache Layer** | Populate Firestore cache from BigQuery | Cloud Tasks (priority + batch queues) | Firestore cache documents | [architecture-cache-layer.md](architecture-cache-layer.md) |

---

## Data Stores Summary

| Store | Role | Written by | Read by |
|---|---|---|---|
| **GCS `authors_json/`** | Raw author JSON (staging) | Crawler | Ingestion |
| **GCS `publications_json/`** | Raw publication JSON (staging) | Crawler | Ingestion |
| **GCS `authors_archive/`** | Archived author JSON | Ingestion | (debugging only) |
| **GCS `publications_archive/`** | Archived publication JSON | Ingestion | (debugging only) |
| **GCS `dead_letter/`** | Failed files | Ingestion | (debugging only) |
| **GCS `all_authors_stats.csv`** | Author rankings export | Frontend | Frontend (signed URL download) |
| **BigQuery `scholar_raw_data.author`** | Raw author records | Ingestion | Analytics (via `_latest` views) |
| **BigQuery `scholar_raw_data.pub`** | Raw publication records | Ingestion | Analytics (via `_latest` views) |
| **BigQuery views** | Computed metrics/percentiles | Analytics | Cache Layer, Refresh & Expand |
| **BigQuery materialized tables** | Daily snapshots of views | Analytics | Cache Layer (bulk exports) |
| **Firestore (cache collections)** | Query result cache | Cache Layer | Frontend, Author Search |
| **Cloud Tasks `process-authors`** | Author fetch queue | Refresh & Expand | Crawler |
| **Cloud Tasks `process-pubs`** | Publication fetch queue | Crawler | Crawler |
| **Cloud Tasks `cache-priority`** | Interactive cache population | Frontend (on miss), Ingestion (on load) | Cache Layer |
| **Cloud Tasks `cache-batch`** | Background cache warming/rebuild | Cloud Scheduler, Refresh, Cache Layer | Cache Layer |

---

## Design Decisions

1. **Staleness tracking: BigQuery.** Use `SELECT MAX(timestamp) FROM scholar_raw_data.author WHERE document_id = @id`. This eliminates Firestore as a dependency for Refresh & Expand. BigQuery latency (~500ms) is acceptable since staleness checks run on schedules, not in user-facing request paths.

2. **Author search: Separate component (Component 6).** A dedicated service with local BigQuery search before Scholar fallback. Benefits from 9-region rotation for Scholar API calls. The frontend does not import `scholarly`.

3. **On-demand latency: Show "queued" status.** The crawler is not real-time. When a user searches for an unknown author, Refresh & Expand enqueues the crawl and the frontend shows "Author queued for analysis." No event-driven ingestion needed for most cases.

4. **Self-contained components.** Each component (`frontend/`, `crawler/`, `ingestion/`, `refresh/`, `author_search/`, `cache_layer/`) has its own `config.py`, service modules, `requirements.txt`, and tests. No shared code directory — components are fully independent.

5. **Cache Layer separation (Component 7).** The frontend does not query BigQuery directly. A dedicated Cache Layer service owns all BigQuery reads and Firestore writes. Benefits: (a) frontend latency is bounded by Firestore read time, not BigQuery query time; (b) BigQuery costs are controlled by the cache layer, not by user traffic; (c) BigQuery outages don't take down the frontend (it serves whatever is in cache); (d) the cache is fully disposable and can be rebuilt from BigQuery at any time. Two Cloud Tasks queues (priority + batch) separate interactive requests from background warming.

---

## Cost and Performance

### Cost profile

| Area | Cost | Notes |
|---|---|---|
| **GCS storage** | Negligible | Raw JSON + archives < 1GB |
| **Cloud Tasks** | Near-zero | Low volume |
| **BigQuery storage** | Minimal | Small raw + materialized tables |
| **Daily batch ingestion** | Free tier | One load job per day |
| **9-region function deployment** | Low | Minimal cost when idle, needed for rate-limit avoidance |
| **Firestore cache** | Low | 5-7 reads per page view |
| **Server-side matplotlib** | Moderate | ~200-500ms CPU per page load for 4-6 charts |

### Performance characteristics

| Operation | Latency | Mitigation |
|---|---|---|
| Frontend page load (cache hit) | ~50-100ms | Firestore read only — no BigQuery in request path |
| Frontend page load (cache miss) | 2-5s | Priority queue → Cache Layer → BigQuery → Firestore; user sees loading page then auto-refresh |
| BigQuery per-author query (Cache Layer) | 1-3s | Runs in Cache Layer, not frontend. Distribution table lookups keep view cost low. |
| matplotlib chart generation | 200-500ms | Could move to client-side JS charting in the future. |
| scholarly.fill() for author | 10-60s | Unavoidable — Scholar is slow. 1-hour timeout is appropriate. |
| scholarly.search_author() | 2-5s | Local BigQuery search first (Component 6) avoids this for most queries. |
| Bulk export (all authors) | 5-15s | Uses materialized tables. Pre-generated CSV served from GCS. |
| Full cache rebuild | Minutes | Batch queue fan-out; runs in background, no user impact. |

---

## Future Directions

| Feature | Description |
|---|---|
| **REST API** | Expose authors, publications, and stats as JSON API endpoints for third-party integrations. Cache Layer already provides the data; API would read from Firestore. |
| **Client-side charting** | Replace server-side matplotlib with Chart.js/Plotly for interactive plots. Data served via API from Firestore cache. |
| **Field-specific benchmarks** | Compare against specific fields (business, CS, biology) with per-field percentiles |
| **Crossref integration** | Enrich publications with DOIs and metadata from Crossref |
| **Author comparison** | Side-by-side PiP-AUC and percentile plots for multiple authors |

---

_Last updated: 2026-03-23_
