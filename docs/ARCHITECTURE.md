# System Architecture: PiP Score

## Overview

PiP Score is a distributed system for analyzing research impact using percentile-based, age-aware metrics (PiP-AUC). It uses **Semantic Scholar** bulk datasets (200M+ papers, 2.4B+ citations, 102M authors) and serves an interactive Flask web app at [pip-score.org](https://www.pip-score.org/).

```
                    Semantic Scholar
                     (S2 Datasets API)
                          |
                  [1. DATASET INGESTION]
                     (Cloud Run Job)
                          |
                   S3 → GCS → BigQuery
                   (s2_data: papers, citations, authors)
                          |
                  [2. ANALYTICS + MATERIALIZATION]
                     (materialize_tables.py)
                          |
                   BigQuery (materialized tables, 7-level DAG)
                         /|\
                        / | \
                       /  |  \
     [5. CACHE LAYER]    |   [4. AUTHOR SEARCH]
      (BQ → Firestore)   |     (in-memory index + S2 API fallback)
            |             |           |
      Firestore cache     |     integrated in frontend
            |             |
     [3. FRONTEND]        |
     (Firestore-only)     |
            |             |
            +--- calls ---+
```

---

## Components

| # | Component | Purpose | Input | Output | Details |
|---|---|---|---|---|---|
| 1 | **Dataset Ingestion** | Download S2 datasets and load into BigQuery | S2 API (S3 bulk files) | BigQuery tables (`s2_data.*`) + materialized analytics DAG | [architecture-ingestion.md](architecture-ingestion.md) |
| 2 | **Analytics** | Compute metrics, percentiles, and PiP-AUC scores | BigQuery base tables (`s2_data.*`) | BigQuery materialized tables (`statistics.*`) | [architecture-analytics.md](architecture-analytics.md) |
| 3 | **Frontend** | Display precomputed analytics (read-only from Firestore) | User queries, Firestore cache | HTML pages with Plotly.js charts | [architecture-frontend.md](architecture-frontend.md) |
| 4 | **Author Search** | Find authors by name (in-memory index + S2 API fallback) | Author name query string | Matching author list | [architecture-author-search.md](architecture-author-search.md) |
| 5 | **Cache Layer** | Populate Firestore cache from BigQuery | Cloud Tasks (priority + batch queues) | Firestore cache documents | [architecture-cache-layer.md](architecture-cache-layer.md) |

---

## Data Stores Summary

| Store | Role | Written by | Read by |
|---|---|---|---|
| **GCS `s2_datasets/`** | Downloaded S2 dataset files (staging) | Dataset Ingestion | Dataset Ingestion (→ BigQuery load) |
| **BigQuery `s2_data.papers`** | S2 paper records (233M rows) | Dataset Ingestion | Analytics |
| **BigQuery `s2_data.citations`** | S2 citation records (5.6B rows) | Dataset Ingestion | Analytics |
| **BigQuery `s2_data.authors`** | S2 author records (102M rows) | Dataset Ingestion | Analytics |
| **BigQuery `s2_data.author_paper_bridge`** | Derived: authorid → corpusid mapping | Dataset Ingestion | Analytics |
| **BigQuery `s2_data.author_paper_stats`** | Derived: author publication summaries | Dataset Ingestion | Analytics |
| **BigQuery `s2_data.paper_citations_by_year`** | Derived: per-paper citation counts by year | Dataset Ingestion | Analytics |
| **BigQuery `statistics.*` tables** | Materialized analytics (6-level DAG, 15 tables) | Materialization (during ingestion) | Cache Layer, Author Search |
| **BigQuery `statistics.*` views** | Live analytics views | CI/CD (bigquery-views.yml) | Cache Layer (when `USE_MATERIALIZED_TABLES=false`), dev/debugging |
| **Firestore (cache collections)** | Query result cache | Cache Layer | Frontend, Author Search |
| **Cloud Tasks `cache-priority`** | Interactive cache population | Frontend (on miss), legacy Ingestion (on load) | Cache Layer |
| **Cloud Tasks `cache-batch`** | Background cache warming/rebuild | Cloud Scheduler, Cache Layer (`rebuild_all` fan-out) | Cache Layer |

---

## Design Decisions

1. **Data source: Semantic Scholar bulk datasets.** S2 provides weekly diff releases of their full academic graph (200M+ papers, 102M authors). Monthly ingestion with diff application keeps data fresh while controlling BigQuery costs.

2. **Selective DAG materialization.** 15 analytics tables (across 6 levels) are materialized during each monthly ingestion — only tables directly queried by the app, used as inputs by downstream materializations, or needed for percentile lookups. Intermediate views are left as views; their output is consumed inline by downstream materialized tables.

3. **Author search: In-memory index with S2 API fallback.** An in-memory index of ~360K prominent S2 authors (hindex ≥ 20, citedby > 5000) runs inside the frontend Cloud Run service, reloaded from a Firestore-persisted index every 6 hours. The Firestore index is rebuilt from BigQuery on bootstrap or manual trigger. For less-known researchers, the S2 API provides fallback coverage of the full 102M author universe.

4. **Cache Layer separation (Component 5).** The frontend does not query BigQuery directly. A dedicated Cache Layer service owns all BigQuery reads and Firestore writes. Benefits: (a) frontend latency is bounded by Firestore read time, not BigQuery query time; (b) BigQuery costs are controlled by the cache layer, not by user traffic; (c) BigQuery outages don't take down the frontend; (d) the cache is fully disposable and can be rebuilt from BigQuery.

5. **Client-side visualization.** Charts are rendered in the browser using Plotly.js. The server passes structured JSON data to templates; no server-side chart generation.

6. **Self-contained components.** Each component (`frontend/`, `cache_layer/`, `dataset_ingestion/`, `author_search/`, `ingestion/`) has its own `config.py`, service modules, `requirements.txt`, and tests.

---

## Cost and Performance

### Cost profile

| Area | Cost | Notes |
|---|---|---|
| **GCS storage** | Moderate | S2 dataset files (~100GB per release, cleaned after load) |
| **Cloud Tasks** | Near-zero | Low volume |
| **BigQuery storage** | Moderate | S2 raw tables + materialized analytics tables |
| **Monthly ingestion** | Moderate | Bulk load + full DAG materialization (21 tables) |
| **Firestore cache** | Low | 5-7 reads per page view |
| **Client-side Plotly.js** | Zero server cost | Charts render in the browser |

### Performance characteristics

| Operation | Latency | Notes |
|---|---|---|
| Frontend page load (cache hit) | ~50-100ms | Firestore read only — no BigQuery in request path |
| Frontend page load (cache miss) | 2-5s | Priority queue → Cache Layer → BigQuery → Firestore; user sees loading page |
| BigQuery per-author query (Cache Layer) | 1-3s | Materialized table lookups keep cost low |
| Author search (in-memory) | <10ms | Instant substring matching on ~360K authors |
| Author search (S2 API fallback) | 1-3s | For authors not in the in-memory index |
| Bulk export (all authors) | 5-15s | Pre-materialized tables; CSV served from GCS |
| Full cache rebuild | Minutes | Batch queue fan-out; runs in background |
| Monthly S2 ingestion + materialization | Hours | Full dataset diff + 7-level DAG materialization |

---

## Legacy Components

The `ingestion/` directory contains the original GCS → BigQuery batch load pipeline for Google Scholar data (`scholar_raw_data` dataset). While superseded by `dataset_ingestion/` for S2 bulk datasets, it is still actively deployed: `deploy-ingestion.yml` provisions the `v3_batch_load_gcs_to_bq` Cloud Function and an hourly Cloud Scheduler job to process any remaining Google Scholar JSON files.

---

_Last updated: 2026-03-30_
