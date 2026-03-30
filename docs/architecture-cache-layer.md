# Component 5: Cache Layer

> Part of [System Architecture](ARCHITECTURE.md)

**Purpose:** Populate and maintain the Firestore cache from BigQuery. This is the **only** component that writes to Firestore cache collections. The cache is fully disposable — it can be wiped and rebuilt from BigQuery at any time.

**Input:** Tasks from two Cloud Tasks queues (priority and batch).

**Output:** Firestore cache documents.

## What it does

1. **Priority queue tasks** (interactive, user-waiting — target <2s):
   - `populate_author_profile` — query BigQuery for author_stats, pub_stats, temporal_stats; write all to Firestore
   - `populate_publication_detail` — query BigQuery for publication citation temporal data; write to Firestore
   - `invalidate_author` — re-populate all caches for an author whose data has changed

2. **Batch queue tasks** (background, scheduled):
   - `populate_recent_authors` — query BigQuery for recently analyzed authors; write to Firestore (scheduled every 5 min)
   - `warm_author` — pre-populate cache for a newly crawled author before a user visits
   - `rebuild_all` — full cache rebuild from BigQuery (enqueues individual `populate_author_profile` tasks to batch queue)

## Data flow

```
Cloud Tasks (priority or batch queue)
    → Cache Layer (Cloud Run)
        → BigQuery (read analytics views)
        → Firestore (write cache documents)
```

The Cache Layer is the **single writer** to Firestore cache collections. This one-way flow makes the cache fully disposable: delete all Firestore cache documents, trigger `rebuild_all`, and the cache is restored from BigQuery.

## Cache collections

| Collection | Document ID | Data | Populated by |
|---|---|---|---|
| `v3_author_stats/{id}` | scholar_id | Author metrics + percentiles + PiP-AUC | `populate_author_profile` |
| `v3_author_pub_stats/{id}` | scholar_id | Per-publication PiP inputs and metadata | `populate_author_profile` |
| `v3_author_temporal/{id}` | scholar_id | H-index, citations, i10 over time | `populate_author_profile` |
| `v3_pub_stats/{id}` | author_pub_id | Temporal citation data for one publication | `populate_publication_detail` |
| `v3_author_freshness/{id}` | scholar_id | Existence + last_updated timestamp | `populate_author_profile` |
| `v3_recent_authors` | `recent` | List of recently analyzed authors | `populate_recent_authors` |

## Cache invalidation

The Cache Layer owns invalidation logic:
- **On data change:** The legacy Ingestion Pipeline (Component 1) enqueues `invalidate_author` to the priority queue after loading new data for an author. The Cache Layer checks the author's latest BigQuery timestamp and re-populates all caches. Note: the S2 Dataset Ingestion pipeline does not currently trigger cache invalidation.
- **On cache miss:** The Frontend (Component 3) enqueues a `populate` task to the priority queue. The Cache Layer runs the queries and writes fresh data to Firestore.
- **Scheduled:** `populate_recent_authors` runs every 5 minutes via Cloud Scheduler → batch queue.
- **Manual rebuild:** `rebuild_all` can reconstruct the entire cache from BigQuery.

No timestamp-comparison logic is needed in the frontend — it simply reads whatever is in Firestore.

## Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery (analytics views and materialized tables) | |
| **Writes** | | Firestore (cache collections) |
| **Receives work from** | Cloud Tasks (`cache-priority`, `cache-batch`) | |
| **Enqueues work to** | Cloud Tasks (`cache-batch`, for `rebuild_all` fan-out) | |

## Implementation

| File | Role |
|---|---|
| `cache_layer/main.py` | Cloud Run HTTP entry points (one handler per queue) |
| `cache_layer/cache_service.py` | Orchestration: dispatch by request type, coordinate queries |
| `cache_layer/bigquery_client.py` | Read-only BigQuery queries (moved from frontend) |
| `cache_layer/cache_writer.py` | Write-only Firestore client |
| `cache_layer/config.py` | Config with env var overrides |
| `cache_layer/Dockerfile` | Python 3.12-slim |

## Infrastructure

- **Cloud Run:** `cache-layer-service`, us-central1
- **Cloud Tasks queues:**
  - `cache-priority` — high concurrency, short timeout (~30s), for interactive cache population
  - `cache-batch` — rate-limited, longer timeout (~5min), for warming and rebuilds
- **Cloud Scheduler:** Triggers `populate_recent_authors` every 5 minutes via batch queue
