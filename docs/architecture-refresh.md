# Component 5: Refresh & Expand

> Part of [System Architecture](ARCHITECTURE.md)

**Purpose:** Orchestrate data freshness and database growth by instructing the Crawler what to fetch. This is the only component that enqueues crawl tasks.

**Input:** Schedules, user requests, staleness analysis, coauthor graph.

**Output:** Tasks in Cloud Tasks queues (which feed the Crawler).

## What it does

1. **Stale author refresh:** Identify authors not updated in N days → enqueue for re-crawl
2. **Error author re-crawl:** Find authors with highest fetch errors and re-crawl them, with a **24-hour cooldown** to avoid retry loops
3. **User-triggered refresh:** When a user views an author and requests a refresh → enqueue for re-crawl
4. **New author fetch:** When a user searches for an author not in the database → enqueue for initial crawl
5. **Coauthor expansion:** Analyze the coauthor graph → identify high-value authors not yet in the database → enqueue for crawl (~1 per 10 min = ~4K new authors/month)
6. **On-demand ingestion trigger:** After enqueuing a crawl for a user-requested author, optionally trigger an immediate ingestion cycle so results appear faster than the daily batch

## Scheduled tasks

| Task | Schedule | Description |
|---|---|---|
| **Refresh stale** | Periodic | Find oldest entries by timestamp → enqueue for re-crawl |
| **Fix errors** | Periodic | Find authors with highest error counts → re-crawl (skip if processed within 24h) |
| **Add coauthors** | ~1 per 10 min | Pick from `coauthors_to_add` view → enqueue for initial crawl |

## Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery (`coauthors_to_add` view, raw table timestamps for staleness, error counts) | |
| **Writes** | | Cloud Tasks (`process-authors` queue) |

## Refresh policies

| Trigger | Condition | Action |
|---|---|---|
| Scheduled | Author not updated in 90+ days | Enqueue for re-crawl |
| Scheduled | Author has high error count + last attempt > 24h ago | Enqueue for re-crawl |
| Scheduled | Coauthor not in database, high coauthor frequency | Enqueue for initial crawl |
| User-driven | User clicks "refresh" on author profile | Enqueue for re-crawl |
| User-driven | User searches for unknown author ID | Enqueue for initial crawl + trigger ingestion |

## Implementation

Refresh & Expand runs as a **separate Cloud Run service** with:
- **Cloud Scheduler triggers** for periodic stale refresh and coauthor expansion
- **HTTP endpoints** for user-triggered refreshes (called by the frontend)
- Its own deployment pipeline, independent of the frontend

This separation keeps the frontend truly read-only.
