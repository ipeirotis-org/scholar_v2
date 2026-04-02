# Component 3: Frontend

> Part of [System Architecture](ARCHITECTURE.md)

**Purpose:** Display precomputed analytics with interactive visualizations. **Read-only** — reads only from Firestore cache (populated by the Cache Layer). Does not query BigQuery directly.

**Input:** User queries (author search via Author Search library, author ID for profile display).

**Output:** HTML pages with client-side Plotly.js charts.

## What it does

1. **Home page:** Recently analyzed authors (from Firestore cache) and search bar
2. **Author search:** User enters an author name → calls Author Search library (in-process) → display matching profiles
3. **Author profile:** User selects an author → read metrics from Firestore → render interactive charts with Plotly.js
4. **Publication detail:** User clicks a publication → read citation timeline data from Firestore → render chart
5. **Download:** Export author publications as CSV
6. **API endpoints:** JSON API for programmatic access to author data

## Cache miss handling

When the frontend reads from Firestore and the data is not present (cache miss):
1. Enqueue a `populate` task to the Cache Layer's **priority queue** (Cloud Tasks)
2. Return a "loading" page to the user
3. The user's page auto-refreshes; when the Cache Layer has populated the data, the next request returns the full page

## Visualization

All charts are rendered **client-side** using Plotly.js:
- Percentile rank plot (paper rank vs citation percentile)
- PiP-AUC scatter plot (num_papers_percentile vs num_citations_percentile)
- Publication citation timeline (yearly + cumulative percentile)
- Temporal author metrics (h-index, citations, i10 over time)

The server passes structured JSON data to Jinja2 templates; `frontend/static/js/charts.js` handles all Plotly rendering. No server-side chart generation.

## Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | Firestore (cache — sole data source for display) | |
| | Author Search library (in-process, in-memory index) | |
| **Writes** | | Cloud Tasks (`cache-priority` queue, on cache miss) |

**The frontend does NOT:**
- Query BigQuery directly (all data comes from Firestore, populated by the Cache Layer)
- Write to Firestore (the Cache Layer owns all cache writes)
- Write to BigQuery
- Modify raw data

## Implementation

| File | Role |
|---|---|
| `frontend/main.py` | App entry point |
| `frontend/app.py` | Flask app factory with security headers |
| `frontend/routes.py` | Routes: `/`, `/results`, `/publication`, `/download`, `/data`, `/help`, `/api/*` |
| `frontend/cache.py` | Read-only Firestore cache client |
| `frontend/queue_client.py` | Thin client to enqueue cache-miss tasks to priority queue |
| `frontend/config.py` | Config with env var overrides |
| `frontend/templates/` | Jinja2 HTML templates |
| `frontend/static/js/charts.js` | Plotly.js chart rendering |
| `frontend/static/` | CSS, JS assets |

## Infrastructure

- **Cloud Run:** `scholar-service`, us-central1, port 8080
- **Docker:** Python 3.12-slim
