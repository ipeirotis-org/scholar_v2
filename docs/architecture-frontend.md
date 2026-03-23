# Component 4: Frontend

> Part of [System Architecture](ARCHITECTURE.md)

**Purpose:** Display precomputed analytics with visualizations. **Read-only** — reads only from Firestore cache (populated by the Cache Layer). Does not query BigQuery directly.

**Input:** User queries (author search via Author Search Service, author ID for profile display).

**Output:** HTML pages with embedded charts.

## What it does

1. **Home page:** Recently analyzed authors (from Firestore cache) and search bar
2. **Author search:** User enters an author name → calls Author Search Service (Component 6) → display matching profiles
3. **Author profile:** User selects an author → read metrics from Firestore → render charts with visualization module
4. **Publication detail:** User clicks a publication → read citation timeline data from Firestore → render chart
5. **Download:** Export author publications as CSV
6. **Refresh request:** User clicks "refresh" on a stale author → forwards request to Refresh & Expand service (Component 5)

## Cache miss handling

When the frontend reads from Firestore and the data is not present (cache miss):
1. Enqueue a `populate` task to the Cache Layer's **priority queue** (Cloud Tasks)
2. Return a "loading" page to the user (this pattern already exists for uncrawled authors)
3. The user's page auto-refreshes; when the Cache Layer has populated the data, the next request returns the full page

## Page structure

All pages share a consistent template with:
- **Header:** Permanent links to Home, Help, and other key pages
- **Footer:** Attribution, contact, methodology link
- **Navigation:** Consistent across all pages via a base template

## Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | Firestore (cache — sole data source for display) | |
| | Author Search Service (Component 6) | |
| **Writes** | | Cloud Tasks (`cache-priority` queue, on cache miss) |
| **Calls** | | Refresh & Expand (Component 5) for user-triggered refreshes |

**The frontend does NOT:**
- Query BigQuery directly (all data comes from Firestore, populated by the Cache Layer)
- Write to Firestore (the Cache Layer owns all cache writes)
- Write to BigQuery
- Enqueue crawl tasks directly
- Modify raw data
- Call Google Scholar directly (that goes through Component 6)
- Run scheduled refresh or expansion (that is Component 5's job)

## Visualization

All charts are generated server-side with matplotlib and embedded as base64 PNG:
- Percentile rank plot (paper rank vs citation percentile)
- PiP-AUC scatter plot (num_papers_percentile vs num_citations_percentile)
- Publication citation timeline (yearly + cumulative percentile, dual axis)
- Temporal author metrics (h-index, citations, i10 over time)

The Cache Layer provides the structured data; the frontend owns how to visualize it.

## Implementation

| File | Role |
|---|---|
| `frontend/main.py` | App entry point |
| `frontend/app.py` | Flask app factory with security headers |
| `frontend/routes.py` | Routes: `/`, `/results`, `/publication`, `/download`, `/data`, `/help`, `/api/*` |
| `frontend/cache.py` | Read-only Firestore cache client |
| `frontend/queue_client.py` | Thin client to enqueue cache-miss tasks to priority queue |
| `frontend/visualization.py` | Matplotlib chart generation (base64 PNG) |
| `frontend/config.py` | Config with env var overrides |
| `frontend/templates/` | Jinja2 HTML templates |
| `frontend/static/` | CSS, JS assets |

## Infrastructure

- **Cloud Run:** `scholar-service`, us-central1, port 8080
- **Docker:** Python 3.12-slim
