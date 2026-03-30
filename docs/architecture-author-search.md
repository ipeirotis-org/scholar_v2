# Component 4: Author Search

> Part of [System Architecture](ARCHITECTURE.md)

**Purpose:** Help users find authors by name. Uses an in-memory index for instant results, with Semantic Scholar API fallback for less-known researchers.

**Input:** Author name query string.

**Output:** List of matching authors with name, affiliation, S2 author ID, citation count.

## What it does

1. **In-memory search (instant):** Substring matching against an in-memory index of ~360K prominent S2 authors (hindex ≥ 20, citedby > 5000). Loaded from BigQuery's `ranked_author_current_table`, refreshed every 6 hours.
2. **S2 API fallback (1-3s):** For authors not in the index, queries the Semantic Scholar API to search the full 102M author universe.
3. **Cache results:** S2 API search results are cached in Firestore (24h TTL) to avoid repeated API calls.

## Search modes

| Mode | Trigger | Source | Latency |
|------|---------|--------|---------|
| Typeahead | `typeahead=True` | In-memory index, top 10 | <10ms |
| Full search | Default | In-memory index, top 50 | <10ms |
| Extended search | `scholar=True` | In-memory + S2 API fallback | 1-3s |

## Architecture

Author Search is a **library integrated into the frontend**, not a standalone service. It runs in-process within the frontend's Cloud Run container:

- `author_search/search_service.py` — manages the in-memory index and search logic
- `author_search/bigquery_client.py` — loads active authors from BigQuery for the index
- `author_search/cache.py` — Firestore cache for search results and chunked index data

The in-memory index enables instant typeahead with zero external calls for most queries.

## Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery (`ranked_author_current_table` for index refresh) | |
| | Firestore (search result cache, chunked index cache) | |
| | Semantic Scholar API (fallback for unknown authors) | |
| **Writes** | | Firestore (search result cache) |

## Frontend integration

- Debounced input (300ms) → in-process search call
- Display results as dropdown: name, affiliation, S2 author ID
- Clicking a result navigates to `/results?author_id=X`
- If no results found: "Search Semantic Scholar?" button triggers the S2 API fallback

## Implementation

| File | Role |
|---|---|
| `author_search/search_service.py` | In-memory index search (loaded from BQ, refreshed every 6h) |
| `author_search/bigquery_client.py` | Loads active S2 authors for the in-memory index |
| `author_search/cache.py` | Firestore cache (24h TTL for search results, chunked index) |
| `author_search/config.py` | Config with env var overrides |
