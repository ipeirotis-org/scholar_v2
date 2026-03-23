# Component 6: Author Search Service

> Part of [System Architecture](ARCHITECTURE.md)

**Purpose:** Help users find authors by name. Searches local data first (cheap, fast), falls back to Google Scholar only when needed.

**Input:** Author name query string.

**Output:** List of matching authors with name, affiliation, scholar_id, citation count.

## What it does

1. **Local search (fast, free):** Query BigQuery for authors already in the database whose name matches the query. This covers ~15,000+ faculty profiles plus all their coauthors (names and IDs available from the coauthor network).
2. **Google Scholar fallback (slow, rate-limited):** If local search returns too few results, call `scholarly.search_author()` to find authors not yet in the database.
3. **Cache results:** Cache Google Scholar search results to avoid repeated API calls for the same query.

## Data sources for local search

| Source | What it contains | Coverage |
|---|---|---|
| `stats_author_current` view | name, affiliation, email_domain, scholar_id, metrics | All crawled authors (~15k+) |
| `coauthor_network` view | coauthor_name, coauthor_affiliation, coauthor_scholar_id | All coauthors of crawled authors (much larger set) |
| `coauthors_to_add` view | name, affiliation, scholar_id of authors not yet crawled | Discovery candidates |

Most academic searches can be answered locally — a professor searching for their colleague will almost certainly find them in the coauthor graph without hitting Google Scholar.

## Search strategy

```
1. Query BigQuery: SELECT name, affiliation, scholar_id
   FROM stats_author_current WHERE LOWER(name) LIKE @pattern
   → Return if sufficient results

2. Query BigQuery: SELECT coauthor_name, coauthor_affiliation, coauthor_scholar_id
   FROM coauthor_network WHERE LOWER(coauthor_name) LIKE @pattern
   → Return if sufficient results

3. Check Firestore cache for this query string
   → Return if cached and fresh

4. Fall back to scholarly.search_author(name)
   → Cache results in Firestore
   → Return
```

## Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery (author + coauthor views) | |
| | Firestore (search result cache) | |
| | Google Scholar (fallback via `scholarly`) | |
| **Writes** | | Firestore (search result cache) |

## Frontend integration

The frontend calls this service for author search:
- Debounced input (300ms) → HTTP call to Author Search Service
- Display results as dropdown: name, affiliation, scholar_id
- Clicking a result navigates to `/results?author_id=X`
- If no results found: show "Search Google Scholar?" button that triggers the Scholar fallback explicitly

## Infrastructure

- **Cloud Function (Gen2):** Deployed across 9 US regions (same rotation as Crawler)
- Benefits from region rotation for Google Scholar fallback calls
