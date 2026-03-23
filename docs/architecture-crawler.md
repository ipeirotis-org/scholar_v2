# Component 1: Crawler

> Part of [System Architecture](ARCHITECTURE.md)

**Purpose:** Fetch author and publication data from Google Scholar and write raw JSON to GCS.

**Input:** A Google Scholar author ID (via Cloud Tasks queue).

**Output:** Raw JSON files in GCS.

## What it does

1. Receives an author ID from the `process-authors` Cloud Tasks queue
2. Calls `scholarly.search_author_id()` + `scholarly.fill()` to get the full author profile
3. Serializes the author profile to JSON and uploads to GCS: `authors_json/YYYY/MM/DD/{scholar_id}.json`
4. For each publication in the author's profile, enqueues a publication fetch task to the `process-pubs` queue (with 0.1s stagger delay)
5. Each publication task calls `scholarly.fill()` on the publication and uploads to GCS: `publications_json/YYYY/MM/DD/{author_pub_id}.json`

## Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | Google Scholar (via `scholarly`) | |
| **Writes** | | GCS (`authors_json/`, `publications_json/`) |
| **Receives work from** | Cloud Tasks (`process-authors`, `process-pubs`) | |

## Implementation

| File | Role |
|---|---|
| `crawler/fetch_author.py` | Cloud Function: fetch author profile → GCS |
| `crawler/fetch_publication.py` | Cloud Function: fetch publication details → GCS |
| `crawler/gcs_writer.py` | GCS upload client with retry |
| `crawler/scholarly_client.py` | scholarly wrapper with timeout, retry, error classification |
| `crawler/task_enqueuer.py` | Enqueue publication tasks |
| `crawler/config.py` | Region rotation, queue config |

## Infrastructure

- **Cloud Functions (Gen2):** Deployed across 9 US regions with daily rotation to distribute Scholar API load and avoid rate limiting
- **Cloud Tasks queues:** `process-authors` (author fetches), `process-pubs` (publication fetches), both in `northamerica-northeast1`
- **Timeouts:** 1 hour for author fetch, 60 seconds for publication fetch
- **Idempotency:** Task names include the scholar/pub ID; Cloud Tasks deduplicates (AlreadyExists caught gracefully)

## Archival

After ingestion, raw JSON files are archived:
- `authors_json/` → `authors_archive/`
- `publications_json/` → `publications_archive/`

Archives are kept indefinitely in GCS for debugging and historical analysis.
