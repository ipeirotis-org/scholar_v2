# Google Scholar Crawler (Phase A: profiles only)

> Part of [System Architecture](ARCHITECTURE.md). Background and vendor choice:
> [google-scholar-data-sources.md](google-scholar-data-sources.md).

**Purpose:** keep Google Scholar (GS) profile snapshots in BigQuery and compute current PiP-AUC from them,
in parallel to the Semantic Scholar (S2) pipeline. The live app reads GS data only when `DATA_SOURCE=gs`.

**Scope:** profile pages only: metrics table, author yearly chart, co-authors, and the full publication
list with per-paper totals. No per-paper citation histograms, so the temporal views stay S2-only for now
(Phase B, see `TASKS.md`).

## Data flow

```
gs-daily-refresh (Cloud Scheduler) ──► gs-crawler job (CRAWL_MODE=refresh)
cache miss in GS mode ──► cache layer ──► gs_crawler.crawl_and_store (on demand)
        │
        ├─► SerpApi google_scholar_author (100 publications per search, validated)
        ├─► GCS  gs://scholar_data_share/gs_raw/serpapi/author/YYYY/MM/DD/<id>/...json
        └─► BigQuery gs_data.author_snapshots + publication_snapshots (append)
                │
                ▼
        gs_data.authors_latest / publications_latest (latest snapshot per author)
                │
                ▼
        gs_statistics.* views + dist_* tables ──► cache layer ──► Firestore ──► frontend

gs-weekly-materialize ──► gs-crawler job (CRAWL_MODE=materialize): rebuild dist_* tables
```

## Components

| Path | Role |
|------|------|
| `gs_crawler/serpapi_client.py` | SerpApi requests, retries, response validation (empty or error 200s are failures) |
| `gs_crawler/crawler.py` | Paginate one profile; a partially fetched profile is reported as failed, never stored |
| `gs_crawler/parser.py` | SerpApi pages → snapshot rows (legacy column names: `citedby`, `hindex`, `i10index`, `email_domain`, ...) |
| `gs_crawler/writer.py` | Raw pages to GCS, rows to BigQuery (load jobs; publications before authors) |
| `gs_crawler/scheduler.py` | Pick authors within the search budget: queued on-demand requests first, then stalest |
| `gs_crawler/materialize.py` | Deploy `gs_statistics` views / rebuild `dist_*` tables in `deploy_order.txt` order |
| `gs_crawler/main.py` | Job entry point: `CRAWL_MODE=refresh \| authors \| materialize \| deploy` |
| `bigquery/gs_data/` | Snapshot tables, latest views, one-off legacy seed |
| `bigquery/gs_statistics/` | GS versions of the non-temporal analytics views (same output columns as `statistics`) |

## Budget (SerpApi Developer plan, 5,000 searches/month)

- **Refresh:** `MAX_SEARCHES_PER_RUN=130` per daily run, about 3,900 a month. Runs never take the account
  below `SERPAPI_SEARCH_RESERVE=1000`, which is kept for on-demand lookups.
- **Cost per author:** an average profile costs ~3 searches, so ~1,300 authors are refreshed a month and the
  16.2K reference set turns over in roughly a year.
- **On-demand:** a new author costs ~3 searches. These crawls stop when the account is down to
  `GS_ON_DEMAND_MIN_SEARCHES=25`.
- **Free calls:** account checks (`account.json`) and failed searches are not counted by SerpApi.

## Differences from the S2 analytics

- **Reference population.** Percentiles are relative to the profiled GS authors: ~16.2K, seeded from the
  2024–2025 legacy crawl, plus on-demand additions. The S2 version compared against ~99M S2 authors. This is how
  the pre-S2 GS version worked, but the scale moves. A PiP-AUC of ~0.58 is the 90th percentile here (as of
  2026-11).
- **Metrics source.** h-index, i10 and citations come straight from the GS profile. The `*5y` columns hold
  the profile's "since YYYY" values.
- **Cohort year.** `year_of_first_pub` is the earliest *cited* paper with a plausible year, because GS
  profiles often contain stray, misdated uncited items.
- **No minimum.** There is no ≥6-publication filter, since the population is already curated.
- **Benchmark.** The `active_authors` benchmark is unchanged: hindex ≥ 3 and ≥ 3 publications.

## Switching the app to GS

1. **Grant secret access.** Give the cache layer's runtime account (`875626982900-compute@developer.gserviceaccount.com`)
   `roles/secretmanager.secretAccessor` on secret `serpapi-key`. On-demand crawls need it.
2. **Check write access.** Make sure that account can append to `gs_data` and write `gs://scholar_data_share/gs_raw/`.
3. **Flip the setting.** Set the repository variable `DATA_SOURCE=gs` and re-run *Deploy Frontend & Cache Layer*.
4. **Rebuild the search index.** Call `POST /api/refresh_author_index`. GS mode uses separate Firestore
   collections (`v3_gs_author_name_index`, `v3_gs_author_search`).

To roll back, set `DATA_SOURCE=s2` and redeploy. The S2 views and caches are untouched.
