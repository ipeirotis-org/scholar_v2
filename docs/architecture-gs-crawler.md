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

gs-weekly-materialize ──► gs-crawler job (CRAWL_MODE=materialize): rebuild dist_* tables + search index
gs-monthly-wikidata ──► gs-crawler job (CRAWL_MODE=wikidata): Wikidata GS IDs + Wikipedia pageviews
        └─► gs_data.wikidata_authors ──► search index (unprofiled names) + refresh scheduler (new authors)
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
| `gs_crawler/wikidata.py` | GS IDs from Wikidata (P1960) with sitelinks and English Wikipedia pageviews → `gs_data.wikidata_authors` |
| `gs_crawler/main.py` | Job entry point: `CRAWL_MODE=refresh \| authors \| materialize \| deploy \| wikidata` |
| `bigquery/gs_data/` | Snapshot tables, latest views, one-off legacy seed |
| `bigquery/gs_statistics/` | GS versions of the non-temporal analytics views (same output columns as `statistics`) |

## Finding authors we have not crawled (Wikidata)

Google Scholar's author search needs a signed-in browser, and SerpApi's Scholar results no longer
link author names to profiles, so the app cannot turn a name into a GS ID. Wikidata can: ~130K people
carry a Google Scholar author ID (property P1960). The monthly `wikidata` run:

1. Pulls every P1960 holder with an English label (~125K) (QLever 429s and 5xx are retried after
   1, 3 and 7 minutes; the pageview window shrinks by the time this takes), plus the English description, the
   sitelinks count and the English Wikipedia article title. One query on QLever (`qlever.dev`,
   ~20 s); if that fails, the Wikidata Query Service, one query per leading ID character (split
   further, with back-off, on timeouts). WDQS alone proved fragile: the first production run
   failed after 26 min when it throttled small partitions.
2. Adds 60-day English Wikipedia pageviews from the per-article REST endpoint (~22K articles,
   8 workers, at most 40 min). Wikimedia throttles it to ~3.5 articles/s, so a run covers ~8K:
   views from earlier runs are carried forward with their fetch time (`views_fetched_at`), and
   each run takes articles without views first, then the stalest (best-known by sitelinks first
   within ties). Coverage is complete after about three monthly runs, and from then on every
   article's views are at most about three months old. Missing views
   stay NULL. Rejected alternatives: the Action API's `prop=pageviews` answers ~5 titles per
   request before asking for a continuation; the monthly pageview dump is 5.7 GB.
3. Replaces `gs_data.wikidata_authors`, refusing a load that would shrink it by more than half.
4. Rebuilds the app's author name index (the weekly `materialize` run does this too).

**Search.** The index holds every profiled author, the unprofiled Wikidata ones (~121K) and the
unprofiled co-authors listed on profiled authors' pages (~180K, ~157K of them not on Wikidata),
about 290K entries. Unprofiled entries show "not analyzed yet"; selecting one crawls the profile on
demand. Ranking: profiled authors by citations, then Wikidata people by sitelinks, then co-authors by
how many profiled authors list them. For people in both lists, the co-author box's affiliation
replaces the Wikidata description.
Names that are in neither still need a pasted profile URL; the empty result says so and links
to a web search for the profile.

**Ingestion priority.** Each refresh run spends up to `NEW_AUTHOR_SHARE` (25%) of its budget on
Wikidata authors we have never crawled, most-viewed Wikipedia article first, then by sitelinks.
Only living people with a scholarly occupation are eligible (~116K): researcher, engineer, academic,
university teacher or physician (or a subclass), and not a politician. Unfiltered, the most-viewed
candidates were Einstein, Newton, Marx, Xi Jinping and Joe Biden. Search still lists everyone.
Unused share goes back to refreshes. The share is enforced on actual searches: a new profile's crawl is
capped at what is left of it, and one too large to fit is cooled down like a failure. Permanent failures (no profile behind the ID) go to
`gs_data.crawl_failures`, and the scheduler skips those IDs for `FAILURE_COOLDOWN_DAYS` (30).

## Budget (SerpApi Developer plan, 5,000 searches/month)

- **Refresh:** `MAX_SEARCHES_PER_RUN=130` per daily run, about 3,900 a month. Runs never take the account
  below `SERPAPI_SEARCH_RESERVE=1000`, which is kept for on-demand lookups. Up to a quarter of each run
  (~32 searches, ~10 authors a day) goes to new Wikidata authors.
- **Cost per author:** an average profile costs ~3 searches, so ~1,300 authors are crawled a month. With
  ~300 of those new, the 16.2K reference set turns over in roughly 16 months while it grows by ~3,600 a year.
- **On-demand:** a new author costs ~3 searches. A Firestore lock (`v3_gs_crawl_locks/{id}`, 10 min) ensures one crawl per author even though the loading page re-enqueues a populate task on every reload. These crawls stop when the account is down to
  `GS_ON_DEMAND_MIN_SEARCHES=25`. There is deliberately no daily cap (decided 2026-11-02).
- **Free calls:** account checks (`account.json`) and failed searches are not counted by SerpApi.

## Differences from the S2 analytics

- **Reference population.** Percentiles are relative to the profiled GS authors: ~16.2K, seeded from the
  2024–2025 legacy crawl, plus on-demand additions. The S2 version compared against ~99M S2 authors. This is how
  the pre-S2 GS version worked, but the scale moves: a PiP-AUC of ~0.23 is the median, ~0.40 the 75th and
  ~0.58 the 90th percentile (pooled across cohorts, 2026-11). Kept by decision on 2026-11-02.
- **Metrics source.** h-index, i10 and citations come straight from the GS profile. The `*5y` columns hold
  the profile's "since YYYY" values.
- **Cohort year.** `year_of_first_pub` ignores uncited items, and also a small cluster of cited ones
  (at most 2, or 5% of the cited papers) that sits before a gap of more than 5 years: GS profiles carry
  misdated entries and namesakes' papers decades before the real career (Pietro Perona: one 1963 item,
  then 1988 onward). This moves 1,689 of 16,207 profiles (2026-11-03). Wrong cases can be fixed by hand
  in `gs_data.start_year_overrides` (latest `set_at` wins; effective at the next dist rebuild).
- **No minimum.** There is no ≥6-publication filter, since the population is already curated.
- **Benchmark.** The `active_authors` benchmark is unchanged: hindex ≥ 3 and ≥ 3 publications.

## Switching the app to GS (done 2026-11-03)

`deploy-frontend.yml` deploys the frontend and cache layer with `DATA_SOURCE=gs` by default, which
mounts `serpapi-key` into the cache layer. The cache layer's runtime account
(`875626982900-compute@developer.gserviceaccount.com`) has `roles/secretmanager.secretAccessor` on that
secret. GS mode uses separate Firestore collections (`v3_gs_author_name_index`, `v3_gs_author_search`,
`v3_recent_authors/recent_gs`); the name index bootstraps itself on first use, or call
`POST /api/refresh_author_index`.

The author caches (`v3_author_*`, `v3_pub_stats`) are shared between modes. Their S2-era entries (numeric
S2 IDs) were deleted on 2026-11-03: 271 author docs, 4,232 per-paper docs, and the S2 rows in
`v3_recent_authors/recent_gs`. GS mode also refuses numeric IDs: author pages and `/api/author/<id>/data`
answer them with 410 Gone (the page links to a name search, using the cached name when there is one),
rebuild requests skip them, and the recent-authors list drops them (`is_legacy_s2_id` in
`frontend/cache.py`). The S2-only collections (`v3_author_name_index`, `v3_author_search`,
`v3_recent_authors/recent`) are kept for rollback.

**Roll back:** set the repository variable `DATA_SOURCE=s2` and re-run *Deploy Frontend & Cache Layer*.
The S2 views and tables are untouched (S2 data is frozen; no new fetches). S2 author pages repopulate
from BigQuery on first view, since their cache entries were deleted.
