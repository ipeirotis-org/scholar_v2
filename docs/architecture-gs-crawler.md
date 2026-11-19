# Google Scholar Crawler (Phase A: profiles only)

> Part of [System Architecture](ARCHITECTURE.md). Background and vendor choice:
> [google-scholar-data-sources.md](google-scholar-data-sources.md).

**Purpose:** keep Google Scholar (GS) profile snapshots in BigQuery and compute current PiP ($\tau = 0.5$) and PiP-AUC from them,
in parallel to the Semantic Scholar (S2) pipeline. The live app reads GS data only when `DATA_SOURCE=gs`.

**Scope:** profile pages only: metrics table, author yearly chart, co-authors, and the full publication
list with per-paper totals. No per-paper citation histograms, so the temporal views stay S2-only for now
(Phase B, see `TASKS.md`).

## Data flow

```
gs-daily-refresh (Cloud Scheduler, every 6 hours) ──► gs-crawler job (CRAWL_MODE=refresh)
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

gs-weekly-materialize ──► gs-crawler job (CRAWL_MODE=materialize): rebuild dist_* + author_coauthor_fields tables, drop the
        cached author profiles (they hold the old percentiles), then search index + featured authors
gs-daily-percentiles ──► gs-crawler job (CRAWL_MODE=percentiles): the tables and the cache drop only, on the other six days
gs-monthly-wikidata ──► gs-crawler job (CRAWL_MODE=wikidata): Wikidata GS IDs + Wikipedia pageviews
        └─► gs_data.wikidata_authors ──► search index (unprofiled names) + refresh scheduler (new authors)
                                         + featured authors (homepage)
```

## Components

| Path | Role |
|------|------|
| `gs_crawler/serpapi_client.py` | SerpApi requests, retries, response validation (empty or error 200s are failures) |
| `gs_crawler/crawler.py` | Paginate one profile; a partially fetched profile is reported as failed, never stored |
| `gs_crawler/parser.py` | SerpApi pages → snapshot rows (legacy column names: `citedby`, `hindex`, `i10index`, `email_domain`, ...) |
| `gs_crawler/writer.py` | Raw pages to GCS, rows to BigQuery (load jobs; publications before authors) |
| `gs_crawler/scheduler.py` | Pick authors within the search budget: queued on-demand requests first, then stalest |
| `gs_crawler/materialize.py` | Deploy `gs_statistics` views / rebuild its tables (`dist_*`, `author_coauthor_fields`) in `deploy_order.txt` order |
| `gs_crawler/wikidata.py` | GS IDs from Wikidata (P1960) with sitelinks and English Wikipedia pageviews → `gs_data.wikidata_authors` |
| `gs_crawler/featured.py` | Homepage featured authors: profiled authors with the most-read Wikipedia articles → Firestore `v3_featured_authors/featured_gs` (and refreshes `v3_recent_authors/recent_gs` scores) |
| `gs_crawler/cache_invalidate.py` | Evicts Firestore `v3_author_*` cache entries after crawls or one-off schema migrations (`invalidate_missing_pip`) |
| `gs_crawler/main.py` | Job entry point: `CRAWL_MODE=refresh \| authors \| materialize \| deploy \| wikidata \| featured \| invalidate_legacy_pip` |
| `bigquery/gs_data/` | Snapshot tables, latest views (`authors_latest` leaves out `retired_profiles`: profiles Google Scholar no longer serves), one-off legacy seed |
| `bigquery/gs_statistics/` | GS versions of the non-temporal analytics views, PiP peer-standing tables (`dist_pip_thresholds`, `dist_pip_scores`, `pip_standing_json`), field groups (`author_*fields`), and field-relative percentiles (`dist_pip_auc_scores_field`; `field_*` columns on `ranked_author_pip_scores_current` and `ranked_author_current`) |

## Finding authors we have not crawled (Wikidata)

Google Scholar's author search needs a signed-in browser, so there is no way to list every profile
under a name. Wikidata maps ~130K people to their Google Scholar author ID (property P1960), which
puts them in the name index for free; names outside the index cost one SerpApi search each (see
"Name search" below). The monthly `wikidata` run:

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
4. Rebuilds the app's author name index and the featured authors (the weekly `materialize` run does
   both too).

## Name search

`author_search/search_service.py` serves the search box from an in-memory index of ~290K names
(profiled authors, Wikidata, co-authors). Three steps, each tried only when the one before found nothing:

1. **Exact.** Every query word must appear in the name (or match an initial at the same position);
   accents are ignored. The index keeps all names in one string, so a query looks only at the names
   containing its longest word: ~10 ms, against ~150 ms for the earlier scan of every name.
2. **Approximate.** Query words may differ from a word of the name by one typo (4-5 letters) or two
   (6 or more); initials in the query are ignored; the last word may be half-typed. Up to five
   suggestions, closest first, marked `"fuzzy": true` and shown under "Did you mean". Covers the
   misspellings seen in the request logs ("viral acharaya", "arun subdararajan", "robert f. engle").
   Uses `rapidfuzz`; ~50-100 ms.
3. **Live Google Scholar lookup** (`author_search/gs_search_client.py`), on a full search only (Enter or
   the search button, never typeahead). A regular Scholar search for the name through SerpApi
   (`engine=google_scholar`) returns a "User profiles for ..." block with IDs, affiliations and
   citation counts, and links each paper's authors to their profiles; both give GS IDs (checked
   2026-11-16; an earlier note here said SerpApi no longer returned these links). One paid search per
   lookup, from the same monthly plan as the crawler, so: results are cached for 7 days in
   `v3_gs_author_search` (also "no profile found"), lookups are capped at `GS_LIVE_SEARCH_DAILY_LIMIT`
   (25) per UTC day (`v3_gs_live_search_usage/<date>`), only name-like queries qualify, and requests
   with a crawler user agent are refused. `GS_LIVE_SEARCH=false` turns it off. Picking a result
   crawls the profile on demand, like a pasted URL.

**Index storage.** `refresh_author_index` saves the list as gzip-compressed JSON cut into parts of
at most 900 KB (`v3_gs_author_name_index/pack_<version>_<n>`, ~11 documents), then points
`v3_gs_author_name_index/meta` at the new version and deletes the previous parts. A frontend
instance starts loading it in a background thread at start-up (~1.5 s; the earlier format, 145
documents read one after another, took 16 s and made the first search on a new instance wait). Every
6 hours an instance compares its version with `meta` and downloads only if it changed, while the
old index keeps serving. Indexes saved in the earlier `chunk_<n>` format still load.

**Query log.** Full searches and profile views go to `v3_query_log` with `user_agent`, `ip_hash`,
`referrer` (other sites only), `bot` (user agent declares automation) and, for searches, `match`
(`exact`, `fuzzy`, `live`, `none`). `ip_hash` is HMAC-SHA256 of the address, keyed by the
`query-log-salt` secret (or `QUERY_LOG_SALT`); without the key no hash is stored. The Cloud Run request
logs carry the same facts for only 30 days. `/admin/query-log` and `/api/query-log` are public and
leave out the user agent, hash and referrer. Scrapers with browser user agents are not flagged by
`bot`; group by `ip_hash` to find them (many profiles within a minute, no searches). Profile views
from user agents flagged `bot` are logged here but do not enter the homepage "Recently Analyzed
Authors" list (`v3_recent_authors/recent_gs`): it only counts visits from non-bot user agents. A
crawler opening dozens of profiles within a second also made the transactions on that one document
abort each other (up to 161 failed updates a day in 2026-11); a failed update is now a warning.

**Featured authors.** The homepage pins the ten profiled authors whose English Wikipedia articles
were read most over the last 60 days; `/featured` lists the top 100. `gs_crawler/featured.py` ranks
`gs_data.wikidata_authors.enwiki_views_60d` among profiled authors, leaves out deceased people and
profiles with an h-index below 10 (stubs and people known for something else: in 2026-11, a George
Soros profile with h-index 1 and Rachael Gunn, the Olympic breakdancer, at 7), and stores each entry
with the recent-list fields plus the article title and views in `v3_featured_authors/featured_gs`.
It runs after the weekly percentile rebuild, so the scores match what the cache layer serves, and
after the monthly Wikidata reload; `CRAWL_MODE=featured` rebuilds it alone. An empty result keeps
the stored list. The featured authors' result pages are cached like any other: on first view, and
dropped when the profile is re-crawled or the percentile tables are rebuilt.

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

## Budget (SerpApi Big Data plan, 30,000 searches/month, since 2026-11-19)

- **Why the larger plan.** On 2026-11-19, 15,213 of the 16,356 reference profiles still carried their legacy
  snapshot: 8,241 from 2024-02 and 6,650 from 2025-04/05. A profile crawled today was compared with peers
  frozen 1.5 to 2.75 years earlier, so its percentiles were inflated, PiP most of all (see `TASKS.md`,
  "Stale reference profiles"). Re-crawling them takes ~44,000 searches; the Developer plan (5,000 a month,
  130 a day) would have needed about 11 months.
- **Refresh:** four runs a day (`gs-daily-refresh`, every 6 hours) of up to `MAX_SEARCHES_PER_RUN=600`. A search
  takes ~3.2 s, so a run lasts ~32 min; `RUN_TIME_LIMIT_SECONDS=2700` stops it from starting new profiles after
  45 min, because snapshots are loaded at the end and the job's 60 min task timeout would discard them. At
  2,400 searches a day a month's searches last about 12 days; runs then stop at `SERPAPI_SEARCH_RESERVE=1000`,
  which is kept for on-demand lookups, until the plan renews (the 2nd of the month).
- **New authors:** `NEW_AUTHOR_SHARE=0.10` of each run during the catch-up (~60 searches a run, ~60 authors a
  day); the code default is 0.25.
- **Recently crawled profiles are left alone:** the stalest-first tier only takes profiles last crawled more than
  `REFRESH_MIN_AGE_DAYS=30` ago. Without it, a budget larger than the backlog re-crawls week-old profiles.
- **Cost per author:** an average profile costs ~2.7 searches (218 works); the best-known Wikidata authors
  average ~4.
- **After the catch-up** (expected mid-December 2026): Production (15,000 a month) refreshes ~16K profiles
  every 6 months (~7,400 searches a month) with room for ~1,500 new authors a month at first; lower
  `MAX_SEARCHES_PER_RUN` to ~450 with one run a day. The refresh load grows by ~2.7 searches per cycle for
  every author added.
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
  (at most 2, or 5% of the cited papers) that sits before a gap of more than 10 years: GS profiles carry
  misdated entries and namesakes' papers decades before the real career (Pietro Perona: one 1963 item,
  then 1988 onward). This moves 739 of 16,207 profiles (2026-11-03). A 5-year gap was tried first and
  dropped real early work before PhD/MD training breaks (6 of 15 in a checked sample). Wrong cases are
  fixed by hand in `gs_data.start_year_overrides` (latest `set_at` wins; effective at the next dist
  rebuild); the first five were added from that sample.
- **No minimum.** There is no ≥6-publication filter, since the population is already curated.
- **Benchmark.** The `active_authors` benchmark is unchanged: hindex ≥ 3 and ≥ 3 publications.

## Switching the app to GS (done 2026-11-03)

`deploy-frontend.yml` deploys the frontend and cache layer with `DATA_SOURCE=gs` by default, which
mounts `serpapi-key` into the cache layer (on-demand crawls) and the frontend (live name lookup). Their
runtime account (`875626982900-compute@developer.gserviceaccount.com`) has
`roles/secretmanager.secretAccessor` on that secret. GS mode uses separate Firestore collections (`v3_gs_author_name_index`, `v3_gs_author_search`,
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
