# Google Scholar Data Sources: Vendor Evaluation (2026-10)

> Research for moving the data source back from Semantic Scholar to Google Scholar.
> Two research rounds on 2026-10-11/12 covered vendor docs, pricing pages, OpenAPI specs, public bug trackers,
> GitHub issues, court reporting and the bibliometrics literature. Prices are list prices; "~" marks values computed
> from plan tables rather than published per-1K rates.

## TL;DR

1. **Split the problem in two.** Core PiP-AUC (current paper percentiles + author percentiles) needs only
   **profile pages**: ~35–130K requests per refresh of ~15K researchers. The per-publication citations-per-year
   histograms, which are ~98% of the request volume (2–3M), feed only the *temporal* views.
2. **Vendor: SearchApi.io primary, SerpApi fallback**, behind a vendor-agnostic client with response validation.
   Both document profiles + per-publication histograms, bill only successful requests, and offer $2M legal indemnity.
   At the reduced volume in (3) the difference between them is ~$500 vs ~$1,500/month.
3. **Temporal histograms: no backfill needed.** The legacy `scholar_raw_data` tables hold real GS histograms for
   2.42M papers of 16.2K authors (snapshots from Feb 2024 – Sep 2025; see [Legacy data](#legacy-gs-data-in-bigquery-checked-2026-11-01)).
   Extend each one to today with the gap between its old and current total, split across the years since the
   snapshot in proportion to the author's yearly histogram.
   Only *cited* papers added since the snapshot need a `view_citation` fetch (~0.2–0.3M one-off, then ~7K/month).
   Validated offline on 148 authors over 15 months: per-paper cumulative citations have a 1.2% median error;
   temporal h-index is exact for 74% of authors and within ±1 for 99%. That is better than GS's own churn in
   un-refetched past years ([details](#validation-of-histogram-extension-run-2026-11-01)).
4. **Run a pilot first** (~$115 plus free trial credits) that also tests the two cheap options, Scrapingdog and
   Scrappa, which could be 5–10× cheaper if their per-publication histograms work.
5. **Author search by name is gone for everyone** (Google sign-in wall since ~May 2025). Discover GS IDs via
   profile URLs, paper-title lookups, and our own index of crawled authors.
6. **No licensed source replaces Google Scholar.** Scopus/WoS terms forbid public display of derived metrics, and
   all licensed or open alternatives have lower coverage than GS.

## What we need from Google Scholar

Derived from the retired crawler (`crawler/scholarly_client.py`, removed in 92b343c) and the pre-S2 BigQuery views.

| Req | GS page | Fields used | Volume per full refresh (~15K researchers) |
|-----|---------|-------------|-------|
| **R1** Author profile | `citations?user=ID` (≤100 pubs/page via `cstart`/`pagesize`) | name, affiliation, email domain, interests, citedby/hindex/i10 (all + since-year), author yearly histogram, coauthors, full pub list: title, authors, year, `num_citations`, `author_pub_id` | 43,385 pages at 100/page; 181,705 at 20/page (measured on legacy data) |
| **R2** Per-pub histogram | `citations?view_op=view_citation&citation_for_view=ID:PUB` | per-year citation counts (feeds `stats_publication_citations_temporal` and the temporal h-index / PiP views) | **2–3M** (one per publication) |
| **R3** Author search | `citations?view_op=search_authors` | user ID, name, affiliation | **Blocked**: redirects to Google sign-in since ~May 2025 |

State of the pages as of 2026-10-12 (plain `curl` from a GCP IP, no cookies, no JS): profile and `view_citation`
pages return full static HTML. Measured sizes are ~225 KB / 49 KB gzip for a profile and ~95 KB / 26 KB gzip for
`view_citation`. The selectors that `scholarly` used (`gsc_a_tr`, `gsc_rsb_std`, `gsc_g_a`, `gsc_oci_g_a`) are unchanged.

## Option landscape

### 1. Managed Scholar APIs with structured JSON

Vendors that document both R1 and R2:

| Vendor | R1 | R2 | Published $/1K | Failures billed? | Track record / risk |
|--------|----|----|---------------|------------------|---------------------|
| **SerpApi** | `engine=google_scholar_author`, `num` ≤100, `start` | `view_op=view_citation` → `citation.total_citations.table[{year,citations}]` | $7.25 @100K · $3.75 @1M · $2.53 @3M · floor $1.96 @54M | No (errors + 1h cache hits free) | Since 2017; public bug tracker. Scholar incidents: ~1-month author-endpoint outage when Google added the login wall (May–Jun 2025, closed "won't fix" for author search); **34-day bug (Apr–May 2026) returning empty results with no error on ~5–10% of author calls**, which retry logic didn't catch and which were probably billed; wrong-year histogram bug (Mar 2025, fixed in 2 days). Self-reported 99.99% success. Python SDK. $2M Legal Shield. |
| **SearchApi.io** | `engine=google_scholar_author`, `page` only: **no `num`, default 20 articles/page** (OpenAPI spec; sample `request_url` has no `pagesize`) | `view_op=view_citation` → `citation.cites_histogram[{year,cites}]`; works with `citation_id` alone | $2.50 @100K · $1.50 @1M · $1.40 @2M · **$1.00 @5M** · custom above | No (only HTTP 200 billed) | Smaller, no public bug tracker, no independent Scholar reports found. Hourly cap 20% of plan credits. $2M Legal Protection Guarantee. No official SDK. |
| **Scrappa** | `/api/google/scholar/author`, `num` 1–100, **`start` capped at 990** (may truncate lists >1,090 pubs) | `/author/citation` returns `graph[{year,citations}]` | $0.30 @33K · $0.25 @1M · $0.20 @5M (prepaid packs, 12-month validity) | No | Marin Solutions UG (Berlin, HRB 237602 B), apparently a solo founder; domain registered 2024-06-12; ~23 Trustpilot reviews. Probably scrapes on its own (pricing is below every structured API it could resell). |
| **Scrapingdog** | `/google_scholar/author`, `results` param, offset undocumented | **Unverified**: the doc sample for `view_citation` shows author-level h-index/i10, i.e. looks copy-pasted from the author page | ~$0.33–0.35 at 1–3M (5 credits/call) | No | Proxyway 2024 Google benchmark: 25% success. Recent Trustpilot complaints about failures and refunds. Its "best Scholar API" rankings are self-authored. |
| Shifter.io | SerpApi-style author engine | Claimed, no sample | $4.50 @100K, custom above | No | Too expensive. |

Vendors that **do not** cover R1+R2:
- **Scholar keyword search only:**
  - **Oxylabs `google_scholar`** (parsed since 2026-07-22; `render: html` mandatory, so likely billed at the Google JS rate of $0.90–1.35/1K)
  - HasData
  - Serper.dev
  - Traject (Scale SERP / SerpWow)
  - Thordata
  - Scrapeless (current docs)
- **No Scholar product:** DataForSEO, Decodo, Zenserp, Outscraper, ValueSERP, SerpHouse, ScraperAPI, Crawlbase, ZenRows, Nimble, Piloterr, Scrapfly, Firecrawl, NetNut, SOAX.
- **Not Scholar data:** Serply's `/v1/scholar` returns OpenAlex records.
- **Bright Data "Google Scholar Scraper":** an AI scraper builder, not a maintained parser ($1.30–1.50/1K).
- **ScrapeBadger:** profiles but no R2.
- **Apify actors:** $14–20/1K, single developers. Not viable.

No vendor sells a pre-collected Google Scholar dataset.

### 2. Our own parser + rented unblocking

Both page types are static HTML, so non-rendered tiers apply. Parsing is the easy part: `scholarly`'s histogram
logic still matches today's markup, and `pygscholar` (v0.4.0, 2026-09-30) has current selectors for profiles.
Pitfall: on `view_citation` pages, years with zero citations have no bar. Take the year from each bar's
`as_ylo=` link, not from position. Both `scholarly` (#184) and SerpApi (#259) got this wrong at some point.

| Route | ~$/1K successful pages @1M–3M/mo | Notes |
|-------|----------------------------------|-------|
| **Oxylabs** `source=google` + `url` (raw HTML) | ~$0.50–0.70 (probably the Google non-JS row; not confirmed in docs) | Their own docs use a scholar.google.com URL as the example. **2xx and 4xx are billed** (404s for removed papers cost money; 429 doesn't). Domains under 40% success are throttled to 1 req/s. 2K-result free trial. |
| Zyte API (HTTP) | ~$0.21–0.61 (Scholar tier unknown) | |
| Decodo `/scrape` premium | ~$0.50–0.55 | Success-only billing |
| ScrapeOps aggregator | ~$0.60–0.70 (10 credits for Google subdomains) | Fails over across ~20 providers |
| Scrape.do (`super=true` required for Scholar) | ~$0.60–0.80 (credit multiplier unverified) | |
| Bright Data Web Unlocker | ~$1.30 | |
| ScraperAPI (25 credits for Google) | ~$1.5–2.4 | What the old crawler used |
| Raw residential proxies ($2–5/GB) | ~$0.07–0.18 | We own CAPTCHA detection (`/sorry/` redirect, `form#gs_captcha_f`, 200 without `#gsc_prf_in`), retries and IP rotation |

No published, credible per-IP rate limit exists for Scholar. Anecdotes range from ~300 to ~1,200 requests per IP
per day, and blocks last from a few hours to ~24h. A GCP IP was not blocked at a handful of requests.

### 3. Libraries

| Library | Status (2026-10) |
|---------|------------------|
| `scholarly` | Last release 2023-01-16. 2025 breakages: author search redirects to sign-in (#569, #574); httpx 0.28 broke proxies (#561, #572); Python 3.13 proxy failure (#577). Maintainer is looking for help (#570). |
| `scholarly2` (ma-ji) | v2.0.0 (2026-03-06), ID-only, SOCKS5 residential proxies only, single maintainer. |
| `pygscholar` | v0.4.0 (2026-09-30). Profile parser on top of `scholarly`'s fetcher. No histogram parsing. |
| `google-scholar-py`, `scholar.py`, `gscholar` | Unmaintained or archived. |

### 4. Licensed and open alternatives (not Google Scholar)

| Source | Per-paper yearly citations | Coverage vs GS | May we display derived metrics publicly? |
|--------|---------------------------|----------------|----------------------------------------|
| Scopus APIs | Yes (Citation Overview, approval required, 20K/week) | GS finds 93–96% of citations, Scopus 35–77% (Martín-Martín 2018) | **No.** Academic use forbids "display of Scopus data on a website"; repository use forbids aggregated cited-by "for external display". ICSR Lab closed 2023-12-31. |
| Web of Science APIs | Totals only (Starter) | 27–73% | No (subscription-bound use) |
| Dimensions (SRAD program) | Yes | 54% vs GS 88% (Martín-Martín 2021) | Non-commercial research projects only; poor fit for a public tool |
| Lens.org | Yes | Similar to OpenAlex | Yes for non-commercial use, with attribution (CC BY-NC-SA spirit) |
| OpenAlex | `counts_by_year` covers the last ~10 years only; full history from the snapshot | ~47% of GS citations for Hinton (spot check); author-disambiguation rewrite promised for Q1 2026, not shipped as of 2026-10 | Yes (CC0) |
| OpenCitations / Crossref | Yes / totals only | ~28% (COCI) | Yes (CC0) |

None of these solves the problems that made us leave S2: lower counts and disambiguation noise. OpenAlex is
useful as a secondary source for matching papers.

## Reducing R2 (per-publication histograms)

No GS page returns per-paper per-year counts in bulk. Export formats are bibliographic only, and year-filtered
`scholar?cites=` queries cost one request per paper-year. So the options are listed below.

> **Update 2026-11-01:** option 4 (legacy seed + extension) is validated and is the plan for the reference
> population. Options 1–2 remain relevant only for authors with no legacy data, such as new on-demand lookups.

1. **Imputation by raking (IPF).** Seed a paper × year matrix from `s2_data.paper_citations_by_year`. Rake it so
   row sums match each paper's GS `num_citations` and column sums match the author's GS yearly histogram. Both
   targets come free with the profile page.
   - No study directly validates cross-database yearly shares, but article-level totals correlate at r ≈ 0.8–0.99
     across databases (Moed 2016, Martín-Martín 2018).
   - Expected bias: GS indexes earlier and includes more grey literature, so imputation will likely under-allocate
     the most recent 1–2 years.
   - The author histogram equals the sum of per-paper histograms (verified on legacy data), so the column
     constraint is exact.
2. **Selective real fetches.** Fetch `view_citation` only for:
   - papers that are or were in the h-core (they drive temporal h-index);
   - papers with no S2 match or an extreme GS/S2 citation ratio;
   - a 1–2% random sample to monitor imputation error on the metrics we display.
   - Expected 2–3× or larger reduction in R2 volume; measure it.
3. **Monthly snapshots.** Profile-page deltas give exact per-year growth from now on. Re-fetch `view_citation`
   only when deltas are negative or jumpy (GS merges or re-clustering) or for new papers with existing citations.
4. **Legacy seed.** The legacy tables cover nearly the whole reference population (next section), which makes
   option 1 a narrow problem. Instead of imputing whole histograms from S2, we only need to split each paper's growth
   since its snapshot across the years after it, under an exact per-author, per-year constraint.

## Legacy GS data in BigQuery (checked 2026-11-01)

| Table | Rows | Size | Notes |
|-------|------|------|-------|
| `scholar_raw_data.author` | 27,795 | 1.3 GB | All author snapshots (raw JSON) |
| `scholar_raw_data.author_latest_table` | 16,207 authors | 1.5 GB | Parsed: metrics, `publications` array, coauthors, raw `data` with author `cites_per_year` |
| `scholar_raw_data.pub` | 4,115,979 | 5.5 GB | All publication snapshots (raw JSON) |
| `scholar_raw_data.pub_latest_table` | 3,447,496 pubs (16,171 authors) | 5.6 GB | Parsed: `num_citations`, `pub_year`, `cites_per_year` |

**Coverage**
- Author profiles list 3.48M publications, and 3.43M of them were fetched (98.6%).
- 15,720 of the 16,207 authors (97%) have at least 95% of their publications fetched.
- 123 authors have no publications fetched.
- Of 2.42M publications with at least one citation, 2,418,747 (99.9%) have a non-empty histogram.

**Freshness**

| When fetched | Publications | Authors |
|--------------|--------------|---------|
| Feb 2024 | 2.06M | 13.3K |
| Apr–May 2025 | 1.29M | 6.7K (overlaps the Feb 2024 set) |
| Other months, through Sep 2025 | ~0.1M | — |

The last fetch was 2026-03 (a single author).

**Internal consistency**
- Per-paper histograms sum to the paper's `num_citations` within 5% for 97.3% of papers, and exactly for 65%. The mean ratio is 0.985.
- The author yearly histogram equals the sum of that author's per-paper histograms. This was checked on 114K author-years (13.8K authors, 2015–2023, at least 20 citations, snapshots taken within 3 days of each other): the median ratio is 1.000 and the 10th–90th percentiles are 1.000–1.001.
- GS's author histogram is therefore a hard column constraint for splitting the growth since each snapshot.

**Publication-list size**
- Average 215 publications per author (deciles: 25, 42, 63, 89, 123, 166, 226, 322, 505).
- 382 authors have at least 1,000 publications.
- 2 authors sit at exactly 3,000, likely a cap in the old crawler.
- One full profile refresh is **43,385 pages at 100 per page, or 181,705 at 20 per page** (SearchApi).

**How often citation counts change** (consecutive snapshots of the same publication in `scholar_raw_data.pub`)

| Gap between snapshots | Pairs | Median gap | Changed | Increased | Decreased |
|-----------------------|-------|-----------|---------|-----------|-----------|
| < 45 days | 192,614 | 12 days | 15.4% | 13.7% | 1.6% |
| 45–120 days | 10,619 | 55 days | 30.9% | 28.8% | 2.1% |
| > 400 days | 39,687 | 462 days | 57.0% | 51.6% | 5.4% |

About a quarter of these papers have zero citations in both snapshots. The authors that were re-fetched may be
more active than average, so treat the rates as an upper bound. Even so, roughly **25–30% of papers change in a
month**, about twice the 15% assumed earlier. Re-fetching every changed histogram monthly (scenario C) would mean
~0.9M requests/month, which makes the legacy-seed approach more attractive.

## Validation of histogram extension (run 2026-11-01)

Script: `scripts/gs_histogram_validation/` (two export queries plus `validate.py`). It uses no GS requests.

**Setup**
- 148 authors with two profile snapshots: Feb 2024 (t0) and May 2025 (t1), a median gap of 463 days.
- Both snapshots have at least 90% of publications with histograms within 5 days of the profile.
- 22,695 publications appear in both snapshots. 2,281 publications were added between t0 and t1, and they are
  treated as fetched, as they would be in production.
- Mean h-index is 38. These are authors who were re-crawled, so possibly more active than average.
- Inputs at t1 are exactly what a fresh profile page gives: each paper's total and the author's yearly histogram.
  The true t1 per-paper histograms are held out for scoring.

**Estimators**

| | What it does |
|---|---|
| E0 | Stale: keep the t0 histograms unchanged |
| E1 | Split each paper's growth (t1 total − t0 total) across 2024–2025 by elapsed time |
| E2 | As E1, then rake toward the author's yearly residuals (paper totals exact; see finding 3) |
| E3 | As E2, seeded with each paper's own recent trend |
| E4 | As E2, but also places the author's net backfill into pre-2024 years. Dropped: worse than E2 everywhere |

Growth must be computed from GS *totals*: t1 total − t0 total, not t1 total − t0 histogram sum. GS totals include
citations with no year, which never appear in the histogram. The first version of this analysis used the histogram
sum and so kept re-adding that undated gap as fake growth. Fixing it cut allocation error from 0.43 to 0.33.

Scoring covers only papers already published by the scored year, matching production temporal stats, which start
at `pub_year`.

**Results**

| Metric | Through year | E0 stale | E1 | **E2** | E3 | Noise floor: years before t0, not re-fetched |
|--------|------|----------|----|--------|----|---------------------------------------------|
| Paper cumulative citations, median rel. error (papers ≥10 cites) | 2024 | 6.5% | 1.2% | **1.2%** | 1.2% | 2.3% (through 2023) |
| Same, share within 5% | 2024 | 41% | 83% | **84%** | 82% | 74% (through 2023) |
| Same, share within 5% | 2025 (= t1) | 34% | 94% | **94%** | 94% | — |
| Paper percentile in pub-year cohort, mean / p95 abs error (pts) | 2024 | 2.1 / 7.1 | 0.75 / 3.0 | **0.74 / 2.9** | 0.76 / 3.0 | 1.1 / 3.5 (through 2023) |
| Same | 2025 | 2.4 / 9.0 | 0.29 / 0.97 | **0.29 / 0.97** | 0.29 / 0.97 | — |
| Author h-index exact / within ±1 | 2024 | 26% / 64% | 74% / 98% | **74% / 99%** | 72% / 99% | 57% / 94% (through 2023) |
| Same | 2025 | 22% / 53% | 84% / 97% | **84% / 97%** | 84% / 97% | — |
| Author yearly total vs GS author chart, weighted / median error | 2024 (single year) | 88% / 89% | 14% / 10% | **14% / 9%** | 14% / 9% | — |
| Same | 2025 (single year) | 95% / 98% | 16% / 14% | **13% / 9%** | 13% / 8% | — |

**Findings**
1. **Extension works.** It cuts per-paper error 5× against stale histograms. Cumulative metrics in the
   extended years end up *more* accurate than the old years are after 15 months of churn: the 2024 numbers beat
   the "through 2023" noise floor.
2. **GS reshuffles past years.** 35% of the absolute per-paper change between snapshots fell in years before
   2024, but positive and negative changes largely cancel. In 2020, for example, papers gained +3,811 and lost
   −2,274, for an author-level net of +1,298. This looks like citations moving between merged versions.
   Net backfill into past years is ~7% of growth. No estimator can predict it per paper, and E4's attempt made
   things worse. Only a re-fetch corrects it, and its effect is already the "noise floor" column above.
3. **E2 does not match the author's yearly totals exactly.** The per-year error is a median of 9%.
   - `ipf()` keeps paper totals exact. It uses the author's yearly residuals only as *shares*, rescaled to the summed
     paper growth, and clips negative residuals.
   - Summed growth differs from the window residuals by the net backfill into past years (finding 2) and by
     decreases clipped to zero. Cells are then rounded.
   - Single-year totals are therefore off by ~9% (median), while cumulative totals, which drive h-index and
     percentiles, are within ~1%.
   - The true per-paper histograms do sum to the author chart (median gap 0%, p90 ~5%), so the gap is in the
     estimator, not the data.
   - If yearly author totals are displayed directly, take them from the GS author chart rather than summing estimates.
4. **The author constraint adds little over a pure time split** (E2 ≈ E1, slightly better on yearly totals).
   A per-paper trend seed (E3) is no better.
5. **Use E2 in production**, with growth taken from GS totals. Optional: an annual `view_citation` re-fetch of
   h-core papers to clear the churn where it matters most.

**Cost inputs measured**
- 9.0% of a t1 publication list was new since t0 (15 months), but only 35% of those had any citations.
  Uncited papers have empty histograms and need no fetch.
- 5.5% of papers had a lower total at t1 than at t0. They are included in the results above without a re-fetch
  (growth clipped to zero), so re-fetching them is optional.

**Limitations**
- The gap covers 2 calendar years. Production gaps are 18 months (Apr 2025 snapshots) to 33 months (Feb 2024),
  covering 3 calendar years. The per-paper split across a middle year is untested.
- One author cohort and one gap. The authors may be more active than the population.

## Cost scenarios

Assumptions, from the legacy data: 16.2K researchers, an average of 215 pubs, one profile refresh = 43,385 pages at
100/page or 181,705 at 20/page (SearchApi), and ~25–30% of papers changing per month. Monthly profile refresh; on-demand
lookups not included. Costs below are monthly unless marked one-off.

| Scenario | Requests | SerpApi | SearchApi.io | Scrappa | Scrapingdog |
|----------|----------|---------|--------------|---------|-------------|
| **A. Profiles only** (core PiP-AUC) | ~43K/month (~182K at 20/page) | ~$725 (100K plan) | ~$500 (250K plan) | ~$11 | ~$90 |
| **B. A + histogram fetches for newly added, cited papers** (legacy histograms extended with E2; ~7K/month, plus an optional ~1% validation sample) | ~50–90K/month (~190–230K) | ~$725 (100K plan) | ~$500 (250K plan) | ~$15–25 | ~$90 |
| **C. A + re-fetch every changed histogram** | ~0.95M/month (~1.08M) | ~$3,750 | ~$1,500–2,800 | ~$250 | ~$350 |
| One-off: histograms for cited papers added since the legacy snapshots (~0.55M new papers, ~0.2–0.3M cited; extrapolated from the validation) | one-off | ~$1,475–2,750 | ~$500–900 | ~$50–75 | ~$90–200 |
| ~~One-off full R2 backfill (~3M)~~ | not needed | — | — | — | — |

DIY with Oxylabs or residential proxies costs roughly between the Scrappa and Scrapingdog columns, plus engineering
and block-handling time.

## Recommendation

1. **Build a vendor-agnostic `ScholarClient`** with `get_author(id, page)` and `get_citation(id, pub_id)`.
   - Store raw responses in GCS and normalize in BigQuery, so a vendor switch doesn't touch the views.
   - **Validate every response**, because a 200 with no data is a known failure mode (SerpApi #3708). Check that:
     - the profile has a name and metrics;
     - the article count across pages matches what the profile claims;
     - a histogram sums to roughly `num_citations`;
     - years come from the data, not from position.
   - Retry failures on the secondary vendor.
2. **Primary: SearchApi.io. Fallback and canary: SerpApi.** They have the same coverage, success-only billing and
   indemnity. SearchApi is ~2–3× cheaper even after its 20-per-page profile pagination. SerpApi has the better-documented
   track record and 100-per-page profiles, so it also serves as a cross-check on a sample.
3. **Ship scenario A first** (profiles → current PiP-AUC), then add temporal metrics by extending the legacy histograms (scenario B).
4. **Re-evaluate the cheap tier after the pilot.** If Scrapingdog's or Scrappa's histograms prove correct, they can
   take the bulk R2 fetches at a fraction of the cost, with SearchApi as the fallback. Scrappa's counterparty risk
   (solo founder, 2-year-old company) argues against making it primary.

## R3: author discovery without name search

1. **Accept GS profile URLs/IDs directly** in the search box.
2. **Paper-title lookup:** a Scholar keyword search (supported by every vendor above, including Oxylabs' parsed
   source) for one of the author's known titles returns `publication_info.authors[].author_id`. We have titles from S2.
3. **Index of GS authors we have crawled** (reference population + past lookups), replacing the current S2 index.
4. The coauthor lists on crawled profiles expand that index cheaply.

## Legal and ToS context

- Google's ToS prohibit automated querying. `scholar.google.com/robots.txt` allows `/citations?user=`, but disallows:
  - `cstart` pagination;
  - the rest of `/citations?`, including `view_citation`;
  - `/scholar`.
  Reducing R2 volume also reduces the robots-disallowed share.
- **Google v. SerpApi** (N.D. Cal. 4:25-cv-10826, DMCA §1201 over SearchGuard): claims dismissed 2026-07-20, partly
  without leave to amend. Google filed an amended complaint ~2026-08-10, and SerpApi's renewed motion to dismiss is pending.
  Scholar pages have no SearchGuard JS, so the theory fits Scholar less well.
- **Reddit v. SerpApi, Oxylabs, et al.** (S.D.N.Y. 25-cv-08736): motions to dismiss largely denied 2026-07-31.
- Vendors carry the operational exposure, and SerpApi/SearchApi indemnify up to $2M. The ToS/robots position still applies
  to us as the party commissioning the data.

## Pilot plan

Budget: ~$115 (SearchApi Developer $40/10K, SerpApi Developer $75/5K) plus free credits at Scrapingdog (100 credits),
Scrappa ($10 starter pack) and Oxylabs (2K results).

1. Pick ~20 authors from the old GS reference set, including at least 3 with >1,000 publications (one >2,000).
2. Fetch full profiles and all `view_citation` pages through SearchApi and SerpApi. Fetch a small subset through
   Scrapingdog, Scrappa and Oxylabs raw HTML.
3. Measure:
   - SearchApi articles per page;
   - pagination completeness for prolific authors (including Scrappa's `start` ≤990 cap);
   - histogram correctness against a direct fetch (zero-citation years, year alignment);
   - Scrapingdog `view_citation` output;
   - empty-200 rate;
   - p50/p95 latency;
   - wall-clock time for one prolific author (on-demand target: 1–2 min).
4. Check that legacy GS IDs and `author_pub_id`s still resolve, and how many papers have been added since each author's snapshot (sets the catch-up cost).
5. Already answered from legacy data, no GS requests needed:
   - the author histogram equals the sum of per-paper histograms;
   - ~25–30% of papers change per month;
   - histogram extension (E2) is validated (see above).
7. Ask SearchApi and Oxylabs for written quotes for a ~300K–3M/month Scholar workload.

## Sources

- **Vendor docs, pricing, specs:**
  - SerpApi: serpapi.com/google-scholar-author-api, /google-scholar-author-cited-by, /pricing, /status/google_scholar_author; github.com/serpapi/public-roadmap issues #259, #653, #2515, #2694, #2787, #3142, #3708
  - SearchApi.io: searchapi.io/docs/google-scholar-author, /openapi/google_scholar_author.yaml, /pricing
  - Scrappa: scrappa.co/docs, /pricing, /legal/impressum
  - Scrapingdog: scrapingdog.com/documentation/google-scholar-author-citation-api, /pricing
  - Shifter.io: shifter.io/docs (google-scholar-author-api)
  - Oxylabs: oxylabs.io/products/web-api/scrape/serp/google/scholar, /products/scraper-api/web/pricing; developers.oxylabs.io (google/scholar, google/url, billing-information, rate-limits)
  - Bright Data: brightdata.com/products/web-scraper/google-scholar-scraper, /pricing/web-unlocker
  - Others: decodo.com/scraping/web/pricing, scrape.do/blog/google-scholar-scraping, zyte.com/pricing, scrapeops.io/proxy-api-aggregator, docs.trajectdata.com, serply.io/docs, hasdata.com/apis/google-scholar-api, serper.dev, doc.thordata.com
- **Benchmarks and reviews:** affiliate-funded Google Search (not Scholar) benchmarks at proxyway.com/research (2024, 2025-12); trustpilot.com (SerpApi, Scrapingdog, Scrappa). Vendor-authored comparison posts were not relied on.
- **Libraries and field reports:**
  - github.com/scholarly-python-package/scholarly (issues #184, #508, #557, #561, #569, #570, #572, #574, #577, #578)
  - github.com/ma-ji/scholarly2; pygscholar (PyPI)
  - github.com/ChenLiu-1996/CitationMap issues #16, #30, #32
  - harzing.com Publish or Perish manual; forums.zotero.org; forum.posit.co
  - docs.searxng.org (google_scholar engine)
- **Licensed sources:** dev.elsevier.com/policy.html, /academic_research_scopus.html, /api_key_settings.html; developer.clarivate.com/apis; digital-science.com (SRAD, 2025-07-14); about.lens.org/policies; help.openalex.org; blog.openalex.org; download.opencitations.net.
- **Literature:**
  - Martín-Martín et al., J. Informetrics 12(4) 2018 (arXiv:1808.05053) and Scientometrics 126 2021 (arXiv:2004.14329)
  - Moed, Bar-Ilan & Halevi, J. Informetrics 10(2) 2016
  - Harzing & Alakangas, Scientometrics 106 2016
  - Delgado-Quirós & Ortega, J. Informetrics 19(1) 2025
  - Gusenbauer, Res. Synth. Methods 15 2024
  - Culbert et al., Scientometrics 130(4) 2025
  - Thelwall & Jiang, JASIST 2025
  - Ibrahim et al., arXiv:2402.04607
- **Legal:** searchenginejournal.com/court-dismisses-googles-dmca-claims-against-serpapi/583033; serpapi.com/blog (party-authored); blog.google; law360.com/articles/2508228; loeb.com (Reddit v. SerpApi, 2026-08).
