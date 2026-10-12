# Google Scholar Data Sources: Vendor Evaluation (2026-10)

> Research for moving the data source back from Semantic Scholar to Google Scholar.
> Vendor docs, pricing pages, GitHub repos and court reporting were checked on 2026-10-11/12.
> Prices are list prices; "~" marks values computed from plan tables rather than published per-1K rates.

## TL;DR

- **Recommendation: SearchApi.io as the primary provider, SerpApi as the fallback**, both behind a thin
  client interface (their Scholar schemas are nearly identical). Confirm with a ~$115 pilot before committing (see [Pilot plan](#pilot-plan)).
- Only three vendors document structured output for **both** the author profile and the per-publication
  citations-per-year histogram: **SerpApi, SearchApi.io, Scrappa**. Everyone else either covers keyword search only
  or would hand us raw HTML to parse ourselves.
- **Author search by name is gone for everyone.** Since ~April–June 2025, `citations?view_op=search_authors`
  redirects anonymous clients to Google sign-in. SerpApi discontinued its Profiles API (2025-06-23); `scholarly`
  broke the same way (issue #569). We need another way to find GS user IDs (see [Author discovery](#r3-author-discovery-without-name-search)).
- **Profile pages and `view_citation` pages are still public and not JS-gated.** Plain `curl` from a datacenter IP
  on 2026-10-12 returned full HTML with the per-year histogram (5 requests; not a rate-limit test).
- **The biggest cost lever is ours, not the vendor's:** re-fetch a publication's histogram only when its
  `num_citations` on the profile page changed since the last crawl. This should cut steady-state volume several-fold.
- `scholarly` is effectively unmaintained (last PyPI release 1.7.11, 2023-01-16; maintainer looking for help, May 2025).
  Going back to it is not an option.

## What we need from Google Scholar

Derived from the retired crawler (`crawler/scholarly_client.py`, removed in 92b343c) and the pre-S2 BigQuery views.

| Req | GS page | Fields used | Volume per full refresh (~15K researchers) |
|-----|---------|-------------|-------|
| **R1** Author profile | `citations?user=ID` (100 pubs/page via `cstart`/`pagesize`) | name, affiliation, email domain, interests, citedby/hindex/i10 (all + since-year), author `cites_per_year`, coauthors, full pub list: title, authors, year, `num_citations`, `author_pub_id` | 30–50K requests (prolific authors need 20–30 pages) |
| **R2** Per-pub histogram | `citations?view_op=view_citation&citation_for_view=ID:PUB` | per-year citation counts (feeds `stats_publication_citations_temporal` and all temporal metrics) | **2–3M requests** (one per publication) |
| **R3** Author search | `citations?view_op=search_authors&mauthors=NAME` | user ID, name, affiliation, citations | small; on-demand only |

R2 is ~98% of the request volume and drives the cost of every option.

## Option landscape

### 1. Managed Scholar APIs with structured JSON

| Vendor | R1 | R2 | R3 | $/1K @100K | @1M | @3M | Failures billed? | Notes |
|--------|----|----|----|-----------|-----|-----|------------------|-------|
| **SerpApi** | Yes: `engine=google_scholar_author`, `num` ≤100, `start` | **Yes:** `view_op=view_citation` → `citation.total_citations.table[{year,citations}]` | Discontinued | $7.25 | $3.75 | ~$2.53 | No (errors + 1h cache hits free) | Self-reported 30-day status: 99.986% success, 2.1s avg for `google_scholar_author`. Throughput capped per hour (110–130K/h on Cloud tiers). $2M Legal Shield. Python SDK `serpapi` 1.1.2 (2026-09). Operating since 2017. |
| **SearchApi.io** | Yes: `engine=google_scholar_author`, `page`; **page size undocumented** (example suggests 20) | **Yes:** `view_op=view_citation` → `citation.cites_histogram[{year,cites}]` | No | $2.50 | $1.50 | ~$1.00–1.67 (Octo 5M = $5K; custom quotes) | No | Rate limit: 20% of plan credits per hour. $2M Legal Protection Guarantee. No official Python SDK (plain `requests`). No Scholar-specific reliability data published. |
| **Scrappa** (new, 2025–26) | Yes: `num` 1–100, `start` | **Yes:** `/author/citation` "annual citation graph" | No | ~$0.30 | $0.25 | ~$0.23 | No | Cheapest full-coverage option. Unknown company age/ownership, no SLA, legal program, or track record. |
| **Scrapingdog** | Partial: `results` param, offset/max undocumented | **Unverified** (doc example shows only the author-level graph) | `/profiles` documented but likely hits login wall | $0.90 | $0.35 | ~$0.33 | No | 5 credits/Scholar call. Scored 25% success on Google in Proxyway's 2024 benchmark (vendor's own blog claims 100%). |
| ScrapeBadger | Yes (`pagesize` ≤100) | No (author-level chart only) | IDs only | ~$1.05 | n/p | n/p | No | |
| Apify actors | Some | Only one actor | Blocked | $14–20 | — | — | Varies | ~$44–60K per full refresh. Single-developer actors. Not viable. |
| HasData, Serper.dev | No | No | No | — | — | — | — | Scholar keyword search only. |
| Oxylabs `google_scholar`, Traject (Scale SERP/SerpWow) | No | No | No | — | — | — | — | Parsed Scholar keyword search only. |
| Bright Data, Decodo, DataForSEO, Zenserp, Outscraper, ValueSERP | No | No | No | — | — | — | — | No Scholar profile/citation product. Bright Data's "Google Scholar Scraper" page is a generic AI-scraper template. |
| Serply `/v1/scholar` | — | — | — | — | — | — | — | Output uses OpenAlex IDs and links to `api.openalex.org`: not Google Scholar data. |

No vendor sells a pre-collected Google Scholar profile/publication dataset, and there is no official or licensed
Scholar API (Scholar Labs, launched 2025-11-18, is a signed-in AI search mode with no API).

### 2. Our own parser + rented unblocking

Both page types are static HTML (no JS rendering needed), so the cheapest tiers apply. Measured page weight
(2026-10-12): profile 225 KB decoded / 49 KB gzip; `view_citation` 93–99 KB decoded / 26 KB gzip.

| Route | Effective $/1K successful pages @1M–3M/mo | Notes |
|-------|------------------------------------------|-------|
| Zyte API (HTTP) | ~$0.21–0.61 (Scholar tier unknown) | 3,000 req/min standard |
| Oxylabs Web Unblocker (per GB) / Web Scraper API `source=google` + `url` | ~$0.11–0.48 / ~$0.50–0.70 | 2xx/4xx billed; domains <40% success get throttled to 1 req/s |
| ScrapeOps aggregator (10 credits for Google subdomains) | ~$0.60–0.70 | Fails over across ~20 providers |
| Decodo `/scrape` premium | ~$0.50–0.55 | Success-only billing |
| Crawlbase | ~$0.51–1.05 | |
| Bright Data Web Unlocker | ~$1.30 | Docs say to use SERP API for Google; Scholar URLs untested |
| ScraperAPI (25 credits for Google) | ~$1.5–2.4 | What the old crawler used as a fallback |
| Raw residential proxies ($2–5/GB) | ~$0.07–0.18 | We run CAPTCHA detection, retries and IP rotation ourselves; KYC needed for Google at Oxylabs/Bright Data |

There is no independent success-rate benchmark for scholar.google.com specifically. Proxyway's Google Search tests
(2024, 2025) put Bright Data, Decodo, Oxylabs and Zyte at 99.5–100%. Treat these as weak evidence for Scholar.
Known rate limit without proxies: Publish or Perish warns at 120–150 requests/hour per client, and blocks last 1–2 days.

### 3. Libraries

| Library | Status (2026-10) |
|---------|------------------|
| `scholarly` | Last release 2023-01-16. 2025 breakages: author search → sign-in (#569, #574), httpx 0.28 broke proxies (#561, #572), Python 3.13 proxy failure (#577). Maintainer: "proxy services have also become less effective" (#570). |
| `scholarly2` (ma-ji) | v2.0.0 (2026-03-06), ID-only, SOCKS5 residential proxies only, single maintainer, 2 stars. |
| `pygscholar` | Active CLI on top of `scholarly`. |
| `google-scholar-py`, `scholar.py`, `gscholar` | Unmaintained or archived. |

### 4. Not Google Scholar

OpenAlex has the same weaknesses that led us away from Semantic Scholar: lower counts and disambiguation noise.
A spot check of Hinton gave 47% of GS citations (h 147 vs 195), and the planned OpenAlex author-disambiguation
rewrite had still not shipped as of 2026-08.

## Cost scenarios

Assumptions (to be validated in the pilot):
- **Backfill**: one full crawl ≈ 3.0M requests (50K profile + ~2.95M detail).
- **Steady state**: ≈ 500K requests/month = all profiles monthly (50K) + histograms only for publications whose
  `num_citations` changed (~450K; unmeasured guess of ~15% of papers per month).

| Option | Backfill (one-off) | Steady state / month | Main risk |
|--------|-------------------|----------------------|-----------|
| SerpApi | ~$7,600 (Cloud 3M) | ~$2,750 (500K plan) | Price |
| **SearchApi.io** | **~$4,300–5,000** | **~$900–1,500** (upper end if profiles are 20/page) | Less Scholar track record than SerpApi |
| Scrappa | ~$750 | ~$125 | Counterparty risk |
| Scrapingdog | ~$1,000 | ~$200 | R2 unverified; weak Google benchmark |
| Own parser + Zyte/Decodo/ScrapeOps | ~$1,000–2,000 | ~$150–350 | We own parsing, block detection and retries again |
| Own parser + residential proxies | ~$300–550 | ~$50–100 | Same as above, plus IP-rotation logic |

Other ways to cut volume: refresh the reference population quarterly instead of monthly; fetch R2 only for
publications above a citation floor (papers with 0–2 citations barely move the percentiles). The legacy
`scholar_raw_data.pub_latest_table` may still hold per-publication histograms from the old crawler, which could
seed the backfill. I have not verified its contents or freshness.

## Recommendation

**SearchApi.io as the primary provider, with SerpApi as the fallback.**

Why a managed structured API rather than our own parser: the old crawler's failures were all in the layer these
vendors take over (CAPTCHAs, proxy rotation, `scholarly` breakage, region rotation across 9–15 Cloud Function regions).
At our steady-state volume, a managed API costs roughly $500–1,200/month more than DIY with an unblocker.
That is the price of not owning that layer again.

Why SearchApi.io over SerpApi: same coverage (R1 + R2), same billing model (failures free), comparable legal
indemnity ($2M), at roughly 40% of SerpApi's price at our volumes. SerpApi has the stronger track record and
published Scholar success metrics, so it is the fallback and the pilot benchmark.

Why not the cheapest options: Scrappa has no track record we can check, and Scrapingdog's per-publication
histogram is unverified. Either can be re-evaluated later as a cost-reduction step, and the adapter design makes that cheap.

Design implications:
- One `ScholarClient` interface with `get_author(id, start)`, `get_citation(id, pub_id)`, implementations for
  SearchApi and SerpApi; vendor chosen by config.
- Store the vendor's response raw in GCS (as the old crawler did) and normalize in BigQuery, so a vendor switch
  doesn't touch the analytics views.
- Histogram arrays are sparse: years with zero citations are omitted. Fill zeros when normalizing.
- The `since_XXXX` key in the metrics table changes with the year. Parse it, don't hard-code it.

## R3: author discovery without name search

Options, in order of preference:
1. **Accept GS profile URLs/IDs directly** in the search box (users usually know their profile URL).
2. **Paper-title lookup**: run a Scholar keyword search (`engine=google_scholar`, supported by every vendor above) for
   one of the author's known paper titles and read `publication_info.authors[].author_id`. We have candidate
   titles from S2 for the 102M authors already in BigQuery.
3. **In-memory index of GS authors we have already crawled** (reference population + past lookups), replacing the
   current S2 index.

## Legal and ToS context

- Google's ToS prohibit automated querying. `scholar.google.com/robots.txt` allows `/citations?user=` but disallows
  `/citations?*cstart=` (pages past the first 100 pubs) and the rest of `/citations?`, which includes `view_op=view_citation`.
- **Google v. SerpApi** (N.D. Cal. 4:25-cv-10826, DMCA §1201 over SearchGuard): claims dismissed 2026-07-20, partly
  without leave to amend. Google filed an amended complaint ~2026-08-10, and SerpApi's renewed motion to dismiss is pending.
  Scholar has no SearchGuard JS, so this theory fits Scholar less well.
- **Reddit v. SerpApi, Oxylabs, et al.** (S.D.N.Y. 25-cv-08736): motions to dismiss largely denied 2026-07-31
  (reported by Law360 / Loeb & Loeb).
- Using a vendor moves the operational exposure to them, and SerpApi/SearchApi indemnify up to $2M. The ToS and robots.txt position above still
  applies to us as the party commissioning the data.

## Pilot plan

Budget ≈ $115 (SearchApi Developer $40/10K + SerpApi Developer $75/5K).

1. Pick ~20 authors from the old GS reference set, including at least 3 with >1,000 publications.
2. Fetch full profiles + all `view_citation` pages for the same authors through both vendors.
3. Check:
   - SearchApi's profile page size and whether pagination returns every publication (compare to the profile's total)
   - histogram completeness against a direct fetch of a few `view_citation` pages
   - since-year metrics, co-authors, email domain present
   - success rate, p50/p95 latency, wall-clock time for one prolific author (on-demand target: 1–2 min)
4. Re-fetch the profiles ~2–4 weeks later and measure the fraction of publications whose `num_citations` changed.
   This replaces the 15% guess in the cost model.
5. Decide, then build the `ScholarClient` adapter and the new crawler component.

## Sources

Vendor docs and pricing: serpapi.com/google-scholar-author-api, serpapi.com/google-scholar-author-cited-by,
serpapi.com/pricing, serpapi.com/status/google_scholar_author, github.com/serpapi/public-roadmap/issues/2787,
searchapi.io/docs/google-scholar-author, searchapi.io/pricing, scrappa.co/docs/google-scholar-api, scrappa.co/pricing,
scrapingdog.com/pricing, hasdata.com/apis/google-scholar-api, serper.dev, developers.oxylabs.io (google/scholar, google/url,
rate-limits, billing), oxylabs.io/products/web-unblocker, brightdata.com/pricing/web-unlocker, docs.brightdata.com,
decodo.com/scraping/web/pricing, decodo.com/proxies/residential-proxies/pricing, docs.trajectdata.com, zenserp.com,
outscraper.com/pricing, serply.io/docs, scrapingbee.com/pricing, zyte.com/pricing, scrapeops.io/proxy-api-aggregator,
crawlbase.com/pricing, docs.scraperapi.com/credits-and-requests.

Benchmarks (affiliate-funded, Google Search rather than Scholar): proxyway.com/research/comparing-popular-web-scraping-proxy-apis (2024),
proxyway.com/research/web-scraping-api-report-2025. Vendor-authored comparison posts (Scrapingdog, HasData, ScrapingBee,
ScraperAPI) were not relied on.

Libraries: github.com/scholarly-python-package/scholarly (issues #508, #561, #569, #570, #572, #574, #575, #577, #578),
github.com/ma-ji/scholarly2, harzing.com Publish or Perish manual (login requirement, rate-limit warnings).

Legal: searchenginejournal.com/court-dismisses-googles-dmca-claims-against-serpapi/583033, serpapi.com/blog (party-authored),
blog.google (SerpApi lawsuit), law360.com/articles/2508228, loeb.com (Reddit v. SerpApi, 2026-08).

OpenAlex: blog.openalex.org, arXiv 2605.08869, arXiv 2512.16434.
