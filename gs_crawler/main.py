"""Cloud Run Job entry point for the Google Scholar profile crawler.

Modes (CRAWL_MODE):
  - refresh: crawl queued on-demand authors, then the stalest profiles, within
    MAX_SEARCHES_PER_RUN and never dipping below SERPAPI_SEARCH_RESERVE.
  - authors: crawl the comma-separated GS_AUTHOR_IDS now (may use the reserve).
  - materialize: rebuild gs_statistics distribution tables (percentile breakpoints),
    then the app's author search index and featured authors.
  - deploy: create/replace all gs_statistics views and tables.
  - wikidata: reload gs_data.wikidata_authors (GS IDs from Wikidata, for name search
    and ingestion priority), then the app's author search index and featured authors.
  - featured: rebuild only the homepage's featured authors (most-read Wikipedia articles).

Profiles only: publication lists and per-paper totals, no per-paper citation
histograms (see docs/google-scholar-data-sources.md).
"""

import logging
import os
import sys
from datetime import datetime, timezone

from gs_crawler import cache_invalidate, featured, materialize, scheduler, serpapi_client, wikidata, writer
from gs_crawler.config import Config
from gs_crawler.crawler import crawl_author
from gs_crawler.parser import parse_author

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def searches_left():
    return int(serpapi_client.get_account().get("total_searches_left") or 0)


def crawl_and_store(author_ids, budget, new_ids=(), new_budget=None):
    """Crawl authors in order until `budget` searches are used. Returns a summary dict.

    Authors in `new_ids` (never crawled, from Wikidata) share `new_budget` searches:
    each is capped at what is left of it, so a large profile cannot eat into the
    refreshes. One that does not fit is recorded as a failure and waits out the
    cooldown rather than being retried (and paid for) every day.
    """
    new_ids = set(new_ids)
    author_rows, pub_rows, crawled_ids, failures = [], [], [], []
    summary = {"crawled": 0, "failed": 0, "searches": 0, "skipped": 0, "new_searches": 0}
    for scholar_id in author_ids:
        is_new = scholar_id in new_ids
        cap = budget - summary["searches"]
        if is_new and new_budget is not None:
            cap = min(cap, new_budget - summary["new_searches"])
        if cap <= 0:
            summary["skipped"] += 1
            continue
        result = crawl_author(scholar_id, max_searches=cap)
        summary["searches"] += result.searches
        if is_new:
            summary["new_searches"] += result.searches
        if not result.ok:
            summary["failed"] += 1
            too_large = is_new and (result.error or "").startswith("search limit")
            if result.permanent or too_large:
                failures.append({"scholar_id": scholar_id, "error": (result.error or "")[:500],
                                 "failed_at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
            continue
        fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        writer.write_raw_pages(scholar_id, fetched_at, result.pages)
        author_row, pubs = parse_author(result.pages, scholar_id, fetched_at)
        author_rows.append(author_row)
        pub_rows.extend(pubs)
        crawled_ids.append(scholar_id)
        summary["crawled"] += 1
    writer.load_snapshots(author_rows, pub_rows)
    try:
        writer.record_failures(failures)
    except Exception:
        logger.exception("Could not record %d crawl failures", len(failures))
    # Only after the snapshots are in BigQuery, so a repopulation sees the new data.
    cache_invalidate.invalidate_authors(crawled_ids)
    return summary


def refresh_search_index():
    """Rebuild the frontend's author name index (Firestore) so newly crawled and
    Wikidata authors become searchable. Needs DATA_SOURCE=gs; failures are logged."""
    from author_search.config import Config as SearchConfig
    if not SearchConfig.IS_GS:
        logger.info("DATA_SOURCE is not gs; leaving the search index alone")
        return 0
    from author_search.search_service import refresh_author_index
    try:
        count = refresh_author_index()
    except Exception:
        logger.exception("Search index rebuild failed")
        return 0
    logger.info("Search index rebuilt: %d authors", count)
    return count


def refresh_featured():
    """Rebuild the homepage's featured authors (Firestore); failures are logged."""
    try:
        return featured.run()
    except Exception:
        logger.exception("Featured authors rebuild failed")
        return 0


def run_refresh():
    left = searches_left()
    budget = min(Config.MAX_SEARCHES_PER_RUN, left - Config.SEARCH_RESERVE)
    logger.info("SerpApi searches left: %d; refresh budget: %d", left, budget)
    batch = scheduler.select_batch(budget)
    return crawl_and_store([sid for sid, _, _ in batch], budget,
                           new_ids=[sid for sid, _, is_new in batch if is_new],
                           new_budget=scheduler.new_author_budget(budget))


def run_authors(author_ids):
    left = searches_left()
    budget = min(left, Config.MAX_PAGES_PER_AUTHOR * len(author_ids))
    logger.info("On-demand crawl of %d authors; budget %d (searches left %d)", len(author_ids), budget, left)
    return crawl_and_store(author_ids, budget)


def main():
    mode = os.environ.get("CRAWL_MODE", "refresh")
    if mode == "refresh":
        summary = run_refresh()
    elif mode == "authors":
        ids = [s.strip() for s in os.environ.get("GS_AUTHOR_IDS", "").split(",") if s.strip()]
        if not ids:
            raise SystemExit("CRAWL_MODE=authors requires GS_AUTHOR_IDS")
        summary = run_authors(ids)
    elif mode == "materialize":
        logger.info("Rebuilt: %s", materialize.run(kinds=("table",)))
        refresh_search_index()
        refresh_featured()
        return
    elif mode == "deploy":
        logger.info("Deployed: %s", materialize.run(kinds=("view", "table")))
        return
    elif mode == "wikidata":
        logger.info("Wikidata GS IDs loaded: %d", wikidata.run())
        refresh_search_index()
        refresh_featured()
        return
    elif mode == "featured":
        if not featured.run():
            sys.exit(1)
        return
    else:
        raise SystemExit(f"Unknown CRAWL_MODE: {mode}")
    logger.info("Crawl summary: %s", summary)
    if summary["failed"] and not summary["crawled"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
