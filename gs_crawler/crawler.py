"""Crawl a full Google Scholar profile (all publication pages) via SerpApi."""

import logging
from dataclasses import dataclass, field

from gs_crawler import serpapi_client
from gs_crawler.config import Config

logger = logging.getLogger(__name__)


@dataclass
class CrawlResult:
    scholar_id: str
    pages: list = field(default_factory=list)
    searches: int = 0
    error: str = None
    # True when retrying cannot help (no such profile); starts the failure cooldown.
    permanent: bool = False

    @property
    def ok(self):
        return self.error is None and bool(self.pages)


def crawl_author(scholar_id, max_searches=None):
    """Fetch every profile page for `scholar_id`, 100 articles per search.

    Stops when a page has fewer than 100 articles. A profile is only returned
    as ok if every page succeeded: a partial publication list would bias the
    author's percentiles, so it is reported as an error instead.
    """
    limit = min(Config.MAX_PAGES_PER_AUTHOR, max_searches or Config.MAX_PAGES_PER_AUTHOR)
    result = CrawlResult(scholar_id)
    start = 0
    while True:
        if result.searches >= limit:
            result.error = f"search limit {limit} reached at start={start}"
            break
        try:
            page = serpapi_client.fetch_author_page(scholar_id, start)
        except serpapi_client.SerpApiError as exc:
            # A failed request is not billed by SerpApi; still count the attempt
            # against this run's limit so a stuck author cannot loop.
            result.searches += 1
            result.error = str(exc)
            result.permanent = not exc.transient
            break
        result.searches += 1
        articles = page.get("articles") or []
        if start > 0 and not articles:
            break  # past the end of the list
        result.pages.append(page)
        if len(articles) < Config.PAGE_SIZE:
            break
        start += Config.PAGE_SIZE
    if result.error:
        logger.warning("Crawl of %s incomplete after %d searches: %s", scholar_id, result.searches, result.error)
    return result
