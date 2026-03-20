"""Author Search Service — Component 6.

Implements the search strategy described in ARCHITECTURE.md:
1. Search crawled authors in BigQuery (fast, free)
2. Search coauthor network in BigQuery (fast, free)
3. Check Firestore cache for Scholar results
4. Fall back to Google Scholar (slow, rate-limited)

Results from all sources are deduplicated by scholar_id and merged.
"""

import logging

from v3.author_search.bigquery_client import BigQuerySearchClient
from v3.author_search.cache import SearchCache
from v3.author_search.config import Config
from v3.author_search import scholar_client

logger = logging.getLogger(__name__)


class AuthorSearchService:
    def __init__(self, bq_client=None, cache=None):
        self.bq = bq_client or BigQuerySearchClient()
        self.cache = cache or SearchCache()

    def search(self, author_name):
        """Search for authors by name using the tiered strategy.

        Returns a list of author dicts with keys:
            scholar_id, name, affiliation, email_domain, citedby, hindex, source
        """
        if not author_name or len(author_name.strip()) < 2:
            return []

        name = author_name.strip()
        seen_ids = set()
        results = []

        # Step 1: Search crawled authors
        crawled = self.bq.search_crawled_authors(name)
        for author in crawled:
            sid = author.get("scholar_id")
            if sid and sid not in seen_ids:
                seen_ids.add(sid)
                author["source"] = "database"
                results.append(author)

        if len(results) >= Config.LOCAL_RESULTS_THRESHOLD:
            logger.info("Search '%s': %d results from crawled authors", name, len(results))
            return results

        # Step 2: Search coauthor network
        coauthors = self.bq.search_coauthor_network(name)
        for author in coauthors:
            sid = author.get("scholar_id")
            if sid and sid not in seen_ids:
                seen_ids.add(sid)
                author["source"] = "coauthor_network"
                results.append(author)

        if len(results) >= Config.LOCAL_RESULTS_THRESHOLD:
            logger.info("Search '%s': %d results (crawled + coauthor)", name, len(results))
            return results

        # Step 3: Check Firestore cache for Scholar results
        cached = self.cache.get(name)
        if cached is not None:
            for author in cached:
                sid = author.get("scholar_id")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    author["source"] = "scholar_cached"
                    results.append(author)
            logger.info("Search '%s': %d results (local + cached scholar)", name, len(results))
            return results

        # Step 4: Fall back to Google Scholar
        scholar_results = scholar_client.search_scholar(name)
        if scholar_results:
            self.cache.set(name, scholar_results)
            for author in scholar_results:
                sid = author.get("scholar_id")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    author["source"] = "scholar"
                    results.append(author)

        logger.info("Search '%s': %d total results (all sources)", name, len(results))
        return results
