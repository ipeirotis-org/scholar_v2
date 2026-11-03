"""Pick which authors to refresh within a SerpApi search budget."""

import logging
import math

from google.cloud import bigquery

from gs_crawler.config import Config

logger = logging.getLogger(__name__)

# Lists grow ~7%/year (pilot, 2026-11); pad the page estimate so a refresh
# rarely needs more searches than budgeted.
GROWTH_PAD = 1.10


# Unknown authors (on-demand, never crawled) are budgeted as an average profile:
# ~215 publications -> 3 pages (legacy reference set, 2026-11).
UNKNOWN_AUTHOR_SEARCHES = 3


def estimate_searches(num_publications):
    if num_publications is None:
        return UNKNOWN_AUTHOR_SEARCHES
    return max(1, math.ceil(num_publications * GROWTH_PAD / Config.PAGE_SIZE))


def candidates_query():
    """Candidates in crawl order: on-demand requests (oldest first), then authors
    never crawled from Wikidata (most Wikipedia pageviews first), then the
    stalest profiles. Authors whose crawl failed recently are left out."""
    queue = Config.bq_table_ref(Config.REFRESH_QUEUE_TABLE)
    latest = Config.bq_table_ref(Config.AUTHORS_LATEST_VIEW)
    wikidata = Config.bq_table_ref(Config.WIKIDATA_AUTHORS_TABLE)
    failures = Config.bq_table_ref(Config.CRAWL_FAILURES_TABLE)
    return f"""
        WITH pending AS (
          SELECT q.scholar_id, MIN(q.requested_at) AS requested_at
          FROM {queue} q
          LEFT JOIN {latest} a USING (scholar_id)
          WHERE a.fetched_at IS NULL OR a.fetched_at < q.requested_at
          GROUP BY q.scholar_id
        ),
        failed AS (
          SELECT DISTINCT scholar_id FROM {failures}
          WHERE failed_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @cooldown_days DAY)
        ),
        new_authors AS (
          SELECT w.scholar_id, w.enwiki_views_60d, w.sitelinks
          FROM {wikidata} w
          LEFT JOIN {latest} a USING (scholar_id)
          WHERE a.scholar_id IS NULL
            AND w.scholar_id NOT IN (SELECT scholar_id FROM pending)
            AND w.scholar_id NOT IN (SELECT scholar_id FROM failed)
          ORDER BY w.enwiki_views_60d DESC NULLS LAST, w.sitelinks DESC NULLS LAST, w.scholar_id
          LIMIT @new_limit
        )
        SELECT scholar_id, num_publications, 0 AS priority,
               ROW_NUMBER() OVER (ORDER BY requested_at, scholar_id) AS rank
        FROM pending LEFT JOIN {latest} USING (scholar_id)
        UNION ALL
        SELECT scholar_id, NULL AS num_publications, 1 AS priority,
               ROW_NUMBER() OVER (ORDER BY enwiki_views_60d DESC NULLS LAST,
                                           sitelinks DESC NULLS LAST, scholar_id) AS rank
        FROM new_authors
        UNION ALL
        SELECT scholar_id, num_publications, 2 AS priority,
               ROW_NUMBER() OVER (ORDER BY fetched_at, scholar_id) AS rank
        FROM {latest}
        WHERE scholar_id NOT IN (SELECT scholar_id FROM pending)
          AND scholar_id NOT IN (SELECT scholar_id FROM failed)
        ORDER BY priority, rank
        LIMIT @limit
    """


def new_author_budget(budget, share=None):
    """Searches a run of `budget` may spend on never-crawled (Wikidata) authors."""
    share = Config.NEW_AUTHOR_SHARE if share is None else share
    return int(max(budget, 0) * max(0.0, min(share, 1.0)))


def select_batch(budget, client=None, new_author_share=None):
    """Return [(scholar_id, estimated_searches, is_new)] whose estimates fit in `budget`.

    New (never crawled) authors may use at most `new_author_share` of the budget;
    whatever they leave goes to refreshing the stalest profiles. Estimates for new
    authors are guesses, so crawl_and_store enforces the share on actual searches.
    """
    if budget <= 0:
        return []
    new_budget = new_author_budget(budget, new_author_share)
    client = client or bigquery.Client(project=Config.PROJECT_ID)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", max(budget, 1) * 2),
            bigquery.ScalarQueryParameter("new_limit", "INT64", new_budget // UNKNOWN_AUTHOR_SEARCHES + 1),
            bigquery.ScalarQueryParameter("cooldown_days", "INT64", Config.FAILURE_COOLDOWN_DAYS),
        ]
    )
    rows = client.query(candidates_query(), job_config=job_config).result()
    batch, used, used_new = [], 0, 0
    for row in rows:
        cost = estimate_searches(row["num_publications"])
        if used + cost > budget:
            continue  # a smaller profile further down may still fit
        is_new = row.get("priority") == 1
        if is_new and used_new + cost > new_budget:
            continue
        batch.append((row["scholar_id"], cost, is_new))
        used += cost
        used_new += cost if is_new else 0
    logger.info("Selected %d authors, ~%d searches (%d on new authors; budget %d)",
                len(batch), used, used_new, budget)
    return batch
