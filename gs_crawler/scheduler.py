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
    """On-demand requests first (oldest request first), then the stalest profiles."""
    queue = Config.bq_table_ref(Config.REFRESH_QUEUE_TABLE)
    latest = Config.bq_table_ref(Config.AUTHORS_LATEST_VIEW)
    return f"""
        WITH pending AS (
          SELECT q.scholar_id, MIN(q.requested_at) AS requested_at
          FROM {queue} q
          LEFT JOIN {latest} a USING (scholar_id)
          WHERE a.fetched_at IS NULL OR a.fetched_at < q.requested_at
          GROUP BY q.scholar_id
        )
        SELECT scholar_id, num_publications, 0 AS priority, requested_at AS sort_key
        FROM pending LEFT JOIN {latest} USING (scholar_id)
        UNION ALL
        SELECT scholar_id, num_publications, 1 AS priority, fetched_at AS sort_key
        FROM {latest}
        WHERE scholar_id NOT IN (SELECT scholar_id FROM pending)
        ORDER BY priority, sort_key
        LIMIT @limit
    """


def select_batch(budget, client=None):
    """Return [(scholar_id, estimated_searches)] whose estimates fit in `budget`."""
    if budget <= 0:
        return []
    client = client or bigquery.Client(project=Config.PROJECT_ID)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", max(budget, 1) * 2)]
    )
    rows = client.query(candidates_query(), job_config=job_config).result()
    batch, used = [], 0
    for row in rows:
        cost = estimate_searches(row["num_publications"])
        if used + cost > budget:
            continue  # a smaller profile further down may still fit
        batch.append((row["scholar_id"], cost))
        used += cost
    logger.info("Selected %d authors, ~%d searches (budget %d)", len(batch), used, budget)
    return batch
