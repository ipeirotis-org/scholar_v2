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


def wikidata_migration_sql():
    """Adds the eligibility flags (and views_fetched_at) to a wikidata_authors table created before them.
    Idempotent and free; run before candidates_query, which reads the flags."""
    table = Config.bq_table_ref(Config.WIKIDATA_AUTHORS_TABLE)
    return f"""
        ALTER TABLE {table}
          ADD COLUMN IF NOT EXISTS died BOOL,
          ADD COLUMN IF NOT EXISTS is_scholarly BOOL,
          ADD COLUMN IF NOT EXISTS is_politician BOOL,
          ADD COLUMN IF NOT EXISTS views_fetched_at TIMESTAMP
    """


def candidates_query():
    """Candidates in crawl order: on-demand requests (oldest first), then authors
    never crawled, from Wikidata (most Wikipedia pageviews first) and from the co-author
    lists of profiled authors (listed by the most profiled authors first), alternating
    between the two sources, then legacy-seeded
    profiles with untitled works (largest untitled share first, then most cited), then the stalest
    profiles, down to those last crawled REFRESH_MIN_AGE_DAYS ago. Authors whose crawl failed
    recently are left out.

    The untitled tier exists because the legacy seed lacked titles for some
    profiles (2026-11-04: 122 with none, 260 more with at least 10% missing,
    among them Kaiming He and Todd Golub); their pages listed every work as
    "Untitled". Only legacy snapshots qualify, so a SerpApi profile that itself
    lacks a title is not re-crawled every day."""
    queue = Config.bq_table_ref(Config.REFRESH_QUEUE_TABLE)
    latest = Config.bq_table_ref(Config.AUTHORS_LATEST_VIEW)
    publications = Config.bq_table_ref(Config.PUBLICATIONS_LATEST_VIEW)
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
            -- Living scholars only (see gs_crawler/wikidata.py); NULL flags never qualify.
            AND w.is_scholarly IS TRUE AND w.died IS NOT TRUE AND w.is_politician IS NOT TRUE
            AND w.scholar_id NOT IN (SELECT scholar_id FROM pending)
            AND w.scholar_id NOT IN (SELECT scholar_id FROM failed)
          ORDER BY w.enwiki_views_60d DESC NULLS LAST, w.sitelinks DESC NULLS LAST, w.scholar_id
          LIMIT @new_limit
        ),
        new_coauthors AS (
          -- People on profiled authors' co-author lists who have no profile here. Those
          -- Wikidata records as dead or as politicians are left out, as in new_authors.
          SELECT c.scholar_id, COUNT(DISTINCT a.scholar_id) AS listed_by
          FROM {latest} a, UNNEST(a.coauthors) AS c
          WHERE c.scholar_id IS NOT NULL
            AND c.scholar_id NOT IN (SELECT scholar_id FROM {latest})
            AND c.scholar_id NOT IN (SELECT scholar_id FROM pending)
            AND c.scholar_id NOT IN (SELECT scholar_id FROM failed)
            AND c.scholar_id NOT IN (SELECT scholar_id FROM new_authors)
            AND c.scholar_id NOT IN (SELECT scholar_id FROM {wikidata}
                                     WHERE died IS TRUE OR is_politician IS TRUE)
          GROUP BY c.scholar_id
          HAVING listed_by >= @coauthor_min_listed_by
          ORDER BY listed_by DESC, c.scholar_id
          LIMIT @coauthor_limit
        ),
        untitled AS (
          SELECT p.scholar_id,
                 COUNTIF(p.title IS NULL OR TRIM(p.title) = '') / COUNT(*) AS untitled_share
          FROM {publications} p
          JOIN {latest} a USING (scholar_id)
          WHERE a.source = 'legacy_scholarly'
          GROUP BY p.scholar_id
          HAVING untitled_share >= @untitled_min_share
        )
        SELECT scholar_id, num_publications, 0 AS priority,
               ROW_NUMBER() OVER (ORDER BY requested_at, scholar_id) AS rank,
               CAST(NULL AS STRING) AS new_source
        FROM pending LEFT JOIN {latest} USING (scholar_id)
        UNION ALL
        SELECT scholar_id, NULL AS num_publications, 1 AS priority,
               ROW_NUMBER() OVER (ORDER BY enwiki_views_60d DESC NULLS LAST,
                                           sitelinks DESC NULLS LAST, scholar_id) AS rank,
               'wikidata' AS new_source
        FROM new_authors
        UNION ALL
        -- Same priority as the Wikidata authors, so the two sources alternate by rank.
        SELECT scholar_id, NULL AS num_publications, 1 AS priority,
               ROW_NUMBER() OVER (ORDER BY listed_by DESC, scholar_id) AS rank,
               'coauthor' AS new_source
        FROM new_coauthors
        UNION ALL
        SELECT a.scholar_id, a.num_publications, 2 AS priority,
               ROW_NUMBER() OVER (ORDER BY u.untitled_share DESC, a.citedby DESC, a.scholar_id) AS rank,
               CAST(NULL AS STRING) AS new_source
        FROM {latest} a JOIN untitled u USING (scholar_id)
        WHERE a.scholar_id NOT IN (SELECT scholar_id FROM pending)
          AND a.scholar_id NOT IN (SELECT scholar_id FROM failed)
        UNION ALL
        SELECT scholar_id, num_publications, 3 AS priority,
               ROW_NUMBER() OVER (ORDER BY fetched_at, scholar_id) AS rank,
               CAST(NULL AS STRING) AS new_source
        FROM {latest}
        WHERE scholar_id NOT IN (SELECT scholar_id FROM pending)
          AND scholar_id NOT IN (SELECT scholar_id FROM failed)
          AND scholar_id NOT IN (SELECT scholar_id FROM untitled)
          AND fetched_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @min_age_days DAY)
        ORDER BY priority, rank, new_source DESC
        LIMIT @limit
    """


def new_author_budget(budget, share=None):
    """Searches a run of `budget` may spend on never-crawled authors (Wikidata and co-authors)."""
    share = Config.NEW_AUTHOR_SHARE if share is None else share
    return int(max(budget, 0) * max(0.0, min(share, 1.0)))


def coauthor_budget(new_budget, share=None):
    """The part of the new-author budget for co-authors of profiled authors; Wikidata gets the rest."""
    share = Config.COAUTHOR_NEW_SHARE if share is None else share
    return int(max(new_budget, 0) * max(0.0, min(share, 1.0)))


def select_batch(budget, client=None, new_author_share=None, coauthor_share=None):
    """Return [(scholar_id, estimated_searches, is_new)] whose estimates fit in `budget`.

    New (never crawled) authors may use at most `new_author_share` of the budget,
    split between Wikidata and co-authors by `coauthor_share`; whatever either
    source leaves goes to refreshing the stalest profiles. Estimates for new
    authors are guesses, so crawl_and_store enforces the share on actual searches
    (for the two sources together; they alternate in crawl order).
    """
    if budget <= 0:
        return []
    new_budget = new_author_budget(budget, new_author_share)
    source_budget = {"coauthor": coauthor_budget(new_budget, coauthor_share)}
    source_budget["wikidata"] = new_budget - source_budget["coauthor"]
    client = client or bigquery.Client(project=Config.PROJECT_ID)
    try:
        client.query(wikidata_migration_sql()).result()
    except Exception:
        # The candidates query below then fails loudly if the columns are missing.
        logger.exception("wikidata_authors flag migration failed")
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", max(budget, 1) * 2),
            # No budget, no candidates: a zero limit keeps that source out of the batch.
            bigquery.ScalarQueryParameter(
                "new_limit", "INT64",
                source_budget["wikidata"] // UNKNOWN_AUTHOR_SEARCHES + 1 if source_budget["wikidata"] else 0),
            bigquery.ScalarQueryParameter(
                "coauthor_limit", "INT64",
                source_budget["coauthor"] // UNKNOWN_AUTHOR_SEARCHES + 1 if source_budget["coauthor"] else 0),
            bigquery.ScalarQueryParameter("coauthor_min_listed_by", "INT64", Config.COAUTHOR_MIN_LISTED_BY),
            bigquery.ScalarQueryParameter("cooldown_days", "INT64", Config.FAILURE_COOLDOWN_DAYS),
            bigquery.ScalarQueryParameter("untitled_min_share", "FLOAT64", Config.UNTITLED_MIN_SHARE),
            bigquery.ScalarQueryParameter("min_age_days", "INT64", Config.REFRESH_MIN_AGE_DAYS),
        ]
    )
    rows = client.query(candidates_query(), job_config=job_config).result()
    batch, used = [], 0
    used_new = {"wikidata": 0, "coauthor": 0}
    for row in rows:
        cost = estimate_searches(row["num_publications"])
        if used + cost > budget:
            continue  # a smaller profile further down may still fit
        is_new = row.get("priority") == 1
        if is_new:
            source = row.get("new_source") or "wikidata"
            if used_new[source] + cost > source_budget[source]:
                continue
            used_new[source] += cost
        batch.append((row["scholar_id"], cost, is_new))
        used += cost
    logger.info("Selected %d authors, ~%d searches (%d on new authors: %d Wikidata, %d co-authors; budget %d)",
                len(batch), used, sum(used_new.values()), used_new["wikidata"], used_new["coauthor"], budget)
    return batch
