"""Featured authors for the homepage: profiled authors with the most-read Wikipedia articles.

Ranks profiled GS authors by English Wikipedia pageviews over the last 60 days
(`gs_data.wikidata_authors.enwiki_views_60d`, loaded by the monthly wikidata run)
and stores the top MAX_FEATURED in Firestore, with the same fields as the
recent-authors list. The frontend shows the first ten on the homepage and the
rest on /featured.

Left out: deceased people, and profiles with an h-index below MIN_HINDEX, which
are stubs or people known for something other than research (George Soros:
h-index 1; Rachael Gunn, the Olympic breakdancer: 7).

Rebuilt after the weekly percentile rebuild (materialize mode), so the PiP
values match what the cache layer serves, and after the monthly wikidata reload,
which refreshes the pageviews.
"""

import logging
from datetime import datetime, timezone

from google.cloud import bigquery, firestore

from gs_crawler.config import Config

logger = logging.getLogger(__name__)

# Must match frontend/cache.py.
FEATURED_COLLECTION = "v3_featured_authors"
FEATURED_DOC_ID = "featured_gs"
RECENT_COLLECTION = "v3_recent_authors"
RECENT_DOC_ID = "recent_gs"
MAX_FEATURED = 100
MIN_HINDEX = 10


def featured_query():
    wikidata = Config.bq_table_ref(Config.WIKIDATA_AUTHORS_TABLE)
    authors = Config.stats_table_ref("ranked_author_current")
    pip = Config.stats_table_ref("ranked_author_pip_scores_current")
    return f"""
        SELECT
          a.scholar_id, a.name, a.affiliation, a.hindex, a.citedby,
          p.pip_score, p.pip_score_percentile AS pip_percentile,
          p.pip_auc_score, p.pip_auc_score_percentile AS pip_auc_percentile,
          w.enwiki_title AS wikipedia_title, w.enwiki_views_60d AS wikipedia_views_60d
        FROM {wikidata} w
        JOIN {authors} a USING (scholar_id)
        LEFT JOIN {pip} p USING (scholar_id)
        WHERE w.enwiki_views_60d IS NOT NULL
          AND NOT COALESCE(w.died, FALSE)
          AND a.hindex >= @min_hindex
        ORDER BY w.enwiki_views_60d DESC, a.scholar_id
        LIMIT @limit
    """


def recent_scores_query():
    authors = Config.stats_table_ref("ranked_author_current")
    pip = Config.stats_table_ref("ranked_author_pip_scores_current")
    return f"""
        SELECT
          a.scholar_id, a.name, a.affiliation, a.hindex, a.citedby,
          p.pip_score, p.pip_score_percentile AS pip_percentile,
          p.pip_auc_score, p.pip_auc_score_percentile AS pip_auc_percentile
        FROM {authors} a
        LEFT JOIN {pip} p USING (scholar_id)
        WHERE a.scholar_id IN UNNEST(@ids)
    """


def select_featured(client=None, limit=MAX_FEATURED):
    """Featured author entries, most-viewed first."""
    client = client or bigquery.Client(project=Config.PROJECT_ID)
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("min_hindex", "INT64", MIN_HINDEX),
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
    ])
    return [dict(row.items()) for row in client.query(featured_query(), job_config=job_config).result()]


def run(bq=None, db=None):
    """Rebuild the featured list in Firestore. Returns the number of authors stored.

    An empty result leaves the stored list alone (e.g. a query against a table
    that is being reloaded), so the homepage never loses the section.
    """
    entries = select_featured(bq)
    if not entries:
        logger.warning("No featured authors found; keeping the stored list")
        return 0
    db = db or firestore.Client(project=Config.PROJECT_ID)
    db.collection(FEATURED_COLLECTION).document(FEATURED_DOC_ID).set({
        "timestamp": datetime.now(timezone.utc),
        "data": entries,
    })
    logger.info("Featured authors stored: %d (top: %s)", len(entries), entries[0].get("name"))
    return len(entries)


def refresh_recent(bq=None, db=None):
    """Refresh scores on existing homepage recent-author entries from gs_statistics."""
    db = db or firestore.Client(project=Config.PROJECT_ID)
    doc_ref = db.collection(RECENT_COLLECTION).document(RECENT_DOC_ID)
    snap = doc_ref.get()
    if not snap.exists:
        return 0
    existing = (snap.to_dict() or {}).get("data")
    if not isinstance(existing, list) or not existing:
        return 0
    ids = [r["scholar_id"] for r in existing if isinstance(r, dict) and r.get("scholar_id")]
    if not ids:
        return 0

    bq = bq or bigquery.Client(project=Config.PROJECT_ID)
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("ids", "STRING", ids),
    ])
    by_id = {
        row["scholar_id"]: dict(row.items())
        for row in bq.query(recent_scores_query(), job_config=job_config).result()
    }
    if not by_id:
        return 0

    @firestore.transactional
    def _update(transaction):
        current_snap = doc_ref.get(transaction=transaction)
        if not current_snap.exists:
            return 0
        current = (current_snap.to_dict() or {}).get("data")
        if not isinstance(current, list):
            return 0
        updated_rows = []
        refreshed = 0
        for item in current:
            if not isinstance(item, dict):
                continue
            sid = item.get("scholar_id")
            fresh = by_id.get(sid)
            if fresh:
                merged = dict(item)
                merged.update(fresh)
                updated_rows.append(merged)
                refreshed += 1
            else:
                updated_rows.append(item)
        transaction.set(doc_ref, {
            "timestamp": datetime.now(timezone.utc),
            "data": updated_rows,
        })
        return refreshed

    count = _update(db.transaction())
    logger.info("Refreshed recent GS authors: %d", count)
    return count
