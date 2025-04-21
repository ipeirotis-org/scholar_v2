"""
Helpers for retrieving co‑author Scholar IDs that have not yet been
added to Firestore.
"""
from __future__ import annotations

import random
from typing import List

from shared.services.bigquery_service import BigQueryService

_BQ = BigQueryService()


def new_coauthors(count: int, oversample_factor: int = 100) -> List[str]:
    """
    Return *exactly* ``count`` distinct Scholar IDs.

    We first fetch ``count * oversample_factor`` rows from the BigQuery
    view and then randomly choose ``count`` IDs from that pool.  The
    oversampling guards against duplicate or unusable rows without
    hammering BigQuery with huge scans.

    Parameters
    ----------
    count : int
        Number of IDs desired.
    oversample_factor : int, optional
        How many times more rows to pull than needed.  Default is 100.

    Returns
    -------
    List[str]
        A list of Scholar IDs, length == ``count`` (or fewer if the
        source view is very small).
    """
    rows_needed = max(count * oversample_factor, 1)
    sql = f"""
        SELECT
          coauthor_scholar_id
        FROM
          `scholar-version2.firestore_views.coauthors_to_add`
        LIMIT {rows_needed}
    """
    df = _BQ.query(sql)
    ids = df["coauthor_scholar_id"].dropna().drop_duplicates().tolist()

    if len(ids) <= count:
        return ids
    return random.sample(ids, count)
