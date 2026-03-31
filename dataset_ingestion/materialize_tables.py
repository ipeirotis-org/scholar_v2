"""Materialize the BigQuery analytics DAG into tables.

Only materializes tables that are either:
  1. Directly queried by the app (cache_layer, author_search), or
  2. Used as inputs by downstream materializations (via _TABLE_SUBSTITUTIONS), or
  3. Distribution tables (small, needed for percentile lookups).

Intermediate views (base_author_publications, stats_publication_current,
intermediate_author_publication_state_temporal) are left as views —
their output is consumed inline by the downstream materialized tables.
This avoids materializing the expensive intermediate_author_publication_
state_temporal table (~1h) that is never queried directly.

The DAG has 6 levels (1-6). Each level depends on the previous ones.
Tables within a level can run in parallel, but are executed sequentially
here for simplicity and to stay within BigQuery concurrency limits.

Architecture:
  - Level 1: Foundation (stats_author_current + dist tables)
  - Level 2: Temporal foundation + first ranked + dist
  - Level 3: Metrics + PiP inputs + ranked (app-facing)
  - Level 4: PiP scores + higher distributions
  - Level 5: Ranked PiP + temporal ranked (app-facing)
  - Level 6: Temporal PiP distribution
"""

import logging
import pathlib

from google.cloud import bigquery

from dataset_ingestion.config import Config

logger = logging.getLogger(__name__)

_bq_client = None

# Path to the SQL files directory
_SQL_DIR = pathlib.Path(__file__).resolve().parent.parent / "bigquery" / "statistics"

# View → table substitutions applied during materialization.
# SQL files reference views so they work standalone (without requiring
# materialized tables to exist). During pipeline execution, we substitute
# _table references so each level reads from the previous level's
# materialized output instead of re-executing expensive view chains.
# Keys use the configured stats dataset (not hardcoded) so overrides work.
_SUBSTITUTION_PAIRS = [
    ("stats_author_current`", "stats_author_current_table`"),
    ("stats_publication_citations_temporal`", "stats_publication_citations_temporal_table`"),
    ("ranked_publication_current`", "ranked_publication_current_table`"),
    ("stats_author_metrics_temporal_view`", "stats_author_metrics_temporal_table`"),
    ("stats_author_pip_scores_current`", "stats_author_pip_scores_current_table`"),
    ("stats_author_pip_scores_temporal_view`", "stats_author_pip_scores_temporal_table`"),
]


def _get_table_substitutions():
    """Build substitution dict using the configured stats dataset."""
    ds = Config.BQ_STATS_DATASET
    return {
        f"{ds}.{view_suffix}": f"{ds}.{table_suffix}"
        for view_suffix, table_suffix in _SUBSTITUTION_PAIRS
    }


def _get_bq_client():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=Config.PROJECT_ID)
    return _bq_client


def _read_sql(filename):
    """Read a SQL file from the bigquery/statistics/ directory."""
    path = _SQL_DIR / filename
    return path.read_text()


def _view_to_table_sql(view_sql, table_name, cluster_by, partition_by=None):
    """Convert a CREATE VIEW SQL into a CREATE TABLE SQL.

    Extracts the SELECT portion from the view definition and wraps it
    in a CREATE OR REPLACE TABLE statement with clustering.
    """
    upper = view_sql.upper()

    view_idx = upper.find("VIEW")
    if view_idx == -1:
        raise ValueError(f"Could not find VIEW keyword in SQL for {table_name}")

    as_idx = upper.find("\nAS\n", view_idx)
    if as_idx == -1:
        as_idx = upper.find("\nAS ", view_idx)
    if as_idx == -1:
        as_idx = upper.find(" AS\n", view_idx)
    if as_idx == -1:
        as_idx = upper.find(" AS ", view_idx)
    if as_idx == -1:
        raise ValueError(f"Could not find AS keyword in SQL for {table_name}")

    select_sql = view_sql[as_idx + 3:].strip().rstrip(";")

    # BigQuery doesn't allow ORDER BY in CREATE TABLE ... AS SELECT
    # when using CLUSTER BY. Strip a trailing ORDER BY clause if present.
    # Only strips ORDER BY at the end of the statement (not inside CTEs).
    import re
    select_sql = re.sub(
        r'\bORDER\s+BY\s+[\w.,\s]+\s*$', '', select_sql, flags=re.IGNORECASE
    ).rstrip()

    table_ref = Config.bq_stats_table_ref(table_name)
    cluster_clause = f"CLUSTER BY {', '.join(cluster_by)}"
    partition_clause = ""
    if partition_by:
        partition_clause = f"\nPARTITION BY RANGE_BUCKET({partition_by}, GENERATE_ARRAY(1900, 2100, 1))"

    return f"CREATE OR REPLACE TABLE {table_ref}\n{cluster_clause}{partition_clause}\nAS\n{select_sql}"


def _run_sql(sql, description):
    """Execute a SQL statement and log the result."""
    client = _get_bq_client()
    logger.info("Materializing: %s", description)
    job = client.query(sql)
    job.result()

    if job.destination:
        try:
            table = client.get_table(job.destination)
            logger.info("  %s: %d rows", description, table.num_rows)
            return table.num_rows
        except Exception:
            pass

    logger.info("  %s: complete", description)
    return 0


def _apply_table_substitutions(sql):
    """Replace view references with materialized table references."""
    for view_ref, table_ref in _get_table_substitutions().items():
        sql = sql.replace(view_ref, table_ref)
    return sql


def _materialize_from_view(view_sql_file, table_name, cluster_by, partition_by=None):
    """Read a view SQL file, convert to table, and execute."""
    view_sql = _read_sql(view_sql_file)
    table_sql = _view_to_table_sql(view_sql, table_name, cluster_by, partition_by)
    table_sql = _apply_table_substitutions(table_sql)
    return _run_sql(table_sql, table_name)


def _materialize_dist(dist_sql_file, description):
    """Execute a distribution table SQL file directly (already CREATE TABLE)."""
    sql = _read_sql(dist_sql_file)
    sql = _apply_table_substitutions(sql)
    return _run_sql(sql, description)


def _materialize_dist_publication_citations_temporal():
    """Materialize dist_publication_citations_temporal in 4 parts.

    The full query (4 UNION ALL sections with PERCENT_RANK OVER) exceeds
    BigQuery memory when run as a single statement (~170% of limit on
    2B+ rows). Split into CREATE TABLE + 3 INSERT INTO statements.

    The table is written directly (not via temp+swap) because:
    - Each BigQuery INSERT is individually atomic
    - The table is only read by downstream materialization steps in the
      same pipeline run (not by live user queries)
    - Downstream steps haven't started yet when this runs
    """
    table_ref = Config.bq_stats_table_ref("dist_publication_citations_temporal")
    temporal_table = _apply_table_substitutions(
        Config.bq_stats_table_ref("stats_publication_citations_temporal")
    )

    # Part 1: CREATE TABLE with first metric (replaces any existing table)
    sql1 = f"""CREATE OR REPLACE TABLE {table_ref}
CLUSTER BY metric_name, pub_year
AS
SELECT DISTINCT
  pub_year, citation_year, CAST(NULL AS INT64) AS age,
  'pub_year_yearly_citations' AS metric_name,
  yearly_citations AS metric_value,
  PERCENT_RANK() OVER(PARTITION BY pub_year, citation_year ORDER BY yearly_citations ASC) AS percentile
FROM {temporal_table}"""
    _run_sql(sql1, "dist_publication_citations_temporal (1/4: pub_year_yearly)")

    # Part 2: INSERT cumulative by pub_year
    sql2 = f"""INSERT INTO {table_ref}
SELECT DISTINCT
  pub_year, citation_year, CAST(NULL AS INT64) AS age,
  'pub_year_cumulative_citations' AS metric_name,
  cumulative_citations AS metric_value,
  PERCENT_RANK() OVER(PARTITION BY pub_year, citation_year ORDER BY cumulative_citations ASC) AS percentile
FROM {temporal_table}"""
    _run_sql(sql2, "dist_publication_citations_temporal (2/4: pub_year_cumulative)")

    # Parts 3 & 4: yearly/cumulative by age.
    # These partitions are very coarse (~100 age values, ~20M rows each),
    # so we pre-aggregate with GROUP BY to count occurrences, then compute
    # PERCENT_RANK via cumulative sum formula instead of OVER() on the
    # full 2B row table. COALESCE handles single-row partitions (where
    # PERCENT_RANK returns 0 but our denominator would be 0).
    sql3 = f"""INSERT INTO {table_ref}
WITH agg AS (
  SELECT age, yearly_citations AS metric_value, COUNT(*) AS cnt
  FROM {temporal_table}
  GROUP BY age, yearly_citations
)
SELECT DISTINCT
  CAST(NULL AS INT64) AS pub_year, CAST(NULL AS INT64) AS citation_year, age,
  'age_yearly_citations' AS metric_name,
  metric_value,
  COALESCE(
    (SUM(cnt) OVER(PARTITION BY age ORDER BY metric_value ASC) - cnt)
    / NULLIF(SUM(cnt) OVER(PARTITION BY age) - 1, 0),
    0) AS percentile
FROM agg"""
    _run_sql(sql3, "dist_publication_citations_temporal (3/4: age_yearly)")

    sql4 = f"""INSERT INTO {table_ref}
WITH agg AS (
  SELECT age, cumulative_citations AS metric_value, COUNT(*) AS cnt
  FROM {temporal_table}
  GROUP BY age, cumulative_citations
)
SELECT DISTINCT
  CAST(NULL AS INT64) AS pub_year, CAST(NULL AS INT64) AS citation_year, age,
  'age_cumulative_citations' AS metric_name,
  metric_value,
  COALESCE(
    (SUM(cnt) OVER(PARTITION BY age ORDER BY metric_value ASC) - cnt)
    / NULLIF(SUM(cnt) OVER(PARTITION BY age) - 1, 0),
    0) AS percentile
FROM agg"""
    _run_sql(sql4, "dist_publication_citations_temporal (4/4: age_cumulative)")


def materialize_level_1():
    """Level 1: Foundation (independent).

    App-facing:
    - stats_author_current_table (queried by cache_layer)

    Dist tables (needed for percentile lookups):
    - dist_publication_citations
    - dist_author_metrics

    Skipped (intermediate, consumed inline by downstream tables):
    - base_author_publications (view)
    - stats_publication_current (view)
    """
    logger.info("=== Level 1: Foundation ===")

    _materialize_from_view(
        "stats_author_current.sql",
        "stats_author_current_table",
        cluster_by=["scholar_id", "year_of_first_pub"],
    )

    _materialize_dist(
        "dist_publication_citations.sql",
        "dist_publication_citations",
    )

    _materialize_dist(
        "dist_author_metrics.sql",
        "dist_author_metrics",
    )

    logger.info("=== Level 1 complete ===")


def materialize_level_2():
    """Level 2: Temporal foundation + first ranked + dist (depends on Level 1).

    Intermediate (needed by downstream tables):
    - stats_publication_citations_temporal_table (needed by dist below)
    - ranked_publication_current_table (needed by pip_inputs in Level 3)

    Dist tables:
    - dist_publication_citations_temporal

    Skipped (intermediate, consumed inline by downstream tables):
    - intermediate_author_publication_state_temporal (view — most expensive)
    """
    logger.info("=== Level 2: Temporal foundation + first ranked + dist ===")

    _materialize_from_view(
        "stats_publication_citations_temporal.sql",
        "stats_publication_citations_temporal_table",
        cluster_by=["author_pub_id", "pub_year", "citation_year"],
    )

    _materialize_from_view(
        "ranked_publication_current.sql",
        "ranked_publication_current_table",
        cluster_by=["author_pub_id", "pub_year"],
    )

    _materialize_dist_publication_citations_temporal()

    logger.info("=== Level 2 complete ===")


def materialize_level_3():
    """Level 3: Metrics + PiP inputs + ranked (depends on Levels 1-2).

    App-facing:
    - ranked_author_current_table
    - ranked_publication_citations_temporal_table

    Substitution target (read by downstream levels):
    - stats_author_metrics_temporal_table

    Skipped (view works fine for per-author queries):
    - stats_author_publication_pip_inputs_current (complex interpolation
      query that's too expensive to materialize; the cache layer queries
      it filtered by scholar_id which is fast via the view)
    """
    logger.info("=== Level 3: Metrics + PiP inputs + ranked ===")

    _materialize_from_view(
        "stats_author_metrics_temporal.sql",
        "stats_author_metrics_temporal_table",
        cluster_by=["scholar_id", "state_year"],
    )

    _materialize_from_view(
        "ranked_author_current.sql",
        "ranked_author_current_table",
        cluster_by=["scholar_id", "year_of_first_pub"],
    )

    _materialize_from_view(
        "ranked_publication_citations_temporal.sql",
        "ranked_publication_citations_temporal_table",
        cluster_by=["author_pub_id", "pub_year", "citation_year"],
    )

    logger.info("=== Level 3 complete ===")


def materialize_level_4():
    """Level 4: PiP scores + distributions (depends on Levels 1-3).

    Substitution target:
    - stats_author_pip_scores_current_table

    Dist tables:
    - dist_pip_auc_scores
    - dist_author_metrics_temporal
    """
    logger.info("=== Level 4: PiP scores + distributions ===")

    _materialize_from_view(
        "stats_author_pip_scores_current.sql",
        "stats_author_pip_scores_current_table",
        cluster_by=["scholar_id", "year_of_first_pub"],
    )

    _materialize_dist(
        "dist_pip_auc_scores.sql",
        "dist_pip_auc_scores",
    )

    _materialize_dist(
        "dist_author_metrics_temporal.sql",
        "dist_author_metrics_temporal",
    )

    logger.info("=== Level 4 complete ===")


def materialize_level_5():
    """Level 5: Ranked PiP + temporal ranked (depends on Levels 1-4).

    App-facing:
    - ranked_author_pip_scores_current_table
    - ranked_author_metrics_temporal_table

    Substitution target:
    - stats_author_pip_scores_temporal_table
    """
    logger.info("=== Level 5: Ranked PiP + temporal ranked ===")

    _materialize_from_view(
        "ranked_author_pip_scores_current.sql",
        "ranked_author_pip_scores_current_table",
        cluster_by=["scholar_id"],
    )

    _materialize_from_view(
        "ranked_author_metrics_temporal.sql",
        "ranked_author_metrics_temporal_table",
        cluster_by=["scholar_id", "state_year"],
    )

    _materialize_from_view(
        "stats_author_pip_scores_temporal.sql",
        "stats_author_pip_scores_temporal_table",
        cluster_by=["scholar_id", "state_year"],
    )

    logger.info("=== Level 5 complete ===")


def materialize_level_6():
    """Level 6: Temporal PiP distribution (depends on Level 5).

    Dist tables:
    - dist_pip_auc_scores_temporal
    """
    logger.info("=== Level 6: Temporal PiP distribution ===")

    _materialize_dist(
        "dist_pip_auc_scores_temporal.sql",
        "dist_pip_auc_scores_temporal",
    )

    logger.info("=== Level 6 complete ===")


def materialize_all():
    """Materialize the analytics DAG in topological order.

    Materializes 16 tables across 6 levels. Intermediate views
    (base_author_publications, stats_publication_current,
    intermediate_author_publication_state_temporal,
    stats_author_publication_pip_inputs_current,
    ranked_author_pip_scores_temporal) are left as views — their
    output is consumed inline by downstream materialized tables.
    """
    logger.info("Starting DAG materialization (16 tables, 6 levels)...")

    materialize_level_1()
    materialize_level_2()
    materialize_level_3()
    materialize_level_4()
    materialize_level_5()
    materialize_level_6()

    logger.info("DAG materialization complete (16 tables).")
