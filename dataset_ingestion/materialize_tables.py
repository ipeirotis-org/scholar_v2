"""Materialize the full BigQuery analytics DAG into tables.

Replaces all live views with pre-computed tables after S2 dataset ingestion.
Data is static between bulk loads — live views waste compute on every query.

The DAG has 7 levels (1-7). Each level depends on the previous ones.
Tables within a level can run in parallel, but are executed sequentially
here for simplicity and to stay within BigQuery concurrency limits.

All percentile lookups use RANGE_BUCKET + pre-aggregated arrays for O(log n)
floor lookups. This replaces correlated scalar subqueries and makes
materializing even large tables (2B+ rows) fast.

Architecture:
  - Level 1: Foundation (raw data views → tables, dist tables)
  - Level 2: Temporal foundation + first ranked + dist
  - Level 3: Metrics + PiP inputs + more ranked
  - Level 4: PiP scores + higher distributions
  - Level 5: Ranked PiP + temporal ranked
  - Level 6: Temporal PiP distribution
  - Level 7: Temporal PiP ranked
"""

import logging
import pathlib
import re

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
#
# Keys use the configured stats dataset (not hardcoded) so overrides work.
#
# Note: the SQL files themselves hard-code `scholar-version2.statistics.`
# because they're deployed as standalone views by bigquery-views.yml.
# If BQ_STATS_DATASET is overridden, the SQL files must also be updated
# (or deployed to the custom dataset) for substitutions to match.
_SUBSTITUTION_PAIRS = [
    # Level 1 views → tables (used by Level 2+)
    ("base_author_publications`", "base_author_publications_table`"),
    ("stats_publication_current`", "stats_publication_current_table`"),
    ("stats_author_current`", "stats_author_current_table`"),
    # Level 2 views → tables (used by Level 3+)
    ("stats_publication_citations_temporal`", "stats_publication_citations_temporal_table`"),
    ("ranked_publication_current`", "ranked_publication_current_table`"),
    ("intermediate_author_publication_state_temporal`", "intermediate_author_publication_state_temporal_table`"),
    # Level 3 views → tables (used by Level 4+)
    ("stats_author_metrics_temporal_view`", "stats_author_metrics_temporal_table`"),
    ("stats_author_publication_pip_inputs_current`", "stats_author_publication_pip_inputs_current_table`"),
    ("ranked_author_current`", "ranked_author_current_table`"),
    # Level 4 views → tables (used by Level 5+)
    ("stats_author_pip_scores_current`", "stats_author_pip_scores_current_table`"),
    # Level 5 views → tables (used by Level 6+)
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
    """Replace view references with materialized table references.

    SQL files reference views so they work standalone. During pipeline
    execution, we substitute _table names so downstream levels read from
    previously materialized tables instead of re-executing view chains.
    """
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


def materialize_level_1():
    """Level 1: Foundation tables (parallel, independent).

    - base_author_publications_table
    - stats_publication_current_table
    - stats_author_current_table
    - dist_publication_citations (already CREATE TABLE in SQL)
    - dist_author_metrics (already CREATE TABLE in SQL)
    """
    logger.info("=== Level 1: Foundation ===")

    _materialize_from_view(
        "base_author_publications.sql",
        "base_author_publications_table",
        cluster_by=["scholar_id"],
    )

    _materialize_from_view(
        "stats_publication_current.sql",
        "stats_publication_current_table",
        cluster_by=["author_pub_id", "pub_year"],
    )

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
    """Level 2: Temporal foundation + first ranked (depends on Level 1).

    - stats_publication_citations_temporal_table
    - ranked_publication_current_table
    - intermediate_author_publication_state_temporal_table
    - dist_publication_citations_temporal (already CREATE TABLE in SQL)
    """
    logger.info("=== Level 2: Temporal foundation + first ranked ===")

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

    _materialize_from_view(
        "intermediate_author_publication_state_temporal.sql",
        "intermediate_author_publication_state_temporal_table",
        cluster_by=["scholar_id", "author_pub_id", "state_year"],
    )

    _materialize_dist(
        "dist_publication_citations_temporal.sql",
        "dist_publication_citations_temporal",
    )

    logger.info("=== Level 2 complete ===")


def materialize_level_3():
    """Level 3: Metrics + PiP inputs + ranked (depends on Levels 1-2).

    - stats_author_metrics_temporal_table
    - stats_author_publication_pip_inputs_current_table
    - ranked_author_current_table
    - ranked_publication_citations_temporal_table
    """
    logger.info("=== Level 3: Metrics + PiP inputs + ranked ===")

    _materialize_from_view(
        "stats_author_metrics_temporal.sql",
        "stats_author_metrics_temporal_table",
        cluster_by=["scholar_id", "state_year"],
    )

    _materialize_from_view(
        "stats_author_publication_pip_inputs_current.sql",
        "stats_author_publication_pip_inputs_current_table",
        cluster_by=["scholar_id"],
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

    - stats_author_pip_scores_current_table
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

    - ranked_author_pip_scores_current_table
    - ranked_author_metrics_temporal_table
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

    - dist_pip_auc_scores_temporal
    """
    logger.info("=== Level 6: Temporal PiP distribution ===")

    _materialize_dist(
        "dist_pip_auc_scores_temporal.sql",
        "dist_pip_auc_scores_temporal",
    )

    logger.info("=== Level 6 complete ===")


def materialize_level_7():
    """Level 7: Temporal PiP ranked (depends on Levels 5-6).

    - ranked_author_pip_scores_temporal_table
    """
    logger.info("=== Level 7: Temporal PiP ranked ===")

    _materialize_from_view(
        "ranked_author_pip_scores_temporal.sql",
        "ranked_author_pip_scores_temporal_table",
        cluster_by=["scholar_id", "state_year"],
    )

    logger.info("=== Level 7 complete ===")


def materialize_all():
    """Materialize the entire analytics DAG in topological order.

    Executes all 7 levels sequentially. Each level depends on
    the previous ones being complete.

    Returns the total number of tables materialized.
    """
    logger.info("Starting full DAG materialization...")

    materialize_level_1()
    materialize_level_2()
    materialize_level_3()
    materialize_level_4()
    materialize_level_5()
    materialize_level_6()
    materialize_level_7()

    logger.info("Full DAG materialization complete.")
