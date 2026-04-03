"""Materialize the full BigQuery analytics DAG into tables.

Replaces all live views with pre-computed tables after S2 dataset ingestion.
Data is static between bulk loads — live views waste compute on every query.

The DAG has 7 levels (1-7). Each level depends on the previous ones.
Tables within a level run in parallel using ThreadPoolExecutor (configurable
via BQ_MATERIALIZE_WORKERS). Levels run sequentially.

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
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.api_core import exceptions as google_exceptions
from google.api_core import retry as google_retry
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

# Retryable BigQuery exceptions (transient server/quota errors).
_RETRYABLE_EXCEPTIONS = (
    google_exceptions.InternalServerError,
    google_exceptions.ServiceUnavailable,
    google_exceptions.TooManyRequests,
)

# Retry predicate for BigQuery queries.
_BQ_RETRY = google_retry.Retry(
    predicate=google_retry.if_exception_type(*_RETRYABLE_EXCEPTIONS),
    initial=5.0,
    maximum=60.0,
    multiplier=2.0,
    deadline=600.0,
)

# ---------------------------------------------------------------------------
# Data-driven DAG definition
# ---------------------------------------------------------------------------
# Each level is a dict with: level (int), name (str), tables (list of dicts).
# Table dicts have: sql_file, table_name, cluster_by, and optional
# partition_by, is_dist (for distribution tables that are already
# CREATE TABLE statements), and phase (int, default 1).
#
# Phase controls execution order WITHIN a level: all phase-1 tables run
# (possibly in parallel) before any phase-2 tables start. This handles
# intra-level dependencies introduced by _apply_table_substitutions —
# e.g. in Level 2, intermediate_author_publication_state_temporal reads
# stats_publication_citations_temporal_table (after substitution), so
# the producer must finish before the consumer starts.
# ---------------------------------------------------------------------------

_LEVELS = [
    {
        "level": 1,
        "name": "Foundation",
        "tables": [
            {"sql_file": "base_author_publications.sql", "table_name": "base_author_publications_table", "cluster_by": ["scholar_id"]},
            {"sql_file": "stats_publication_current.sql", "table_name": "stats_publication_current_table", "cluster_by": ["author_pub_id", "pub_year"]},
            {"sql_file": "stats_author_current.sql", "table_name": "stats_author_current_table", "cluster_by": ["scholar_id", "year_of_first_pub"]},
            {"sql_file": "dist_publication_citations.sql", "table_name": "dist_publication_citations", "is_dist": True},
            {"sql_file": "dist_author_metrics.sql", "table_name": "dist_author_metrics", "is_dist": True},
        ],
    },
    {
        "level": 2,
        "name": "Temporal foundation + first ranked",
        "tables": [
            # Phase 1: producers (no intra-level deps)
            {"sql_file": "stats_publication_citations_temporal.sql", "table_name": "stats_publication_citations_temporal_table", "cluster_by": ["author_pub_id", "pub_year", "citation_year"], "phase": 1},
            {"sql_file": "ranked_publication_current.sql", "table_name": "ranked_publication_current_table", "cluster_by": ["author_pub_id", "pub_year"], "phase": 1},
            # Phase 2: consumers (read stats_publication_citations_temporal_table after substitution)
            {"sql_file": "intermediate_author_publication_state_temporal.sql", "table_name": "intermediate_author_publication_state_temporal_table", "cluster_by": ["scholar_id", "author_pub_id", "state_year"], "phase": 2},
            {"sql_file": "dist_publication_citations_temporal.sql", "table_name": "dist_publication_citations_temporal", "is_dist": True, "phase": 2},
        ],
    },
    {
        "level": 3,
        "name": "Metrics + PiP inputs + ranked",
        "tables": [
            {"sql_file": "stats_author_metrics_temporal.sql", "table_name": "stats_author_metrics_temporal_table", "cluster_by": ["scholar_id", "state_year"]},
            {"sql_file": "stats_author_publication_pip_inputs_current.sql", "table_name": "stats_author_publication_pip_inputs_current_table", "cluster_by": ["scholar_id"]},
            {"sql_file": "ranked_author_current.sql", "table_name": "ranked_author_current_table", "cluster_by": ["scholar_id", "year_of_first_pub"]},
            {"sql_file": "ranked_publication_citations_temporal.sql", "table_name": "ranked_publication_citations_temporal_table", "cluster_by": ["author_pub_id", "pub_year", "citation_year"]},
        ],
    },
    {
        "level": 4,
        "name": "PiP scores + distributions",
        "tables": [
            # Phase 1: producer
            {"sql_file": "stats_author_pip_scores_current.sql", "table_name": "stats_author_pip_scores_current_table", "cluster_by": ["scholar_id", "year_of_first_pub"], "phase": 1},
            # Phase 2: dist_pip_auc_scores reads stats_author_pip_scores_current_table after substitution
            {"sql_file": "dist_pip_auc_scores.sql", "table_name": "dist_pip_auc_scores", "is_dist": True, "phase": 2},
            # dist_author_metrics_temporal reads from Level 3, no intra-level dep
            {"sql_file": "dist_author_metrics_temporal.sql", "table_name": "dist_author_metrics_temporal", "is_dist": True, "phase": 1},
        ],
    },
    {
        "level": 5,
        "name": "Ranked PiP + temporal ranked",
        "tables": [
            {"sql_file": "ranked_author_pip_scores_current.sql", "table_name": "ranked_author_pip_scores_current_table", "cluster_by": ["scholar_id"]},
            {"sql_file": "ranked_author_metrics_temporal.sql", "table_name": "ranked_author_metrics_temporal_table", "cluster_by": ["scholar_id", "state_year"]},
            {"sql_file": "stats_author_pip_scores_temporal.sql", "table_name": "stats_author_pip_scores_temporal_table", "cluster_by": ["scholar_id", "state_year"]},
        ],
    },
    {
        "level": 6,
        "name": "Temporal PiP distribution",
        "tables": [
            {"sql_file": "dist_pip_auc_scores_temporal.sql", "table_name": "dist_pip_auc_scores_temporal", "is_dist": True},
        ],
    },
    {
        "level": 7,
        "name": "Temporal PiP ranked",
        "tables": [
            {"sql_file": "ranked_author_pip_scores_temporal.sql", "table_name": "ranked_author_pip_scores_temporal_table", "cluster_by": ["scholar_id", "state_year"]},
        ],
    },
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
    """Execute a SQL statement with retry and timing, and return the row count.

    Retries on transient BigQuery errors (503, 429, 500) with exponential
    backoff up to a 600s deadline.

    Returns:
        int row count if available, or None if the row count could not
        be determined (e.g. DDL without a destination table).
    """
    client = _get_bq_client()
    logger.info("Materializing: %s", description)
    t0 = time.monotonic()

    # retry: retries transport/RPC errors (connection resets, DNS failures)
    # job_retry: retries BigQuery job-level failures (internalError, etc.)
    job = client.query(sql, retry=_BQ_RETRY, job_retry=_BQ_RETRY)
    job.result(retry=_BQ_RETRY)

    elapsed = time.monotonic() - t0
    row_count = None

    # Try destination first (set for CTAS queries), then ddl_target_table
    # (set for DDL statements like CREATE OR REPLACE TABLE).
    target = job.destination
    if target is None and hasattr(job, "ddl_target_table"):
        target = job.ddl_target_table

    if target:
        try:
            table = client.get_table(target)
            row_count = table.num_rows
            logger.info("  %s: %d rows in %.1fs", description, row_count, elapsed)
        except Exception:
            logger.info("  %s: complete in %.1fs (row count unavailable)", description, elapsed)
    else:
        logger.info("  %s: complete in %.1fs", description, elapsed)

    return row_count


def _apply_table_substitutions(sql):
    """Replace view references with materialized table references.

    SQL files reference views so they work standalone. During pipeline
    execution, we substitute _table names so downstream levels read from
    previously materialized tables instead of re-executing view chains.
    """
    for view_ref, table_ref in _get_table_substitutions().items():
        sql = sql.replace(view_ref, table_ref)
    return sql


def _validate_row_count(table_name, row_count):
    """Validate that a materialized table has a non-zero row count.

    Args:
        table_name: Name of the table (for error messages).
        row_count: Number of rows, or None if unavailable.

    Raises RuntimeError for zero-row tables (indicates silent data loss).
    Logs warnings for unavailable or suspiciously low counts.
    """
    if row_count is None:
        logger.warning(
            "Table %s: row count unavailable after materialization — "
            "cannot validate; verify manually",
            table_name,
        )
        return
    if row_count == 0:
        raise RuntimeError(
            f"Table {table_name} has 0 rows after materialization. "
            "This likely indicates a data issue upstream."
        )
    if row_count < 100:
        logger.warning(
            "Table %s has only %d rows — verify this is expected",
            table_name, row_count,
        )


def _materialize_one(table_spec):
    """Materialize a single table from its spec dict. Returns (table_name, row_count)."""
    sql_file = table_spec["sql_file"]
    table_name = table_spec["table_name"]
    is_dist = table_spec.get("is_dist", False)

    raw_sql = _read_sql(sql_file)

    if is_dist:
        sql = _apply_table_substitutions(raw_sql)
    else:
        cluster_by = table_spec["cluster_by"]
        partition_by = table_spec.get("partition_by")
        sql = _view_to_table_sql(raw_sql, table_name, cluster_by, partition_by)
        sql = _apply_table_substitutions(sql)

    row_count = _run_sql(sql, table_name)
    _validate_row_count(table_name, row_count)
    return table_name, row_count


def _run_phase(tables, max_workers):
    """Run a group of independent tables, optionally in parallel.

    Returns (results_dict, errors_list) where results maps table_name → row_count.
    """
    results = {}
    errors = []

    if max_workers <= 1 or len(tables) <= 1:
        for spec in tables:
            try:
                name, rows = _materialize_one(spec)
                results[name] = rows
            except Exception as e:
                errors.append((spec["table_name"], e))
    else:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(tables))) as executor:
            future_to_name = {
                executor.submit(_materialize_one, spec): spec["table_name"]
                for spec in tables
            }
            for future in as_completed(future_to_name):
                table_name = future_to_name[future]
                try:
                    name, rows = future.result()
                    results[name] = rows
                except Exception as e:
                    errors.append((table_name, e))

    return results, errors


def _materialize_level(level_spec, max_workers=None):
    """Run all tables in a single DAG level, respecting phase ordering.

    Tables within the same phase run in parallel; phases run sequentially
    (all phase-1 tables complete before phase-2 starts). This ensures
    intra-level dependencies introduced by table substitutions are
    respected — producers finish before consumers start.

    Args:
        level_spec: Dict with 'level', 'name', 'tables' keys.
        max_workers: Max concurrent BQ jobs. None uses Config default.

    Returns:
        Dict mapping table_name → row_count for each materialized table.

    Raises:
        RuntimeError: If any table fails to materialize.
    """
    level_num = level_spec["level"]
    level_name = level_spec["name"]
    tables = level_spec["tables"]

    if max_workers is None:
        max_workers = Config.BQ_MATERIALIZE_WORKERS

    # Group tables by phase (default phase=1)
    phases = {}
    for spec in tables:
        phase = spec.get("phase", 1)
        phases.setdefault(phase, []).append(spec)

    logger.info(
        "=== Level %d: %s (%d tables, %d phase(s), workers=%d) ===",
        level_num, level_name, len(tables), len(phases), max_workers,
    )
    t0 = time.monotonic()

    all_results = {}
    all_errors = []

    for phase_num in sorted(phases):
        phase_tables = phases[phase_num]
        if len(phases) > 1:
            logger.info("  Level %d, phase %d: %d tables", level_num, phase_num, len(phase_tables))
        results, errors = _run_phase(phase_tables, max_workers)
        all_results.update(results)
        if errors:
            all_errors.extend(errors)
            break  # Don't start next phase if current phase failed

    elapsed = time.monotonic() - t0

    if all_errors:
        failed_names = [name for name, _ in all_errors]
        logger.error(
            "=== Level %d FAILED: %d/%d tables failed in %.1fs: %s ===",
            level_num, len(all_errors), len(tables), elapsed, ", ".join(failed_names),
        )
        first_name, first_error = all_errors[0]
        raise RuntimeError(
            f"Level {level_num} failed: {len(all_errors)} table(s) failed "
            f"({', '.join(failed_names)})"
        ) from first_error

    logger.info(
        "=== Level %d complete: %d tables in %.1fs ===",
        level_num, len(all_results), elapsed,
    )
    return all_results


# ---------------------------------------------------------------------------
# Public API: individual level functions (backward compatible)
# ---------------------------------------------------------------------------

def materialize_level_1():
    """Level 1: Foundation tables."""
    return _materialize_level(_LEVELS[0])


def materialize_level_2():
    """Level 2: Temporal foundation + first ranked."""
    return _materialize_level(_LEVELS[1])


def materialize_level_3():
    """Level 3: Metrics + PiP inputs + ranked."""
    return _materialize_level(_LEVELS[2])


def materialize_level_4():
    """Level 4: PiP scores + distributions."""
    return _materialize_level(_LEVELS[3])


def materialize_level_5():
    """Level 5: Ranked PiP + temporal ranked."""
    return _materialize_level(_LEVELS[4])


def materialize_level_6():
    """Level 6: Temporal PiP distribution."""
    return _materialize_level(_LEVELS[5])


def materialize_level_7():
    """Level 7: Temporal PiP ranked."""
    return _materialize_level(_LEVELS[6])


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def materialize_all(start_from_level=1):
    """Materialize the entire analytics DAG in topological order.

    Args:
        start_from_level: Skip levels below this number (1-7).
            Useful for resuming after a partial failure.

    Returns:
        Total number of tables materialized.

    Raises:
        ValueError: If start_from_level is out of range.
        RuntimeError: If any level fails.
    """
    if not 1 <= start_from_level <= 7:
        raise ValueError(f"start_from_level must be 1-7, got {start_from_level}")

    logger.info("Starting DAG materialization (levels %d-7)...", start_from_level)
    t0 = time.monotonic()
    total_tables = 0

    for level_spec in _LEVELS:
        if level_spec["level"] < start_from_level:
            logger.info("Skipping level %d (start_from_level=%d)", level_spec["level"], start_from_level)
            continue
        results = _materialize_level(level_spec)
        total_tables += len(results)

    elapsed = time.monotonic() - t0
    logger.info(
        "Full DAG materialization complete: %d tables in %.1fs",
        total_tables, elapsed,
    )
    return total_tables
