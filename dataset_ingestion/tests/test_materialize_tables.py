"""Tests for materialize_tables.py."""

from unittest.mock import patch, MagicMock, call

import pytest

from dataset_ingestion import materialize_tables
from dataset_ingestion.config import Config


class TestViewToTableSql:
    def test_converts_simple_view(self):
        view_sql = """CREATE OR REPLACE VIEW `scholar-version2.statistics.my_view` AS
SELECT col1, col2 FROM `scholar-version2.s2_data.papers`"""
        result = materialize_tables._view_to_table_sql(
            view_sql, "my_table", cluster_by=["col1"]
        )
        assert "CREATE OR REPLACE TABLE" in result
        assert Config.bq_stats_table_ref("my_table") in result
        assert "CLUSTER BY col1" in result
        assert "SELECT col1, col2 FROM" in result
        assert "CREATE OR REPLACE VIEW" not in result

    def test_preserves_complex_select(self):
        view_sql = """CREATE OR REPLACE VIEW `scholar-version2.statistics.test_view` AS
WITH cte AS (SELECT 1) SELECT * FROM cte;"""
        result = materialize_tables._view_to_table_sql(
            view_sql, "test_table", cluster_by=["a", "b"]
        )
        assert "CLUSTER BY a, b" in result
        assert "WITH cte AS" in result

    def test_multiple_cluster_columns(self):
        view_sql = """CREATE OR REPLACE VIEW `scholar-version2.statistics.v` AS
SELECT a, b, c FROM t"""
        result = materialize_tables._view_to_table_sql(
            view_sql, "tbl", cluster_by=["a", "b", "c"]
        )
        assert "CLUSTER BY a, b, c" in result

    def test_raises_on_missing_view_keyword(self):
        with pytest.raises(ValueError, match="Could not find VIEW keyword"):
            materialize_tables._view_to_table_sql(
                "SELECT 1", "tbl", cluster_by=["a"]
            )

    def test_raises_on_missing_as_keyword(self):
        with pytest.raises(ValueError, match="Could not find AS keyword"):
            materialize_tables._view_to_table_sql(
                "CREATE OR REPLACE VIEW `foo`\nSELECT 1", "tbl", cluster_by=["a"]
            )

    def test_strips_trailing_semicolon(self):
        view_sql = """CREATE OR REPLACE VIEW `scholar-version2.statistics.v` AS
SELECT 1;"""
        result = materialize_tables._view_to_table_sql(
            view_sql, "tbl", cluster_by=["a"]
        )
        assert not result.rstrip().endswith(";")


class TestReadSql:
    def test_reads_existing_sql_file(self):
        sql = materialize_tables._read_sql("base_author_publications.sql")
        assert "CREATE OR REPLACE VIEW" in sql
        assert "base_author_publications" in sql

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            materialize_tables._read_sql("nonexistent_file.sql")


class TestRunSql:
    @patch.object(materialize_tables, "_get_bq_client")
    def test_executes_query(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.destination = None
        mock_client.query.return_value = mock_job

        materialize_tables._run_sql("SELECT 1", "test")

        mock_client.query.assert_called_once()
        mock_job.result.assert_called_once()

    @patch.object(materialize_tables, "_get_bq_client")
    def test_returns_row_count_when_destination_exists(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.destination = "project.dataset.table"
        mock_client.query.return_value = mock_job
        mock_table = MagicMock()
        mock_table.num_rows = 42
        mock_client.get_table.return_value = mock_table

        result = materialize_tables._run_sql("SELECT 1", "test")
        assert result == 42

    @patch.object(materialize_tables, "_get_bq_client")
    def test_passes_retry_to_query(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.destination = None
        mock_job.ddl_target_table = None
        mock_client.query.return_value = mock_job

        materialize_tables._run_sql("SELECT 1", "test")

        # Verify retry was passed to both query() and result()
        _, kwargs = mock_client.query.call_args
        assert "retry" in kwargs
        _, result_kwargs = mock_job.result.call_args
        assert "retry" in result_kwargs

    @patch.object(materialize_tables, "_get_bq_client")
    def test_returns_none_when_no_destination(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.destination = None
        mock_job.ddl_target_table = None
        mock_client.query.return_value = mock_job

        result = materialize_tables._run_sql("SELECT 1", "test")
        assert result is None

    @patch.object(materialize_tables, "_get_bq_client")
    def test_uses_ddl_target_table_fallback(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.destination = None
        mock_job.ddl_target_table = "project.dataset.table"
        mock_client.query.return_value = mock_job
        mock_table = MagicMock()
        mock_table.num_rows = 99
        mock_client.get_table.return_value = mock_table

        result = materialize_tables._run_sql("SELECT 1", "test")
        assert result == 99
        mock_client.get_table.assert_called_once_with("project.dataset.table")


class TestApplyTableSubstitutions:
    def test_substitutes_known_views(self):
        sql = "FROM `scholar-version2.statistics.stats_author_metrics_temporal_view`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert "stats_author_metrics_temporal_table`" in result
        assert "stats_author_metrics_temporal_view" not in result

    def test_substitutes_publication_citations_temporal(self):
        sql = "FROM `scholar-version2.statistics.stats_publication_citations_temporal`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert "stats_publication_citations_temporal_table`" in result

    def test_substitutes_ranked_publication_current(self):
        sql = "FROM `scholar-version2.statistics.ranked_publication_current`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert "ranked_publication_current_table`" in result

    def test_substitutes_pip_scores_current(self):
        sql = "FROM `scholar-version2.statistics.stats_author_pip_scores_current`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert "stats_author_pip_scores_current_table`" in result

    def test_substitutes_pip_scores_temporal(self):
        sql = "FROM `scholar-version2.statistics.stats_author_pip_scores_temporal_view`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert "stats_author_pip_scores_temporal_table`" in result

    def test_substitutes_ranked_author_current(self):
        sql = "FROM `scholar-version2.statistics.ranked_author_current`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert "ranked_author_current_table`" in result

    def test_substitutes_base_author_publications(self):
        sql = "FROM `scholar-version2.statistics.base_author_publications`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert "base_author_publications_table`" in result

    def test_substitutes_stats_publication_current(self):
        sql = "FROM `scholar-version2.statistics.stats_publication_current`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert "stats_publication_current_table`" in result

    def test_substitutes_intermediate_temporal(self):
        sql = "FROM `scholar-version2.statistics.intermediate_author_publication_state_temporal`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert "intermediate_author_publication_state_temporal_table`" in result

    def test_substitutes_pip_inputs(self):
        sql = "FROM `scholar-version2.statistics.stats_author_publication_pip_inputs_current`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert "stats_author_publication_pip_inputs_current_table`" in result

    def test_leaves_unknown_views_unchanged(self):
        sql = "FROM `scholar-version2.statistics.some_unknown_view`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert result == sql

    def test_respects_custom_dataset(self):
        with patch.object(Config, 'BQ_STATS_DATASET', 'custom_stats'):
            sql = "FROM `scholar-version2.custom_stats.ranked_publication_current`"
            result = materialize_tables._apply_table_substitutions(sql)
            assert "custom_stats.ranked_publication_current_table`" in result


class TestValidateRowCount:
    def test_raises_on_zero_rows(self):
        with pytest.raises(RuntimeError, match="0 rows"):
            materialize_tables._validate_row_count("my_table", 0)

    def test_warns_on_low_row_count(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            materialize_tables._validate_row_count("my_table", 50)
        assert "only 50 rows" in caplog.text

    def test_passes_on_normal_row_count(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            materialize_tables._validate_row_count("my_table", 1000)
        assert caplog.text == ""

    def test_warns_on_none_row_count(self, caplog):
        """Row count unavailable (None) should warn, not raise."""
        import logging
        with caplog.at_level(logging.WARNING):
            materialize_tables._validate_row_count("my_table", None)
        assert "row count unavailable" in caplog.text

    def test_none_does_not_raise(self):
        """Ensure None row count doesn't raise RuntimeError."""
        # Should not raise
        materialize_tables._validate_row_count("my_table", None)


class TestMaterializeOne:
    @patch.object(materialize_tables, "_get_bq_client")
    def test_materializes_view_table(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.destination = "project.dataset.table"
        mock_client.query.return_value = mock_job
        mock_table = MagicMock()
        mock_table.num_rows = 1000
        mock_client.get_table.return_value = mock_table

        spec = {
            "sql_file": "base_author_publications.sql",
            "table_name": "base_author_publications_table",
            "cluster_by": ["scholar_id"],
        }
        name, rows = materialize_tables._materialize_one(spec)

        assert name == "base_author_publications_table"
        assert rows == 1000
        sql = mock_client.query.call_args[0][0]
        assert "CREATE OR REPLACE TABLE" in sql
        assert "CLUSTER BY scholar_id" in sql

    @patch.object(materialize_tables, "_get_bq_client")
    def test_materializes_dist_table(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.destination = "project.dataset.table"
        mock_client.query.return_value = mock_job
        mock_table = MagicMock()
        mock_table.num_rows = 500
        mock_client.get_table.return_value = mock_table

        spec = {
            "sql_file": "dist_publication_citations.sql",
            "table_name": "dist_publication_citations",
            "is_dist": True,
        }
        name, rows = materialize_tables._materialize_one(spec)

        assert name == "dist_publication_citations"
        assert rows == 500
        sql = mock_client.query.call_args[0][0]
        assert "CREATE OR REPLACE TABLE" in sql


class TestMaterializeLevel:
    @patch.object(materialize_tables, "_materialize_one")
    def test_materializes_all_tables_in_level(self, mock_one):
        mock_one.side_effect = [
            ("table_a", 100),
            ("table_b", 200),
            ("table_c", 300),
        ]
        level_spec = {
            "level": 1,
            "name": "Test",
            "tables": [
                {"sql_file": "a.sql", "table_name": "table_a", "cluster_by": ["x"]},
                {"sql_file": "b.sql", "table_name": "table_b", "cluster_by": ["y"]},
                {"sql_file": "c.sql", "table_name": "table_c", "cluster_by": ["z"]},
            ],
        }

        results = materialize_tables._materialize_level(level_spec, max_workers=1)

        assert len(results) == 3
        assert results["table_a"] == 100
        assert results["table_b"] == 200
        assert results["table_c"] == 300

    @patch.object(materialize_tables, "_materialize_one")
    def test_parallel_execution(self, mock_one):
        mock_one.side_effect = [
            ("table_a", 100),
            ("table_b", 200),
        ]
        level_spec = {
            "level": 1,
            "name": "Test",
            "tables": [
                {"sql_file": "a.sql", "table_name": "table_a", "cluster_by": ["x"]},
                {"sql_file": "b.sql", "table_name": "table_b", "cluster_by": ["y"]},
            ],
        }

        results = materialize_tables._materialize_level(level_spec, max_workers=4)

        assert len(results) == 2
        assert mock_one.call_count == 2

    @patch.object(materialize_tables, "_materialize_one")
    def test_raises_on_table_failure(self, mock_one):
        mock_one.side_effect = [
            ("table_a", 100),
            RuntimeError("BQ failed"),
        ]
        level_spec = {
            "level": 2,
            "name": "Test",
            "tables": [
                {"sql_file": "a.sql", "table_name": "table_a", "cluster_by": ["x"]},
                {"sql_file": "b.sql", "table_name": "table_b", "cluster_by": ["y"]},
            ],
        }

        with pytest.raises(RuntimeError, match="Level 2 failed"):
            materialize_tables._materialize_level(level_spec, max_workers=1)

    @patch.object(materialize_tables, "_materialize_one")
    def test_reports_all_failures_in_parallel(self, mock_one):
        mock_one.side_effect = [
            RuntimeError("fail A"),
            RuntimeError("fail B"),
        ]
        level_spec = {
            "level": 1,
            "name": "Test",
            "tables": [
                {"sql_file": "a.sql", "table_name": "table_a", "cluster_by": ["x"]},
                {"sql_file": "b.sql", "table_name": "table_b", "cluster_by": ["y"]},
            ],
        }

        with pytest.raises(RuntimeError, match="2 table\\(s\\) failed"):
            materialize_tables._materialize_level(level_spec, max_workers=4)

    @patch.object(materialize_tables, "_materialize_one")
    def test_phase_ordering(self, mock_one):
        """Phase 1 tables run before phase 2 tables."""
        call_order = []

        def track_call(spec):
            call_order.append(spec["table_name"])
            return spec["table_name"], 100

        mock_one.side_effect = track_call

        level_spec = {
            "level": 2,
            "name": "Test phases",
            "tables": [
                {"sql_file": "a.sql", "table_name": "producer", "cluster_by": ["x"], "phase": 1},
                {"sql_file": "b.sql", "table_name": "consumer", "cluster_by": ["y"], "phase": 2},
            ],
        }

        results = materialize_tables._materialize_level(level_spec, max_workers=1)
        assert call_order == ["producer", "consumer"]
        assert len(results) == 2

    @patch.object(materialize_tables, "_materialize_one")
    def test_phase_1_failure_skips_phase_2(self, mock_one):
        """If phase 1 fails, phase 2 should not run."""
        mock_one.side_effect = RuntimeError("phase 1 failed")

        level_spec = {
            "level": 2,
            "name": "Test",
            "tables": [
                {"sql_file": "a.sql", "table_name": "producer", "cluster_by": ["x"], "phase": 1},
                {"sql_file": "b.sql", "table_name": "consumer", "cluster_by": ["y"], "phase": 2},
            ],
        }

        with pytest.raises(RuntimeError, match="Level 2 failed"):
            materialize_tables._materialize_level(level_spec, max_workers=1)
        # Only phase 1 table was attempted
        assert mock_one.call_count == 1

    @patch.object(materialize_tables, "_materialize_one")
    def test_default_phase_is_1(self, mock_one):
        """Tables without explicit phase default to phase 1 (all parallel)."""
        mock_one.side_effect = [("a", 100), ("b", 200)]

        level_spec = {
            "level": 1,
            "name": "Test",
            "tables": [
                {"sql_file": "a.sql", "table_name": "a", "cluster_by": ["x"]},
                {"sql_file": "b.sql", "table_name": "b", "cluster_by": ["y"]},
            ],
        }

        results = materialize_tables._materialize_level(level_spec, max_workers=4)
        assert len(results) == 2


class TestMaterializeLevels:
    """Test that individual level functions delegate to _materialize_level correctly."""

    @patch.object(materialize_tables, "_materialize_level")
    def test_level_1_delegates(self, mock_level):
        mock_level.return_value = {}
        materialize_tables.materialize_level_1()
        mock_level.assert_called_once_with(materialize_tables._LEVELS[0])

    @patch.object(materialize_tables, "_materialize_level")
    def test_level_2_delegates(self, mock_level):
        mock_level.return_value = {}
        materialize_tables.materialize_level_2()
        mock_level.assert_called_once_with(materialize_tables._LEVELS[1])

    @patch.object(materialize_tables, "_materialize_level")
    def test_level_3_delegates(self, mock_level):
        mock_level.return_value = {}
        materialize_tables.materialize_level_3()
        mock_level.assert_called_once_with(materialize_tables._LEVELS[2])

    @patch.object(materialize_tables, "_materialize_level")
    def test_level_4_delegates(self, mock_level):
        mock_level.return_value = {}
        materialize_tables.materialize_level_4()
        mock_level.assert_called_once_with(materialize_tables._LEVELS[3])

    @patch.object(materialize_tables, "_materialize_level")
    def test_level_5_delegates(self, mock_level):
        mock_level.return_value = {}
        materialize_tables.materialize_level_5()
        mock_level.assert_called_once_with(materialize_tables._LEVELS[4])

    @patch.object(materialize_tables, "_materialize_level")
    def test_level_6_delegates(self, mock_level):
        mock_level.return_value = {}
        materialize_tables.materialize_level_6()
        mock_level.assert_called_once_with(materialize_tables._LEVELS[5])

    @patch.object(materialize_tables, "_materialize_level")
    def test_level_7_delegates(self, mock_level):
        mock_level.return_value = {}
        materialize_tables.materialize_level_7()
        mock_level.assert_called_once_with(materialize_tables._LEVELS[6])

    def test_level_1_has_5_tables(self):
        assert len(materialize_tables._LEVELS[0]["tables"]) == 5

    def test_level_2_has_4_tables(self):
        assert len(materialize_tables._LEVELS[1]["tables"]) == 4

    def test_level_3_has_4_tables(self):
        assert len(materialize_tables._LEVELS[2]["tables"]) == 4

    def test_level_4_has_3_tables(self):
        assert len(materialize_tables._LEVELS[3]["tables"]) == 3

    def test_level_5_has_3_tables(self):
        assert len(materialize_tables._LEVELS[4]["tables"]) == 3

    def test_level_6_has_1_table(self):
        assert len(materialize_tables._LEVELS[5]["tables"]) == 1

    def test_level_7_has_1_table(self):
        assert len(materialize_tables._LEVELS[6]["tables"]) == 1


class TestMaterializeAll:
    @patch.object(materialize_tables, "_materialize_level")
    def test_calls_all_levels_in_order(self, mock_level):
        mock_level.return_value = {"t": 100}

        materialize_tables.materialize_all()

        assert mock_level.call_count == 7
        # Verify levels called in order 1-7
        for i, call_args in enumerate(mock_level.call_args_list):
            assert call_args[0][0]["level"] == i + 1

    @patch.object(materialize_tables, "_materialize_level")
    def test_stops_on_level_failure(self, mock_level):
        mock_level.side_effect = [
            {"t1": 100},
            {"t2": 200},
            RuntimeError("BQ failed"),
        ]

        with pytest.raises(RuntimeError, match="BQ failed"):
            materialize_tables.materialize_all()

        assert mock_level.call_count == 3

    @patch.object(materialize_tables, "_materialize_level")
    def test_start_from_level_skips_earlier(self, mock_level):
        mock_level.return_value = {"t": 100}

        materialize_tables.materialize_all(start_from_level=4)

        # Should only call levels 4, 5, 6, 7
        assert mock_level.call_count == 4
        levels_called = [c[0][0]["level"] for c in mock_level.call_args_list]
        assert levels_called == [4, 5, 6, 7]

    @patch.object(materialize_tables, "_materialize_level")
    def test_start_from_level_1_runs_all(self, mock_level):
        mock_level.return_value = {"t": 100}

        materialize_tables.materialize_all(start_from_level=1)

        assert mock_level.call_count == 7

    def test_start_from_level_0_raises(self):
        with pytest.raises(ValueError, match="start_from_level must be 1-7"):
            materialize_tables.materialize_all(start_from_level=0)

    def test_start_from_level_8_raises(self):
        with pytest.raises(ValueError, match="start_from_level must be 1-7"):
            materialize_tables.materialize_all(start_from_level=8)

    @patch.object(materialize_tables, "_materialize_level")
    def test_returns_total_table_count(self, mock_level):
        mock_level.side_effect = [
            {"a": 1, "b": 2},
            {"c": 3},
            {"d": 4, "e": 5},
            {"f": 6},
            {"g": 7, "h": 8},
            {"i": 9},
            {"j": 10},
        ]

        total = materialize_tables.materialize_all()
        assert total == 10


class TestLevelsDataIntegrity:
    """Verify the _LEVELS data structure is internally consistent."""

    def test_levels_are_numbered_1_through_7(self):
        assert [l["level"] for l in materialize_tables._LEVELS] == [1, 2, 3, 4, 5, 6, 7]

    def test_all_tables_have_required_keys(self):
        for level in materialize_tables._LEVELS:
            for table in level["tables"]:
                assert "sql_file" in table
                assert "table_name" in table
                # Non-dist tables must have cluster_by
                if not table.get("is_dist"):
                    assert "cluster_by" in table, f"{table['table_name']} missing cluster_by"

    def test_total_table_count(self):
        total = sum(len(l["tables"]) for l in materialize_tables._LEVELS)
        assert total == 21  # 5 + 4 + 4 + 3 + 3 + 1 + 1

    def test_all_sql_files_exist(self):
        """Verify every referenced SQL file actually exists."""
        for level in materialize_tables._LEVELS:
            for table in level["tables"]:
                path = materialize_tables._SQL_DIR / table["sql_file"]
                assert path.exists(), f"SQL file missing: {table['sql_file']}"

    def test_level_2_has_phase_ordering(self):
        """Level 2 must have phase 1 (producers) before phase 2 (consumers)."""
        tables = materialize_tables._LEVELS[1]["tables"]
        phase_1 = [t for t in tables if t.get("phase", 1) == 1]
        phase_2 = [t for t in tables if t.get("phase", 1) == 2]
        assert len(phase_1) == 2  # temporal + ranked
        assert len(phase_2) == 2  # intermediate + dist

    def test_level_4_has_phase_ordering(self):
        """Level 4 must have phase 1 (pip scores) before phase 2 (dist_pip_auc)."""
        tables = materialize_tables._LEVELS[3]["tables"]
        phase_1 = [t for t in tables if t.get("phase", 1) == 1]
        phase_2 = [t for t in tables if t.get("phase", 1) == 2]
        assert len(phase_1) == 2  # pip scores + dist_author_metrics_temporal
        assert len(phase_2) == 1  # dist_pip_auc_scores
