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

        mock_client.query.assert_called_once_with("SELECT 1")
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

    def test_substitutes_pip_scores_current(self):
        sql = "FROM `scholar-version2.statistics.stats_author_pip_scores_current`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert "stats_author_pip_scores_current_table`" in result

    def test_substitutes_pip_scores_temporal(self):
        sql = "FROM `scholar-version2.statistics.stats_author_pip_scores_temporal_view`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert "stats_author_pip_scores_temporal_table`" in result

    def test_leaves_unknown_views_unchanged(self):
        sql = "FROM `scholar-version2.statistics.ranked_author_current`"
        result = materialize_tables._apply_table_substitutions(sql)
        assert result == sql


class TestMaterializeFromView:
    @patch.object(materialize_tables, "_get_bq_client")
    def test_reads_sql_and_executes(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.destination = None
        mock_client.query.return_value = mock_job

        materialize_tables._materialize_from_view(
            "base_author_publications.sql",
            "base_author_publications_table",
            cluster_by=["scholar_id"],
        )

        sql = mock_client.query.call_args[0][0]
        assert "CREATE OR REPLACE TABLE" in sql
        assert "base_author_publications_table" in sql
        assert "CLUSTER BY scholar_id" in sql


class TestMaterializeDist:
    @patch.object(materialize_tables, "_get_bq_client")
    def test_executes_dist_sql_directly(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.destination = None
        mock_client.query.return_value = mock_job

        materialize_tables._materialize_dist(
            "dist_publication_citations.sql",
            "dist_publication_citations",
        )

        sql = mock_client.query.call_args[0][0]
        # dist SQL files already contain CREATE TABLE
        assert "CREATE OR REPLACE TABLE" in sql
        assert "dist_publication_citations" in sql


class TestMaterializeLevels:
    @patch.object(materialize_tables, "_run_sql")
    @patch.object(materialize_tables, "_read_sql")
    def test_level_1_materializes_3_tables(self, mock_read, mock_run):
        mock_read.return_value = "CREATE OR REPLACE VIEW `scholar-version2.statistics.v` AS\nSELECT 1"
        mock_run.return_value = 0

        materialize_tables.materialize_level_1()

        assert mock_run.call_count == 3

    @patch.object(materialize_tables, "_run_sql")
    @patch.object(materialize_tables, "_read_sql")
    def test_level_2_materializes_2_tables(self, mock_read, mock_run):
        mock_read.return_value = "CREATE OR REPLACE VIEW `scholar-version2.statistics.v` AS\nSELECT 1"
        mock_run.return_value = 0

        materialize_tables.materialize_level_2()

        assert mock_run.call_count == 2

    @patch.object(materialize_tables, "_run_sql")
    @patch.object(materialize_tables, "_read_sql")
    def test_level_3_materializes_4_tables(self, mock_read, mock_run):
        mock_read.return_value = "CREATE OR REPLACE VIEW `scholar-version2.statistics.v` AS\nSELECT 1"
        mock_run.return_value = 0

        materialize_tables.materialize_level_3()

        assert mock_run.call_count == 4

    @patch.object(materialize_tables, "_run_sql")
    @patch.object(materialize_tables, "_read_sql")
    def test_level_4_materializes_3_tables(self, mock_read, mock_run):
        mock_read.return_value = "CREATE OR REPLACE VIEW `scholar-version2.statistics.v` AS\nSELECT 1"
        mock_run.return_value = 0

        materialize_tables.materialize_level_4()

        assert mock_run.call_count == 3

    @patch.object(materialize_tables, "_run_sql")
    @patch.object(materialize_tables, "_read_sql")
    def test_level_5_materializes_3_tables(self, mock_read, mock_run):
        mock_read.return_value = "CREATE OR REPLACE VIEW `scholar-version2.statistics.v` AS\nSELECT 1"
        mock_run.return_value = 0

        materialize_tables.materialize_level_5()

        assert mock_run.call_count == 3

    @patch.object(materialize_tables, "_run_sql")
    @patch.object(materialize_tables, "_read_sql")
    def test_level_6_materializes_1_table(self, mock_read, mock_run):
        mock_read.return_value = "CREATE OR REPLACE VIEW `scholar-version2.statistics.v` AS\nSELECT 1"
        mock_run.return_value = 0

        materialize_tables.materialize_level_6()

        assert mock_run.call_count == 1


class TestMaterializeAll:
    @patch.object(materialize_tables, "materialize_level_6")
    @patch.object(materialize_tables, "materialize_level_5")
    @patch.object(materialize_tables, "materialize_level_4")
    @patch.object(materialize_tables, "materialize_level_3")
    @patch.object(materialize_tables, "materialize_level_2")
    @patch.object(materialize_tables, "materialize_level_1")
    def test_calls_all_levels_in_order(self, l1, l2, l3, l4, l5, l6):
        materialize_tables.materialize_all()

        l1.assert_called_once()
        l2.assert_called_once()
        l3.assert_called_once()
        l4.assert_called_once()
        l5.assert_called_once()
        l6.assert_called_once()

    @patch.object(materialize_tables, "materialize_level_6")
    @patch.object(materialize_tables, "materialize_level_5")
    @patch.object(materialize_tables, "materialize_level_4")
    @patch.object(materialize_tables, "materialize_level_3")
    @patch.object(materialize_tables, "materialize_level_2")
    @patch.object(materialize_tables, "materialize_level_1")
    def test_stops_on_level_failure(self, l1, l2, l3, l4, l5, l6):
        l3.side_effect = RuntimeError("BQ failed")

        with pytest.raises(RuntimeError, match="BQ failed"):
            materialize_tables.materialize_all()

        l1.assert_called_once()
        l2.assert_called_once()
        l3.assert_called_once()
        l4.assert_not_called()
        l5.assert_not_called()
        l6.assert_not_called()
