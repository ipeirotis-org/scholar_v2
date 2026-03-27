"""Tests for diff_updater.py."""

from unittest.mock import patch, MagicMock, call

import pytest

from dataset_ingestion import diff_updater
from dataset_ingestion.config import Config


class TestTempTable:
    def test_generates_temp_table_name_with_execution_id(self):
        result = diff_updater._temp_table("papers", "updates")
        assert result.startswith(f"{Config.PROJECT_ID}.{Config.BQ_DATASET}._tmp_papers_updates_")
        # Should contain an 8-char hex execution ID suffix
        suffix = result.split("_tmp_papers_updates_")[1]
        assert len(suffix) == 8

    def test_generates_delete_table_name(self):
        result = diff_updater._temp_table("citations", "deletes")
        assert "_tmp_citations_deletes_" in result

    def test_consistent_execution_id_within_run(self):
        a = diff_updater._temp_table("papers", "updates")
        b = diff_updater._temp_table("papers", "deletes")
        # Both should share the same execution ID suffix
        id_a = a.rsplit("_", 1)[1]
        id_b = b.rsplit("_", 1)[1]
        assert id_a == id_b


class TestApplyDeletes:
    @patch.object(diff_updater, "_get_bq_client")
    def test_deletes_matching_rows(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.num_dml_affected_rows = 42
        mock_client.query.return_value = mock_job

        result = diff_updater._apply_deletes("papers", "project.dataset.tmp_del")

        assert result == 42
        sql = mock_client.query.call_args[0][0]
        assert "DELETE FROM" in sql
        assert "corpusid" in sql
        assert "project.dataset.tmp_del" in sql

    @patch.object(diff_updater, "_get_bq_client")
    def test_uses_correct_primary_key_per_dataset(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.num_dml_affected_rows = 0
        mock_client.query.return_value = mock_job

        diff_updater._apply_deletes("citations", "t")
        sql = mock_client.query.call_args[0][0]
        assert "citationid" in sql

        diff_updater._apply_deletes("authors", "t")
        sql = mock_client.query.call_args[0][0]
        assert "authorid" in sql


class TestApplyUpserts:
    @patch.object(diff_updater, "_get_bq_client")
    def test_merges_into_main_table(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.num_dml_affected_rows = 100
        mock_client.query.return_value = mock_job

        result = diff_updater._apply_upserts("papers", "project.dataset.tmp_upd")

        assert result == 100
        sql = mock_client.query.call_args[0][0]
        assert "MERGE" in sql
        assert "WHEN MATCHED THEN" in sql
        assert "WHEN NOT MATCHED THEN" in sql
        assert "corpusid" in sql

    @patch.object(diff_updater, "_get_bq_client")
    def test_upsert_includes_all_columns(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_job.num_dml_affected_rows = 0
        mock_client.query.return_value = mock_job

        diff_updater._apply_upserts("authors", "tmp")
        sql = mock_client.query.call_args[0][0]
        assert "name" in sql
        assert "hindex" in sql
        assert "papercount" in sql


class TestDropTempTables:
    @patch.object(diff_updater, "_get_bq_client")
    def test_drops_both_temp_tables(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        diff_updater._drop_temp_tables("papers")

        assert mock_client.delete_table.call_count == 2
        calls = [c[0][0] for c in mock_client.delete_table.call_args_list]
        assert any("updates" in c for c in calls)
        assert any("deletes" in c for c in calls)


class TestApplyDiff:
    @patch.object(diff_updater, "_drop_temp_tables")
    @patch.object(diff_updater, "_apply_upserts", return_value=50)
    @patch.object(diff_updater, "_apply_deletes", return_value=10)
    @patch.object(diff_updater, "_load_temp_table", return_value="tmp_table")
    @patch.object(diff_updater, "download_dataset")
    def test_applies_deletes_and_upserts(
        self, mock_download, mock_load, mock_del, mock_ups, mock_drop
    ):
        mock_download.return_value = {"failed": 0, "downloaded": 5}

        diff = {
            "update_files": ["https://example.com/u1.gz"],
            "delete_files": ["https://example.com/d1.gz"],
        }

        result = diff_updater.apply_diff("2026-03-10", "papers", diff)

        assert result["deleted"] == 10
        assert result["upserted"] == 50
        assert mock_del.called
        assert mock_ups.called
        mock_drop.assert_called_once_with("papers")

    @patch.object(diff_updater, "_drop_temp_tables")
    @patch.object(diff_updater, "_apply_upserts", return_value=50)
    @patch.object(diff_updater, "_load_temp_table", return_value="tmp_table")
    @patch.object(diff_updater, "download_dataset")
    def test_handles_updates_only(self, mock_download, mock_load, mock_ups, mock_drop):
        mock_download.return_value = {"failed": 0, "downloaded": 5}

        diff = {"update_files": ["https://example.com/u1.gz"], "delete_files": []}

        result = diff_updater.apply_diff("2026-03-10", "papers", diff)

        assert result["deleted"] == 0
        assert result["upserted"] == 50

    @patch.object(diff_updater, "_drop_temp_tables")
    @patch.object(diff_updater, "download_dataset")
    def test_raises_on_download_failure(self, mock_download, mock_drop):
        mock_download.return_value = {"failed": 2, "downloaded": 3}

        diff = {"update_files": ["https://example.com/u1.gz"], "delete_files": []}

        with pytest.raises(RuntimeError, match="update file downloads failed"):
            diff_updater.apply_diff("2026-03-10", "papers", diff)

        # Temp tables should still be cleaned up
        mock_drop.assert_called_once()

    @patch.object(diff_updater, "_drop_temp_tables")
    def test_empty_diff(self, mock_drop):
        diff = {"update_files": [], "delete_files": []}

        result = diff_updater.apply_diff("2026-03-10", "papers", diff)

        assert result["deleted"] == 0
        assert result["upserted"] == 0
        mock_drop.assert_called_once()
