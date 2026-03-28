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


class TestApplyDiffDml:
    @patch.object(diff_updater, "_get_bq_client")
    def test_wraps_in_transaction(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_client.query.return_value = mock_job

        diff_updater._apply_diff_dml("papers", "del_table", "upd_table")

        sql = mock_client.query.call_args[0][0]
        assert "BEGIN TRANSACTION" in sql
        assert "COMMIT TRANSACTION" in sql
        assert "DELETE FROM" in sql
        assert "MERGE" in sql
        assert "corpusid" in sql

    @patch.object(diff_updater, "_get_bq_client")
    def test_delete_only(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_client.query.return_value = mock_job

        diff_updater._apply_diff_dml("papers", "del_table", None)

        sql = mock_client.query.call_args[0][0]
        assert "DELETE FROM" in sql
        assert "MERGE" not in sql

    @patch.object(diff_updater, "_get_bq_client")
    def test_upsert_only(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_client.query.return_value = mock_job

        diff_updater._apply_diff_dml("authors", None, "upd_table")

        sql = mock_client.query.call_args[0][0]
        assert "DELETE" not in sql
        assert "MERGE" in sql
        assert "name" in sql
        assert "hindex" in sql

    @patch.object(diff_updater, "_get_bq_client")
    def test_uses_correct_primary_key_per_dataset(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_client.query.return_value = mock_job

        diff_updater._apply_diff_dml("citations", "del", "upd")
        sql = mock_client.query.call_args[0][0]
        assert "citationid" in sql


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


def _mock_bq_client_for_counts():
    """Create a mock BQ client that returns row counts for get_table."""
    mock_client = MagicMock()
    mock_table = MagicMock()
    mock_table.num_rows = 42
    mock_client.get_table.return_value = mock_table
    return mock_client


class TestApplyDiff:
    @patch.object(diff_updater, "_get_bq_client")
    @patch.object(diff_updater, "_drop_temp_tables")
    @patch.object(diff_updater, "_apply_diff_dml")
    @patch.object(diff_updater, "_load_temp_table", return_value="tmp_table")
    @patch.object(diff_updater, "download_dataset")
    def test_applies_deletes_and_upserts(
        self, mock_download, mock_load, mock_dml, mock_drop, mock_bq
    ):
        mock_bq.return_value = _mock_bq_client_for_counts()
        mock_download.return_value = {"failed": 0, "downloaded": 5}

        diff = {
            "update_files": ["https://example.com/u1.gz"],
            "delete_files": ["https://example.com/d1.gz"],
        }

        result = diff_updater.apply_diff("2026-03-10", "papers", diff)

        mock_dml.assert_called_once_with("papers", "tmp_table", "tmp_table")
        mock_drop.assert_called_once_with("papers")
        assert result["deleted"] == 42
        assert result["upserted"] == 42

    @patch.object(diff_updater, "_get_bq_client")
    @patch.object(diff_updater, "_drop_temp_tables")
    @patch.object(diff_updater, "_apply_diff_dml")
    @patch.object(diff_updater, "_load_temp_table", return_value="tmp_table")
    @patch.object(diff_updater, "download_dataset")
    def test_handles_updates_only(self, mock_download, mock_load, mock_dml, mock_drop, mock_bq):
        mock_bq.return_value = _mock_bq_client_for_counts()
        mock_download.return_value = {"failed": 0, "downloaded": 5}

        diff = {"update_files": ["https://example.com/u1.gz"], "delete_files": []}

        result = diff_updater.apply_diff("2026-03-10", "papers", diff)

        mock_dml.assert_called_once_with("papers", None, "tmp_table")
        assert result["deleted"] == 0
        assert result["upserted"] == 42

    @patch.object(diff_updater, "_drop_temp_tables")
    @patch.object(diff_updater, "download_dataset")
    def test_raises_on_download_failure(self, mock_download, mock_drop):
        mock_download.return_value = {"failed": 2, "downloaded": 3}

        diff = {"update_files": ["https://example.com/u1.gz"], "delete_files": []}

        with pytest.raises(RuntimeError, match="update file downloads failed"):
            diff_updater.apply_diff("2026-03-10", "papers", diff)

        # Temp tables should still be cleaned up
        mock_drop.assert_called_once()

    @patch.object(diff_updater, "_get_bq_client")
    @patch.object(diff_updater, "_drop_temp_tables")
    def test_empty_diff(self, mock_drop, mock_bq):
        diff = {"update_files": [], "delete_files": []}

        result = diff_updater.apply_diff("2026-03-10", "papers", diff)

        assert result["deleted"] == 0
        assert result["upserted"] == 0
        mock_drop.assert_called_once()
