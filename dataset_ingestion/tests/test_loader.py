"""Tests for loader.py."""

from unittest.mock import patch, MagicMock, call

import pytest

from dataset_ingestion import loader
from dataset_ingestion.config import Config


class TestEnsureDatasetExists:
    @patch.object(loader, "_get_bq_client")
    def test_creates_dataset(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        loader.ensure_dataset_exists()

        mock_client.create_dataset.assert_called_once()
        args = mock_client.create_dataset.call_args
        assert args[1]["exists_ok"] is True


class TestLoadDataset:
    @patch.object(loader, "_get_bq_client")
    def test_loads_papers(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_job = MagicMock()
        mock_client.load_table_from_uri.return_value = mock_job

        mock_table = MagicMock()
        mock_table.num_rows = 200_000_000
        mock_client.get_table.return_value = mock_table

        rows = loader.load_dataset("2026-03-10", "papers")

        assert rows == 200_000_000
        mock_client.load_table_from_uri.assert_called_once()
        uri_arg = mock_client.load_table_from_uri.call_args[0][0]
        assert "s2_datasets/2026-03-10/papers/" in uri_arg
        mock_job.result.assert_called_once()

    @patch.object(loader, "_get_bq_client")
    def test_loads_citations(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_client.load_table_from_uri.return_value = mock_job
        mock_table = MagicMock()
        mock_table.num_rows = 2_400_000_000
        mock_client.get_table.return_value = mock_table

        rows = loader.load_dataset("2026-03-10", "citations")
        assert rows == 2_400_000_000

    def test_unknown_dataset_raises(self):
        with pytest.raises(ValueError, match="Unknown dataset"):
            loader.load_dataset("2026-03-10", "unknown_dataset")


class TestBuildDerivedTables:
    @patch.object(loader, "_get_bq_client")
    def test_build_paper_citations_by_year(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_client.query.return_value = mock_job
        mock_table = MagicMock()
        mock_table.num_rows = 5_000_000_000
        mock_client.get_table.return_value = mock_table

        rows = loader.build_paper_citations_by_year()
        assert rows == 5_000_000_000
        sql = mock_client.query.call_args[0][0]
        assert "paper_citations_by_year" in sql
        assert "citedcorpusid" in sql

    @patch.object(loader, "_get_bq_client")
    def test_build_author_paper_bridge(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_client.query.return_value = mock_job
        mock_table = MagicMock()
        mock_table.num_rows = 800_000_000
        mock_client.get_table.return_value = mock_table

        rows = loader.build_author_paper_bridge()
        assert rows == 800_000_000
        sql = mock_client.query.call_args[0][0]
        assert "author_paper_bridge" in sql
        assert "authorId" in sql
        assert "CLUSTER BY authorid" in sql

    @patch.object(loader, "_get_bq_client")
    def test_build_author_paper_stats(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_job = MagicMock()
        mock_client.query.return_value = mock_job
        mock_table = MagicMock()
        mock_table.num_rows = 75_000_000
        mock_client.get_table.return_value = mock_table

        rows = loader.build_author_paper_stats()
        assert rows == 75_000_000
        sql = mock_client.query.call_args[0][0]
        assert "author_paper_stats" in sql
        assert "i10_index" in sql


class TestGetLastLoadedRelease:
    @patch.object(loader, "_get_bq_client")
    def test_returns_release_id(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_row = MagicMock()
        mock_row.release_id = "2026-03-10"
        mock_client.query.return_value.result.return_value = [mock_row]

        assert loader.get_last_loaded_release() == "2026-03-10"

    @patch.object(loader, "_get_bq_client")
    def test_returns_none_when_no_table(self, mock_get_client):
        from google.api_core import exceptions as google_exceptions

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.query.return_value.result.side_effect = google_exceptions.NotFound("Table not found")

        assert loader.get_last_loaded_release() is None

    @patch.object(loader, "_get_bq_client")
    def test_raises_on_transient_error(self, mock_get_client):
        from google.api_core import exceptions as google_exceptions

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.query.return_value.result.side_effect = google_exceptions.ServiceUnavailable("503")

        with pytest.raises(google_exceptions.ServiceUnavailable):
            loader.get_last_loaded_release()
