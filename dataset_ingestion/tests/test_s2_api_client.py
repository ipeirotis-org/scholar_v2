"""Tests for s2_api_client.py."""

from unittest.mock import patch, MagicMock

import pytest

from dataset_ingestion import s2_api_client


class TestListReleases:
    def test_returns_release_list(self, mock_s2_api_key):
        mock_response = MagicMock()
        mock_response.json.return_value = ["2026-03-03", "2026-03-10"]
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response) as mock_get:
            result = s2_api_client.list_releases()

        assert result == ["2026-03-03", "2026-03-10"]
        mock_get.assert_called_once()
        assert "/release/" in mock_get.call_args[0][0]

    def test_includes_api_key_header(self, mock_s2_api_key):
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response) as mock_get:
            s2_api_client.list_releases()

        headers = mock_get.call_args[1]["headers"]
        assert headers["x-api-key"] == "test-api-key"


class TestGetLatestReleaseId:
    def test_returns_last_release(self, mock_s2_api_key):
        with patch.object(s2_api_client, "list_releases", return_value=["2026-03-03", "2026-03-10"]):
            assert s2_api_client.get_latest_release_id() == "2026-03-10"

    def test_raises_on_empty(self, mock_s2_api_key):
        with patch.object(s2_api_client, "list_releases", return_value=[]):
            with pytest.raises(RuntimeError, match="No S2 releases"):
                s2_api_client.get_latest_release_id()


class TestGetDatasetFiles:
    def test_returns_file_urls(self, mock_s2_api_key):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "name": "papers",
            "files": ["https://s3.example.com/file1.gz", "https://s3.example.com/file2.gz"],
        }
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response):
            result = s2_api_client.get_dataset_files("2026-03-10", "papers")

        assert len(result) == 2
        assert result[0] == "https://s3.example.com/file1.gz"


class TestGetDiffs:
    def test_returns_diff_data(self, mock_s2_api_key):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "dataset": "papers",
            "start_release": "2026-03-03",
            "end_release": "2026-03-10",
            "diffs": [
                {
                    "from_release": "2026-03-03",
                    "to_release": "2026-03-10",
                    "update_files": ["https://s3.example.com/update1.gz"],
                    "delete_files": ["https://s3.example.com/delete1.gz"],
                }
            ],
        }
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response):
            result = s2_api_client.get_diffs("2026-03-03", "2026-03-10", "papers")

        assert len(result["diffs"]) == 1
        assert result["diffs"][0]["update_files"] == ["https://s3.example.com/update1.gz"]
