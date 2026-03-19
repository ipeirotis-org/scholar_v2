"""Tests for gcs_writer module."""

import json
from unittest import mock

from v3.crawler.gcs_writer import upload_json, author_blob_path, publication_blob_path


class TestBlobPaths:
    @mock.patch("v3.crawler.gcs_writer.Config")
    def test_author_blob_path(self, mock_config):
        mock_config.gcs_date_prefix.return_value = "2026/03/19"
        path = author_blob_path("abc123")
        assert path == "authors_json/2026/03/19/abc123.json"

    @mock.patch("v3.crawler.gcs_writer.Config")
    def test_publication_blob_path_sanitizes_colons(self, mock_config):
        mock_config.gcs_date_prefix.return_value = "2026/03/19"
        path = publication_blob_path("abc123:XyZ789")
        assert path == "publications_json/2026/03/19/abc123_XyZ789.json"

    @mock.patch("v3.crawler.gcs_writer.Config")
    def test_publication_blob_path_sanitizes_slashes(self, mock_config):
        mock_config.gcs_date_prefix.return_value = "2026/03/19"
        path = publication_blob_path("abc/def")
        assert path == "publications_json/2026/03/19/abc___def.json"


class TestUploadJson:
    @mock.patch("v3.crawler.gcs_writer._get_client")
    def test_upload_success(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        mock_blob = mock.MagicMock()
        mock_client.bucket.return_value.blob.return_value = mock_blob

        data = {"key": "value"}
        upload_json(data, "test/path.json")

        mock_blob.upload_from_string.assert_called_once()
        call_args = mock_blob.upload_from_string.call_args
        assert json.loads(call_args[0][0]) == data
        assert call_args[1]["content_type"] == "application/json"

    @mock.patch("v3.crawler.gcs_writer.time.sleep")
    @mock.patch("v3.crawler.gcs_writer._get_client")
    def test_upload_retries_on_failure(self, mock_get_client, mock_sleep):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        mock_blob = mock.MagicMock()
        mock_client.bucket.return_value.blob.return_value = mock_blob
        mock_blob.upload_from_string.side_effect = [
            Exception("network error"),
            None,  # success on retry
        ]

        upload_json({"key": "value"}, "test/path.json")
        assert mock_blob.upload_from_string.call_count == 2

    @mock.patch("v3.crawler.gcs_writer.time.sleep")
    @mock.patch("v3.crawler.gcs_writer._get_client")
    def test_upload_raises_after_exhausting_retries(self, mock_get_client, mock_sleep):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        mock_blob = mock.MagicMock()
        mock_client.bucket.return_value.blob.return_value = mock_blob
        mock_blob.upload_from_string.side_effect = Exception("persistent failure")

        import pytest
        with pytest.raises(Exception, match="persistent failure"):
            upload_json({"key": "value"}, "test/path.json")
        assert mock_blob.upload_from_string.call_count == 3  # 1 + 2 retries
