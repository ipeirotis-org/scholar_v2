"""Tests for downloader.py."""

from unittest.mock import patch, MagicMock, call

import pytest

from dataset_ingestion import downloader


class TestGcsBlobName:
    def test_extracts_filename_from_url(self):
        url = "https://ai2-s2ag.s3.amazonaws.com/staging/2026-03-10/papers/20260313_070637_00001.gz"
        result = downloader._gcs_blob_name("2026-03-10", "papers", url)
        assert result == "s2_datasets/2026-03-10/papers/20260313_070637_00001.gz"

    def test_handles_query_params(self):
        url = "https://s3.example.com/path/file.gz?AWSAccessKeyId=xxx&Expires=123"
        result = downloader._gcs_blob_name("2026-03-10", "authors", url)
        assert result == "s2_datasets/2026-03-10/authors/file.gz"


class TestDownloadFile:
    @patch.object(downloader, "_blob_exists", return_value=True)
    @patch.object(downloader, "_get_storage_client")
    def test_skips_existing_file(self, mock_client, mock_exists):
        blob_name, was_skipped = downloader.download_file("2026-03-10", "papers", "https://example.com/file.gz")
        assert "file.gz" in blob_name
        assert was_skipped is True
        mock_client.return_value.bucket.return_value.blob.return_value.open.assert_not_called()

    @patch.object(downloader, "_blob_exists", return_value=False)
    @patch.object(downloader, "_get_storage_client")
    @patch("requests.get")
    def test_downloads_new_file(self, mock_requests_get, mock_storage, mock_exists):
        # Mock the streaming response
        mock_response = MagicMock()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.headers = {"Content-Type": "application/gzip"}
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
        mock_requests_get.return_value = mock_response

        # Mock GCS blob writer
        mock_writer = MagicMock()
        mock_writer.__enter__ = MagicMock(return_value=mock_writer)
        mock_writer.__exit__ = MagicMock(return_value=False)
        mock_storage.return_value.bucket.return_value.blob.return_value.open.return_value = mock_writer

        blob_name, was_skipped = downloader.download_file("2026-03-10", "papers", "https://example.com/file.gz")
        assert "file.gz" in blob_name
        assert was_skipped is False
        assert mock_writer.write.call_count == 2


class TestDownloadDataset:
    @patch.object(downloader, "download_file")
    def test_downloads_all_files(self, mock_download):
        mock_download.return_value = ("s2_datasets/2026-03-10/papers/file.gz", False)

        result = downloader.download_dataset(
            "2026-03-10",
            "papers",
            ["https://example.com/f1.gz", "https://example.com/f2.gz"],
        )

        assert result["total"] == 2
        assert result["downloaded"] == 2
        assert result["skipped"] == 0
        assert mock_download.call_count == 2

    @patch.object(downloader, "download_file")
    def test_counts_skipped_files(self, mock_download):
        mock_download.return_value = ("s2_datasets/2026-03-10/papers/file.gz", True)

        result = downloader.download_dataset(
            "2026-03-10",
            "papers",
            ["https://example.com/f1.gz", "https://example.com/f2.gz"],
        )

        assert result["downloaded"] == 2
        assert result["skipped"] == 2

    @patch.object(downloader, "download_file")
    def test_handles_failures(self, mock_download):
        mock_download.side_effect = [
            ("s2_datasets/2026-03-10/papers/f1.gz", False),
            Exception("Network error"),
        ]

        result = downloader.download_dataset(
            "2026-03-10",
            "papers",
            ["https://example.com/f1.gz", "https://example.com/f2.gz"],
        )

        assert result["total"] == 2
        assert result["downloaded"] == 1
        assert result["failed"] == 1
