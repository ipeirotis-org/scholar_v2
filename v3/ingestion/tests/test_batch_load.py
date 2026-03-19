"""Tests for the batch_load module."""

import json
from datetime import datetime, timezone
from unittest import mock

import pytest

from v3.ingestion.batch_load import (
    archive_files,
    cleanup_temp_file,
    iter_gcs_files,
    load_to_bigquery,
    move_to_dead_letter,
    prepare_ndjson_line,
    process_batch,
    process_entity,
    upload_ndjson,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_blob(name, updated=None):
    """Create a mock GCS blob."""
    blob = mock.MagicMock()
    blob.name = name
    blob.updated = updated or datetime(2026, 3, 19, 12, 0, 0, tzinfo=timezone.utc)
    return blob


def _make_file_info(name, content=None):
    """Create a file_info dict with a mock blob that returns content."""
    blob = _make_blob(name)
    if content is not None:
        blob.download_as_text.return_value = content
    return {
        "gcs_uri": f"gs://test-bucket/{name}",
        "name": name,
        "blob_object": blob,
        "updated_time": "2026-03-19T12:00:00+00:00",
    }


# ── iter_gcs_files ───────────────────────────────────────────────────────────


class TestIterGcsFiles:
    @mock.patch("v3.ingestion.batch_load._get_storage_client")
    def test_yields_only_json_files(self, mock_client):
        blobs = [
            _make_blob("authors_json/2026/03/19/a.json"),
            _make_blob("authors_json/2026/03/19/b.txt"),
            _make_blob("authors_json/2026/03/19/c.json"),
        ]
        mock_client.return_value.list_blobs.return_value = iter(blobs)

        results = list(iter_gcs_files("bucket", "authors_json/"))
        assert len(results) == 2
        assert results[0]["name"] == "authors_json/2026/03/19/a.json"
        assert results[1]["name"] == "authors_json/2026/03/19/c.json"

    @mock.patch("v3.ingestion.batch_load._get_storage_client")
    def test_respects_max_files(self, mock_client):
        blobs = [_make_blob(f"prefix/{i}.json") for i in range(10)]
        mock_client.return_value.list_blobs.return_value = iter(blobs)

        results = list(iter_gcs_files("bucket", "prefix/", max_files=3))
        assert len(results) == 3

    @mock.patch("v3.ingestion.batch_load._get_storage_client")
    def test_empty_listing(self, mock_client):
        mock_client.return_value.list_blobs.return_value = iter([])
        results = list(iter_gcs_files("bucket", "prefix/"))
        assert results == []

    @mock.patch("v3.ingestion.batch_load._get_storage_client")
    def test_streams_without_materializing(self, mock_client):
        """Verify we iterate lazily — the list_blobs iterator is consumed one at a time."""
        call_count = 0

        def counting_iter():
            nonlocal call_count
            for i in range(100):
                call_count += 1
                yield _make_blob(f"prefix/{i}.json")

        mock_client.return_value.list_blobs.return_value = counting_iter()

        # Only consume 5
        gen = iter_gcs_files("bucket", "prefix/", max_files=5)
        results = list(gen)
        assert len(results) == 5
        # Should have only iterated 5 times, not 100
        assert call_count == 5


# ── prepare_ndjson_line ──────────────────────────────────────────────────────


class TestPrepareNdjsonLine:
    def test_valid_json(self):
        file_info = _make_file_info(
            "authors_json/2026/03/19/abc123.json",
            content='{"scholar_id": "abc123", "name": "Test Author"}',
        )
        line, error = prepare_ndjson_line(file_info, "authors_json/")
        assert error is None
        parsed = json.loads(line)
        assert parsed["document_id"] == "abc123.json"
        assert parsed["timestamp"] == "2026-03-19T12:00:00+00:00"
        # data should be wrapped under {"data": ...}
        data = json.loads(parsed["DATA"])
        assert data["data"]["scholar_id"] == "abc123"

    def test_empty_file(self):
        file_info = _make_file_info("authors_json/empty.json", content="   ")
        line, error = prepare_ndjson_line(file_info, "authors_json/")
        assert line is None
        assert error == "empty_file"

    def test_invalid_json(self):
        file_info = _make_file_info("authors_json/bad.json", content="not json{")
        line, error = prepare_ndjson_line(file_info, "authors_json/")
        assert line is None
        assert "invalid_json" in error

    def test_download_error(self):
        file_info = _make_file_info("authors_json/fail.json")
        file_info["blob_object"].download_as_text.side_effect = Exception("network")
        line, error = prepare_ndjson_line(file_info, "authors_json/")
        assert line is None
        assert "download_error" in error


# ── move_to_dead_letter ──────────────────────────────────────────────────────


class TestMoveToDeadLetter:
    @mock.patch("v3.ingestion.batch_load._get_storage_client")
    def test_moves_file(self, mock_client):
        bucket = mock.MagicMock()
        mock_client.return_value.bucket.return_value = bucket
        blob = _make_blob("authors_json/2026/03/19/bad.json")
        file_info = {
            "gcs_uri": "gs://bucket/authors_json/2026/03/19/bad.json",
            "name": "authors_json/2026/03/19/bad.json",
            "blob_object": blob,
        }

        move_to_dead_letter(file_info, "authors_json/", "invalid_json")

        bucket.copy_blob.assert_called_once()
        copy_args = bucket.copy_blob.call_args
        assert copy_args[0][2] == "dead_letter/2026/03/19/bad.json"
        blob.delete.assert_called_once()

    @mock.patch("v3.ingestion.batch_load._get_storage_client")
    def test_handles_copy_failure(self, mock_client):
        bucket = mock.MagicMock()
        mock_client.return_value.bucket.return_value = bucket
        bucket.copy_blob.side_effect = Exception("copy failed")
        blob = _make_blob("authors_json/bad.json")
        file_info = {
            "gcs_uri": "gs://bucket/authors_json/bad.json",
            "name": "authors_json/bad.json",
            "blob_object": blob,
        }

        # Should not raise
        move_to_dead_letter(file_info, "authors_json/", "reason")
        blob.delete.assert_not_called()


# ── archive_files ────────────────────────────────────────────────────────────


class TestArchiveFiles:
    @mock.patch("v3.ingestion.batch_load._get_storage_client")
    def test_archives_all(self, mock_client):
        bucket = mock.MagicMock()
        mock_client.return_value.bucket.return_value = bucket

        files = [
            _make_file_info("authors_json/2026/03/19/a.json"),
            _make_file_info("authors_json/2026/03/19/b.json"),
        ]

        archive_files(files, "authors_json/", "authors_archive/")

        assert bucket.copy_blob.call_count == 2
        # Check archive paths
        copy_calls = bucket.copy_blob.call_args_list
        assert copy_calls[0][0][2] == "authors_archive/2026/03/19/a.json"
        assert copy_calls[1][0][2] == "authors_archive/2026/03/19/b.json"

    @mock.patch("v3.ingestion.batch_load._get_storage_client")
    def test_handles_partial_failure(self, mock_client):
        bucket = mock.MagicMock()
        mock_client.return_value.bucket.return_value = bucket
        # First copy succeeds, second fails
        bucket.copy_blob.side_effect = [None, Exception("fail")]

        files = [
            _make_file_info("authors_json/a.json"),
            _make_file_info("authors_json/b.json"),
        ]

        # Should not raise
        archive_files(files, "authors_json/", "authors_archive/")


# ── upload_ndjson ────────────────────────────────────────────────────────────


class TestUploadNdjson:
    @mock.patch("v3.ingestion.batch_load._get_storage_client")
    def test_uploads_content(self, mock_client):
        blob = mock.MagicMock()
        mock_client.return_value.bucket.return_value.blob.return_value = blob

        lines = ['{"a": 1}', '{"b": 2}']
        uri = upload_ndjson(lines, "test.ndjson")

        blob.upload_from_string.assert_called_once()
        content = blob.upload_from_string.call_args[0][0]
        assert content == '{"a": 1}\n{"b": 2}'
        assert uri.startswith("gs://")
        assert "test.ndjson" in uri


# ── load_to_bigquery ────────────────────────────────────────────────────────


class TestLoadToBigquery:
    @mock.patch("v3.ingestion.batch_load._get_bigquery_client")
    def test_successful_load(self, mock_client):
        job = mock.MagicMock()
        job.job_id = "job-123"
        job.error_result = None
        job.errors = None
        job.output_rows = 10
        mock_client.return_value.load_table_from_uri.return_value = job

        result = load_to_bigquery("gs://bucket/file.ndjson", "project.dataset.table")
        assert result is True

    @mock.patch("v3.ingestion.batch_load._get_bigquery_client")
    def test_failed_load(self, mock_client):
        job = mock.MagicMock()
        job.job_id = "job-456"
        job.error_result = {"reason": "invalid"}
        job.errors = [{"message": "bad data"}]
        mock_client.return_value.load_table_from_uri.return_value = job

        result = load_to_bigquery("gs://bucket/file.ndjson", "project.dataset.table")
        assert result is False

    @mock.patch("v3.ingestion.batch_load._get_bigquery_client")
    def test_exception_during_load(self, mock_client):
        mock_client.return_value.load_table_from_uri.side_effect = Exception("timeout")

        result = load_to_bigquery("gs://bucket/file.ndjson", "project.dataset.table")
        assert result is False


# ── cleanup_temp_file ────────────────────────────────────────────────────────


class TestCleanupTempFile:
    @mock.patch("v3.ingestion.batch_load._get_storage_client")
    def test_deletes_file(self, mock_client):
        blob = mock.MagicMock()
        mock_client.return_value.bucket.return_value.blob.return_value = blob

        cleanup_temp_file("gs://my-bucket/bq_load_temp/file.ndjson")
        blob.delete.assert_called_once()

    @mock.patch("v3.ingestion.batch_load._get_storage_client")
    def test_handles_delete_failure(self, mock_client):
        blob = mock.MagicMock()
        blob.delete.side_effect = Exception("not found")
        mock_client.return_value.bucket.return_value.blob.return_value = blob

        # Should not raise
        cleanup_temp_file("gs://my-bucket/bq_load_temp/file.ndjson")

    def test_none_uri(self):
        # Should not raise
        cleanup_temp_file(None)


# ── process_batch ────────────────────────────────────────────────────────────


class TestProcessBatch:
    @mock.patch("v3.ingestion.batch_load.cleanup_temp_file")
    @mock.patch("v3.ingestion.batch_load.archive_files")
    @mock.patch("v3.ingestion.batch_load.load_to_bigquery", return_value=True)
    @mock.patch("v3.ingestion.batch_load.upload_ndjson", return_value="gs://b/temp.ndjson")
    @mock.patch("v3.ingestion.batch_load.move_to_dead_letter")
    def test_full_batch_success(self, mock_dl, mock_upload, mock_load, mock_archive, mock_cleanup):
        files = [
            _make_file_info("authors_json/a.json", '{"id": "a"}'),
            _make_file_info("authors_json/b.json", '{"id": "b"}'),
        ]

        result = process_batch(
            files, "authors_json/", "authors_archive/",
            "project.dataset.author", "Author", 1,
        )

        assert result == 2
        mock_upload.assert_called_once()
        mock_load.assert_called_once()
        mock_archive.assert_called_once()
        mock_dl.assert_not_called()
        mock_cleanup.assert_called_once()

    @mock.patch("v3.ingestion.batch_load.cleanup_temp_file")
    @mock.patch("v3.ingestion.batch_load.archive_files")
    @mock.patch("v3.ingestion.batch_load.load_to_bigquery", return_value=True)
    @mock.patch("v3.ingestion.batch_load.upload_ndjson", return_value="gs://b/temp.ndjson")
    @mock.patch("v3.ingestion.batch_load.move_to_dead_letter")
    def test_mixed_good_and_bad_files(self, mock_dl, mock_upload, mock_load, mock_archive, mock_cleanup):
        files = [
            _make_file_info("authors_json/good.json", '{"id": "good"}'),
            _make_file_info("authors_json/bad.json", "not json{"),
        ]

        result = process_batch(
            files, "authors_json/", "authors_archive/",
            "project.dataset.author", "Author", 1,
        )

        assert result == 1
        mock_dl.assert_called_once()  # bad file moved to dead letter
        mock_upload.assert_called_once()
        # archive_files received only the 1 good file
        archived_files = mock_archive.call_args[0][0]
        assert len(archived_files) == 1

    @mock.patch("v3.ingestion.batch_load.cleanup_temp_file")
    @mock.patch("v3.ingestion.batch_load.upload_ndjson")
    @mock.patch("v3.ingestion.batch_load.move_to_dead_letter")
    def test_all_bad_files(self, mock_dl, mock_upload, mock_cleanup):
        files = [
            _make_file_info("authors_json/bad1.json", ""),
            _make_file_info("authors_json/bad2.json", "{broken"),
        ]

        result = process_batch(
            files, "authors_json/", "authors_archive/",
            "project.dataset.author", "Author", 1,
        )

        assert result == 0
        assert mock_dl.call_count == 2
        mock_upload.assert_not_called()

    @mock.patch("v3.ingestion.batch_load.cleanup_temp_file")
    @mock.patch("v3.ingestion.batch_load.archive_files")
    @mock.patch("v3.ingestion.batch_load.load_to_bigquery", return_value=False)
    @mock.patch("v3.ingestion.batch_load.upload_ndjson", return_value="gs://b/temp.ndjson")
    def test_bq_load_failure_skips_archive(self, mock_upload, mock_load, mock_archive, mock_cleanup):
        files = [_make_file_info("authors_json/a.json", '{"id": "a"}')]

        result = process_batch(
            files, "authors_json/", "authors_archive/",
            "project.dataset.author", "Author", 1,
        )

        assert result == 0
        mock_archive.assert_not_called()
        mock_cleanup.assert_called_once()  # temp file still cleaned up


# ── process_entity ───────────────────────────────────────────────────────────


class TestProcessEntity:
    @mock.patch("v3.ingestion.batch_load.process_batch")
    @mock.patch("v3.ingestion.batch_load.iter_gcs_files")
    def test_batches_files_correctly(self, mock_iter, mock_batch):
        """Verify files are split into batches of the correct size."""
        mock_iter.return_value = iter([
            _make_file_info(f"prefix/{i}.json") for i in range(7)
        ])
        mock_batch.return_value = 3  # pretend 3 archived per batch

        process_entity("prefix/", "archive/", "p.d.t", "Author", batch_size=3, max_files=10)

        # 7 files / batch_size 3 = 3 batches (3, 3, 1)
        assert mock_batch.call_count == 3
        batch_sizes = [len(call.args[0]) for call in mock_batch.call_args_list]
        assert batch_sizes == [3, 3, 1]

    @mock.patch("v3.ingestion.batch_load.process_batch")
    @mock.patch("v3.ingestion.batch_load.iter_gcs_files")
    def test_no_files(self, mock_iter, mock_batch):
        mock_iter.return_value = iter([])

        process_entity("prefix/", "archive/", "p.d.t", "Author", batch_size=50)

        mock_batch.assert_not_called()

    @mock.patch("v3.ingestion.batch_load.process_batch")
    @mock.patch("v3.ingestion.batch_load.iter_gcs_files")
    def test_exact_batch_size(self, mock_iter, mock_batch):
        """Verify that exactly batch_size files produces one batch."""
        mock_iter.return_value = iter([
            _make_file_info(f"prefix/{i}.json") for i in range(5)
        ])
        mock_batch.return_value = 5

        process_entity("prefix/", "archive/", "p.d.t", "Pub", batch_size=5, max_files=10)

        assert mock_batch.call_count == 1
        assert len(mock_batch.call_args_list[0].args[0]) == 5

    @mock.patch("v3.ingestion.batch_load.process_batch")
    @mock.patch("v3.ingestion.batch_load.iter_gcs_files")
    def test_memory_bounded(self, mock_iter, mock_batch):
        """After each batch is processed, the batch list is reset (no accumulation)."""
        batch_sizes_seen = []

        def track_batch(files_batch, *args, **kwargs):
            batch_sizes_seen.append(len(files_batch))
            return len(files_batch)

        mock_batch.side_effect = track_batch
        mock_iter.return_value = iter([
            _make_file_info(f"prefix/{i}.json") for i in range(150)
        ])

        process_entity("prefix/", "archive/", "p.d.t", "Author", batch_size=50, max_files=150)

        assert batch_sizes_seen == [50, 50, 50]


# ── Cloud Function entry point ───────────────────────────────────────────────


class TestBatchLoadEntryPoint:
    @mock.patch("v3.ingestion.batch_load.process_entity")
    def test_default_params(self, mock_process):
        from v3.ingestion.batch_load import batch_load_gcs_to_bq

        request = mock.MagicMock()
        request.args = {}

        body, status = batch_load_gcs_to_bq(request)

        assert status == 200
        assert mock_process.call_count == 2

    @mock.patch("v3.ingestion.batch_load.process_entity")
    def test_custom_batch_size(self, mock_process):
        from v3.ingestion.batch_load import batch_load_gcs_to_bq

        request = mock.MagicMock()
        request.args = {"batch_size": "25", "max_files": "100"}

        body, status = batch_load_gcs_to_bq(request)

        assert status == 200
        # Check batch_size was passed correctly
        for call in mock_process.call_args_list:
            assert call.args[4] == 25  # batch_size arg
            assert call.args[5] == 100  # max_files arg

    @mock.patch("v3.ingestion.batch_load.process_entity")
    def test_batch_size_clamped(self, mock_process):
        from v3.ingestion.batch_load import batch_load_gcs_to_bq

        request = mock.MagicMock()
        request.args = {"batch_size": "9999"}

        batch_load_gcs_to_bq(request)

        # batch_size should be clamped to 500
        for call in mock_process.call_args_list:
            assert call.args[4] == 500

    @mock.patch("v3.ingestion.batch_load.process_entity", side_effect=Exception("boom"))
    def test_critical_error(self, mock_process):
        from v3.ingestion.batch_load import batch_load_gcs_to_bq

        request = mock.MagicMock()
        request.args = {}

        body, status = batch_load_gcs_to_bq(request)

        assert status == 500
        assert "Critical Error" in body
