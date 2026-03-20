"""Tests for ingestion cache_enqueuer module."""

import json
from unittest import mock

from ingestion.cache_enqueuer import (
    _extract_scholar_ids_from_ndjson_lines,
    enqueue_cache_invalidations,
)


class TestExtractScholarIds:
    def test_author_files(self):
        lines = [
            json.dumps({"document_id": "abc123.json", "timestamp": "2026-01-01", "DATA": "{}"}),
            json.dumps({"document_id": "def456.json", "timestamp": "2026-01-01", "DATA": "{}"}),
        ]
        ids = _extract_scholar_ids_from_ndjson_lines(lines)
        assert ids == {"abc123", "def456"}

    def test_publication_files(self):
        lines = [
            json.dumps({"document_id": "abc123:pub1.json", "timestamp": "2026-01-01", "DATA": "{}"}),
            json.dumps({"document_id": "abc123:pub2.json", "timestamp": "2026-01-01", "DATA": "{}"}),
            json.dumps({"document_id": "def456:pub3.json", "timestamp": "2026-01-01", "DATA": "{}"}),
        ]
        ids = _extract_scholar_ids_from_ndjson_lines(lines)
        assert ids == {"abc123", "def456"}

    def test_mixed_files(self):
        lines = [
            json.dumps({"document_id": "abc123.json"}),
            json.dumps({"document_id": "abc123:pub1.json"}),
        ]
        ids = _extract_scholar_ids_from_ndjson_lines(lines)
        assert ids == {"abc123"}

    def test_empty_lines(self):
        assert _extract_scholar_ids_from_ndjson_lines([]) == set()

    def test_bad_json(self):
        ids = _extract_scholar_ids_from_ndjson_lines(["not json", "{bad"])
        assert ids == set()


class TestEnqueueCacheInvalidations:
    @mock.patch("ingestion.cache_enqueuer._CACHE_LAYER_URL", "")
    def test_skips_when_no_url(self):
        result = enqueue_cache_invalidations({"abc123"})
        assert result == 0

    @mock.patch("ingestion.cache_enqueuer._get_client")
    @mock.patch("ingestion.cache_enqueuer._CACHE_LAYER_URL", "https://cache.example.com")
    def test_enqueues_tasks(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        result = enqueue_cache_invalidations({"abc123", "def456"})

        assert result == 2
        assert mock_client.create_task.call_count == 2

    @mock.patch("ingestion.cache_enqueuer._get_client")
    @mock.patch("ingestion.cache_enqueuer._CACHE_LAYER_URL", "https://cache.example.com")
    def test_handles_enqueue_failure(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_client.create_task.side_effect = Exception("Network error")
        mock_get_client.return_value = mock_client

        result = enqueue_cache_invalidations({"abc123"})
        assert result == 0
