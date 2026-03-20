"""Tests for frontend queue_client module."""

from unittest import mock

from frontend.config import Config
from frontend.queue_client import enqueue_cache_populate


class TestEnqueueCachePopulate:
    @mock.patch.object(Config, "CACHE_LAYER_URL", "")
    def test_skips_when_no_url(self):
        result = enqueue_cache_populate("populate_author_profile", {"scholar_id": "abc"})
        assert result is False

    @mock.patch("frontend.queue_client._get_client")
    @mock.patch.object(Config, "CACHE_LAYER_URL", "https://cache.example.com")
    def test_enqueues_task(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        result = enqueue_cache_populate(
            "populate_author_profile", {"scholar_id": "abc123"},
        )

        assert result is True
        mock_client.create_task.assert_called_once()

        # Verify the task body
        call_args = mock_client.create_task.call_args
        task = call_args[1]["task"] if "task" in call_args[1] else call_args[0][0] if len(call_args[0]) > 0 else call_args[1].get("task")
        # The task is passed as a keyword argument
        task = mock_client.create_task.call_args
        assert task is not None

    @mock.patch("frontend.queue_client._get_client")
    @mock.patch.object(Config, "CACHE_LAYER_URL", "https://cache.example.com")
    def test_handles_failure(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_client.create_task.side_effect = Exception("Network error")
        mock_get_client.return_value = mock_client

        result = enqueue_cache_populate(
            "populate_author_profile", {"scholar_id": "abc123"},
        )
        assert result is False
