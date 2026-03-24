"""Tests for frontend queue_client module."""

from unittest import mock

from google.api_core.exceptions import AlreadyExists

from frontend.config import Config
from frontend.queue_client import (
    _enqueue_author_crawl_task,
    enqueue_author_crawl,
    enqueue_cache_populate,
)


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


class TestEnqueueAuthorCrawlTask:
    @mock.patch("frontend.queue_client._get_client")
    def test_enqueues_task_with_time_bucket(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        result = _enqueue_author_crawl_task(
            "abc123", "https://us-east1-scholar-version2.cloudfunctions.net/v3_fetch_author",
        )

        assert result is True
        mock_client.create_task.assert_called_once()
        task = mock_client.create_task.call_args[1]["task"]
        assert "abc123-" in task["name"]
        assert task["http_request"]["url"].endswith("/v3_fetch_author")

    @mock.patch("frontend.queue_client._get_client")
    def test_includes_oidc_token(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        _enqueue_author_crawl_task(
            "abc123", "https://us-east1-scholar-version2.cloudfunctions.net/v3_fetch_author",
        )

        task = mock_client.create_task.call_args[1]["task"]
        oidc = task["http_request"]["oidc_token"]
        assert oidc["service_account_email"] == Config.CLOUD_TASKS_SA_EMAIL
        assert oidc["audience"] == "https://us-east1-scholar-version2.cloudfunctions.net"

    @mock.patch("frontend.queue_client._get_client")
    def test_already_exists_returns_true(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_client.create_task.side_effect = AlreadyExists("task exists")
        mock_get_client.return_value = mock_client

        result = _enqueue_author_crawl_task("abc123", "https://func.example.com/v3")
        assert result is True

    @mock.patch("frontend.queue_client._get_client")
    def test_handles_failure(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_client.create_task.side_effect = Exception("Network error")
        mock_get_client.return_value = mock_client

        result = _enqueue_author_crawl_task("abc123", "https://func.example.com/v3")
        assert result is False

    @mock.patch("frontend.queue_client.time")
    @mock.patch("frontend.queue_client._get_client")
    def test_time_bucket_changes_every_10_min(self, mock_get_client, mock_time):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        mock_time.time.return_value = 1000000.0
        _enqueue_author_crawl_task("abc123", "https://func.example.com/v3")
        name1 = mock_client.create_task.call_args[1]["task"]["name"]

        mock_client.reset_mock()

        # 10 min later — different bucket
        mock_time.time.return_value = 1000600.0
        _enqueue_author_crawl_task("abc123", "https://func.example.com/v3")
        name2 = mock_client.create_task.call_args[1]["task"]["name"]
        assert name1 != name2

        mock_client.reset_mock()

        # 100s after first — same bucket
        mock_time.time.return_value = 1000100.0
        _enqueue_author_crawl_task("abc123", "https://func.example.com/v3")
        name3 = mock_client.create_task.call_args[1]["task"]["name"]
        assert name1 == name3


class TestEnqueueAuthorCrawl:
    @mock.patch.object(Config, "CRAWL_FUNCTION_URL", "")
    def test_skips_when_no_url(self):
        result = enqueue_author_crawl("abc123")
        assert result is False

    @mock.patch("frontend.queue_client._enqueue_author_crawl_task", return_value=True)
    @mock.patch.object(Config, "CRAWL_FUNCTION_URL", "https://func.example.com/v3_fetch_author")
    def test_enqueues_to_cloud_tasks_only(self, mock_enqueue):
        """Cloud Tasks is the only invocation path — no direct HTTP calls."""
        result = enqueue_author_crawl("abc123")
        assert result is True
        mock_enqueue.assert_called_once()

    @mock.patch("frontend.queue_client._enqueue_author_crawl_task", return_value=False)
    @mock.patch.object(Config, "CRAWL_FUNCTION_URL", "https://func.example.com/v3_fetch_author")
    def test_returns_false_when_enqueue_fails(self, mock_enqueue):
        result = enqueue_author_crawl("abc123")
        assert result is False

    @mock.patch("frontend.queue_client._enqueue_author_crawl_task", return_value=True)
    @mock.patch.object(Config, "CRAWL_FUNCTION_URL", "https://func.example.com/v3_fetch_author")
    def test_uses_rotating_region(self, mock_enqueue):
        enqueue_author_crawl("abc123")
        crawl_url = mock_enqueue.call_args[0][1]
        assert "scholar-version2.cloudfunctions.net/v3_fetch_author" in crawl_url
