"""Tests for refresh task enqueuer."""

from unittest import mock

import pytest


class TestEnqueueAuthor:
    @mock.patch("refresh.task_enqueuer._get_client")
    def test_enqueue_success(self, mock_get_client):
        client = mock.MagicMock()
        mock_get_client.return_value = client

        from refresh.task_enqueuer import enqueue_author
        result = enqueue_author("abc123")

        assert result is True
        client.create_task.assert_called_once()
        call_kwargs = client.create_task.call_args
        task = call_kwargs[1]["task"] if "task" in call_kwargs[1] else call_kwargs[0][0] if call_kwargs[0] else call_kwargs[1].get("task")
        # Verify task was passed
        assert client.create_task.called

    @mock.patch("refresh.task_enqueuer._get_client")
    def test_enqueue_duplicate(self, mock_get_client):
        from google.api_core.exceptions import AlreadyExists
        client = mock.MagicMock()
        client.create_task.side_effect = AlreadyExists("exists")
        mock_get_client.return_value = client

        from refresh.task_enqueuer import enqueue_author
        result = enqueue_author("abc123")

        assert result is False

    @mock.patch("refresh.task_enqueuer._get_client")
    def test_enqueue_error_propagates(self, mock_get_client):
        client = mock.MagicMock()
        client.create_task.side_effect = RuntimeError("network error")
        mock_get_client.return_value = client

        from refresh.task_enqueuer import enqueue_author
        with pytest.raises(RuntimeError, match="network error"):
            enqueue_author("abc123")

    @mock.patch("refresh.task_enqueuer._get_client")
    def test_includes_oidc_token(self, mock_get_client):
        client = mock.MagicMock()
        mock_get_client.return_value = client

        from refresh.task_enqueuer import enqueue_author
        enqueue_author("abc123")

        task = client.create_task.call_args[1]["task"]
        oidc = task["http_request"]["oidc_token"]
        assert "service_account_email" in oidc
        assert "audience" in oidc
        assert "cloudfunctions.net" in oidc["audience"]


class TestEnqueueAuthors:
    @mock.patch("refresh.task_enqueuer.enqueue_author")
    def test_all_enqueued(self, mock_enqueue):
        mock_enqueue.return_value = True

        from refresh.task_enqueuer import enqueue_authors
        result = enqueue_authors(["a", "b", "c"])

        assert result["enqueued"] == 3
        assert result["duplicates"] == 0
        assert result["errors"] == []

    @mock.patch("refresh.task_enqueuer.enqueue_author")
    def test_mixed_results(self, mock_enqueue):
        mock_enqueue.side_effect = [True, False, True]

        from refresh.task_enqueuer import enqueue_authors
        result = enqueue_authors(["a", "b", "c"])

        assert result["enqueued"] == 2
        assert result["duplicates"] == 1
        assert result["errors"] == []

    @mock.patch("refresh.task_enqueuer.enqueue_author")
    def test_error_handling(self, mock_enqueue):
        mock_enqueue.side_effect = [True, RuntimeError("fail"), True]

        from refresh.task_enqueuer import enqueue_authors
        result = enqueue_authors(["a", "b", "c"])

        assert result["enqueued"] == 2
        assert result["duplicates"] == 0
        assert len(result["errors"]) == 1
        assert result["errors"][0]["scholar_id"] == "b"

    @mock.patch("refresh.task_enqueuer.enqueue_author")
    def test_empty_list(self, mock_enqueue):
        from refresh.task_enqueuer import enqueue_authors
        result = enqueue_authors([])

        assert result["enqueued"] == 0
        assert result["duplicates"] == 0
        assert result["errors"] == []
        mock_enqueue.assert_not_called()


class TestEnqueueCacheWarm:
    @mock.patch("refresh.task_enqueuer.Config")
    def test_skips_when_no_url(self, mock_config):
        mock_config.CACHE_LAYER_URL = ""

        from refresh.task_enqueuer import enqueue_cache_warm
        result = enqueue_cache_warm("abc123")
        assert result is False

    @mock.patch("refresh.task_enqueuer._get_client")
    @mock.patch("refresh.task_enqueuer.Config")
    def test_enqueues_task(self, mock_config, mock_get_client):
        mock_config.CACHE_LAYER_URL = "https://cache.example.com"
        mock_config.QUEUE_NAME_CACHE_BATCH = "cache-batch"
        mock_config.CLOUD_TASKS_SA_EMAIL = "sa@project.iam.gserviceaccount.com"
        mock_config.queue_path.return_value = "projects/p/locations/l/queues/cache-batch"
        client = mock.MagicMock()
        mock_get_client.return_value = client

        from refresh.task_enqueuer import enqueue_cache_warm
        result = enqueue_cache_warm("abc123")

        assert result is True
        client.create_task.assert_called_once()

    @mock.patch("refresh.task_enqueuer._get_client")
    @mock.patch("refresh.task_enqueuer.Config")
    def test_handles_failure(self, mock_config, mock_get_client):
        mock_config.CACHE_LAYER_URL = "https://cache.example.com"
        mock_config.QUEUE_NAME_CACHE_BATCH = "cache-batch"
        mock_config.CLOUD_TASKS_SA_EMAIL = "sa@project.iam.gserviceaccount.com"
        mock_config.queue_path.return_value = "projects/p/locations/l/queues/cache-batch"
        client = mock.MagicMock()
        client.create_task.side_effect = Exception("fail")
        mock_get_client.return_value = client

        from refresh.task_enqueuer import enqueue_cache_warm
        result = enqueue_cache_warm("abc123")
        assert result is False


class TestEnqueueCacheWarmBatch:
    @mock.patch("refresh.task_enqueuer.Config")
    def test_skips_when_no_url(self, mock_config):
        mock_config.CACHE_LAYER_URL = ""

        from refresh.task_enqueuer import enqueue_cache_warm_batch
        result = enqueue_cache_warm_batch(["a", "b"])
        assert result == 0

    @mock.patch("refresh.task_enqueuer.enqueue_cache_warm")
    @mock.patch("refresh.task_enqueuer.Config")
    def test_enqueues_all(self, mock_config, mock_warm):
        mock_config.CACHE_LAYER_URL = "https://cache.example.com"
        mock_warm.return_value = True

        from refresh.task_enqueuer import enqueue_cache_warm_batch
        result = enqueue_cache_warm_batch(["a", "b", "c"])

        assert result == 3
        assert mock_warm.call_count == 3

    @mock.patch("refresh.task_enqueuer.enqueue_cache_warm")
    @mock.patch("refresh.task_enqueuer.Config")
    def test_partial_failures(self, mock_config, mock_warm):
        mock_config.CACHE_LAYER_URL = "https://cache.example.com"
        mock_warm.side_effect = [True, False, True]

        from refresh.task_enqueuer import enqueue_cache_warm_batch
        result = enqueue_cache_warm_batch(["a", "b", "c"])
        assert result == 2


class TestSanitizeTaskId:
    def test_colons_replaced(self):
        from refresh.task_enqueuer import _sanitize_task_id
        assert _sanitize_task_id("abc:def") == "abc__def"

    def test_slashes_replaced(self):
        from refresh.task_enqueuer import _sanitize_task_id
        assert _sanitize_task_id("abc/def") == "abc___def"

    def test_combined(self):
        from refresh.task_enqueuer import _sanitize_task_id
        assert _sanitize_task_id("a:b/c") == "a__b___c"

    def test_no_special_chars(self):
        from refresh.task_enqueuer import _sanitize_task_id
        assert _sanitize_task_id("abc123") == "abc123"
