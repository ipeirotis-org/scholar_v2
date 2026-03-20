"""Tests for refresh task enqueuer."""

from unittest import mock

import pytest


class TestEnqueueAuthor:
    @mock.patch("v3.refresh.task_enqueuer._get_client")
    def test_enqueue_success(self, mock_get_client):
        client = mock.MagicMock()
        mock_get_client.return_value = client

        from v3.refresh.task_enqueuer import enqueue_author
        result = enqueue_author("abc123")

        assert result is True
        client.create_task.assert_called_once()
        call_kwargs = client.create_task.call_args
        task = call_kwargs[1]["task"] if "task" in call_kwargs[1] else call_kwargs[0][0] if call_kwargs[0] else call_kwargs[1].get("task")
        # Verify task was passed
        assert client.create_task.called

    @mock.patch("v3.refresh.task_enqueuer._get_client")
    def test_enqueue_duplicate(self, mock_get_client):
        from google.api_core.exceptions import AlreadyExists
        client = mock.MagicMock()
        client.create_task.side_effect = AlreadyExists("exists")
        mock_get_client.return_value = client

        from v3.refresh.task_enqueuer import enqueue_author
        result = enqueue_author("abc123")

        assert result is False

    @mock.patch("v3.refresh.task_enqueuer._get_client")
    def test_enqueue_error_propagates(self, mock_get_client):
        client = mock.MagicMock()
        client.create_task.side_effect = RuntimeError("network error")
        mock_get_client.return_value = client

        from v3.refresh.task_enqueuer import enqueue_author
        with pytest.raises(RuntimeError, match="network error"):
            enqueue_author("abc123")


class TestEnqueueAuthors:
    @mock.patch("v3.refresh.task_enqueuer.enqueue_author")
    def test_all_enqueued(self, mock_enqueue):
        mock_enqueue.return_value = True

        from v3.refresh.task_enqueuer import enqueue_authors
        result = enqueue_authors(["a", "b", "c"])

        assert result["enqueued"] == 3
        assert result["duplicates"] == 0
        assert result["errors"] == []

    @mock.patch("v3.refresh.task_enqueuer.enqueue_author")
    def test_mixed_results(self, mock_enqueue):
        mock_enqueue.side_effect = [True, False, True]

        from v3.refresh.task_enqueuer import enqueue_authors
        result = enqueue_authors(["a", "b", "c"])

        assert result["enqueued"] == 2
        assert result["duplicates"] == 1
        assert result["errors"] == []

    @mock.patch("v3.refresh.task_enqueuer.enqueue_author")
    def test_error_handling(self, mock_enqueue):
        mock_enqueue.side_effect = [True, RuntimeError("fail"), True]

        from v3.refresh.task_enqueuer import enqueue_authors
        result = enqueue_authors(["a", "b", "c"])

        assert result["enqueued"] == 2
        assert result["duplicates"] == 0
        assert len(result["errors"]) == 1
        assert result["errors"][0]["scholar_id"] == "b"

    @mock.patch("v3.refresh.task_enqueuer.enqueue_author")
    def test_empty_list(self, mock_enqueue):
        from v3.refresh.task_enqueuer import enqueue_authors
        result = enqueue_authors([])

        assert result["enqueued"] == 0
        assert result["duplicates"] == 0
        assert result["errors"] == []
        mock_enqueue.assert_not_called()


class TestSanitizeTaskId:
    def test_colons_replaced(self):
        from v3.refresh.task_enqueuer import _sanitize_task_id
        assert _sanitize_task_id("abc:def") == "abc__def"

    def test_slashes_replaced(self):
        from v3.refresh.task_enqueuer import _sanitize_task_id
        assert _sanitize_task_id("abc/def") == "abc___def"

    def test_combined(self):
        from v3.refresh.task_enqueuer import _sanitize_task_id
        assert _sanitize_task_id("a:b/c") == "a__b___c"

    def test_no_special_chars(self):
        from v3.refresh.task_enqueuer import _sanitize_task_id
        assert _sanitize_task_id("abc123") == "abc123"
