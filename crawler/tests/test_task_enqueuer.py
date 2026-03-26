"""Tests for task_enqueuer module."""

import json
from unittest import mock

import pytest
from google.api_core.exceptions import AlreadyExists

from crawler.config import Config
from crawler.task_enqueuer import enqueue_author, enqueue_publication, enqueue_publications


class TestEnqueueAuthor:
    @mock.patch("crawler.task_enqueuer._get_client")
    def test_enqueue_success(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        result = enqueue_author("abc123")
        assert result is True
        mock_client.create_task.assert_called_once()

    @mock.patch("crawler.task_enqueuer._get_client")
    def test_enqueue_duplicate(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.create_task.side_effect = AlreadyExists("exists")

        result = enqueue_author("abc123")
        assert result is False

    @mock.patch("crawler.task_enqueuer._get_client")
    def test_enqueue_payload(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        enqueue_author("abc123")

        call_kwargs = mock_client.create_task.call_args[1]
        task = call_kwargs["task"]
        assert b"abc123" in task["http_request"]["body"]

    @mock.patch("crawler.task_enqueuer._get_client")
    def test_includes_oidc_token(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        enqueue_author("abc123")

        task = mock_client.create_task.call_args[1]["task"]
        oidc = task["http_request"]["oidc_token"]
        assert oidc["service_account_email"] == Config.CLOUD_TASKS_SA_EMAIL
        assert "cloudfunctions.net" in oidc["audience"]


class TestEnqueuePublication:
    @mock.patch("crawler.task_enqueuer._get_client")
    def test_enqueue_success(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        pub = {"author_pub_id": "abc:pub1"}
        result = enqueue_publication(pub)
        assert result is True

    @mock.patch("crawler.task_enqueuer._get_client")
    def test_enqueue_duplicate(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.create_task.side_effect = AlreadyExists("exists")

        result = enqueue_publication({"author_pub_id": "abc:pub1"})
        assert result is False

    @mock.patch("crawler.task_enqueuer._get_client")
    def test_includes_oidc_token(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        enqueue_publication({"author_pub_id": "abc:pub1"})

        task = mock_client.create_task.call_args[1]["task"]
        oidc = task["http_request"]["oidc_token"]
        assert oidc["service_account_email"] == Config.CLOUD_TASKS_SA_EMAIL

    @mock.patch("crawler.task_enqueuer._get_client")
    def test_standard_queue_when_not_priority(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        enqueue_publication({"author_pub_id": "abc:pub1"}, priority=False)

        parent = mock_client.create_task.call_args[1]["parent"]
        assert "process-pubs" in parent
        assert "priority" not in parent

    @mock.patch("crawler.task_enqueuer._get_client")
    def test_priority_queue_when_priority(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        enqueue_publication({"author_pub_id": "abc:pub1"}, priority=True)

        parent = mock_client.create_task.call_args[1]["parent"]
        assert "process-pub-priority" in parent

    @mock.patch("crawler.task_enqueuer._get_client")
    def test_priority_flag_in_body_when_priority(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        enqueue_publication({"author_pub_id": "abc:pub1"}, priority=True)

        task = mock_client.create_task.call_args[1]["task"]
        body = json.loads(task["http_request"]["body"])
        assert body["priority"] is True

    @mock.patch("crawler.task_enqueuer._get_client")
    def test_no_priority_flag_in_body_when_not_priority(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        enqueue_publication({"author_pub_id": "abc:pub1"}, priority=False)

        task = mock_client.create_task.call_args[1]["task"]
        body = json.loads(task["http_request"]["body"])
        assert "priority" not in body


@mock.patch("crawler.task_enqueuer.resolve_failure")
@mock.patch("crawler.task_enqueuer.record_partial_enqueue_failure")
class TestEnqueuePublications:
    @mock.patch("crawler.task_enqueuer.time.sleep")
    @mock.patch("crawler.task_enqueuer._get_client")
    def test_enqueues_all_with_delay(self, mock_get_client, mock_sleep, _mock_partial, _mock_resolve):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        pubs = [
            {"author_pub_id": "abc:pub1"},
            {"author_pub_id": "abc:pub2"},
            {"author_pub_id": "abc:pub3"},
        ]
        count = enqueue_publications(pubs, delay=0.1)
        assert count == 3
        assert mock_client.create_task.call_count == 3
        assert mock_sleep.call_count == 3

    @mock.patch("crawler.task_enqueuer.time.sleep")
    @mock.patch("crawler.task_enqueuer._get_client")
    def test_enqueues_all_resolves_prior_failure(self, mock_get_client, mock_sleep, _mock_partial, mock_resolve):
        """When all pubs enqueue successfully, resolve any prior partial failure."""
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        pubs = [{"author_pub_id": "abc:pub1"}, {"author_pub_id": "abc:pub2"}]
        enqueue_publications(pubs, delay=0)
        mock_resolve.assert_called_once_with("enqueue_publications", "abc")

    @mock.patch("crawler.task_enqueuer.time.sleep")
    @mock.patch("crawler.task_enqueuer._get_client")
    def test_counts_only_new_tasks(self, mock_get_client, mock_sleep, _mock_partial, _mock_resolve):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.create_task.side_effect = [
            mock.MagicMock(),  # success
            AlreadyExists("dup"),  # duplicate
            mock.MagicMock(),  # success
        ]

        pubs = [
            {"author_pub_id": "abc:pub1"},
            {"author_pub_id": "abc:pub2"},
            {"author_pub_id": "abc:pub3"},
        ]
        count = enqueue_publications(pubs, delay=0)
        assert count == 2

    @mock.patch("crawler.task_enqueuer.time.sleep")
    @mock.patch("crawler.task_enqueuer._get_client")
    def test_continues_on_transient_error(self, mock_get_client, mock_sleep, _mock_partial, _mock_resolve):
        """A transient error on one pub should not stop the rest from being enqueued."""
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.create_task.side_effect = [
            mock.MagicMock(),  # pub1 succeeds
            Exception("DeadlineExceeded"),  # pub2 fails
            mock.MagicMock(),  # pub3 succeeds
        ]

        pubs = [
            {"author_pub_id": "abc:pub1"},
            {"author_pub_id": "abc:pub2"},
            {"author_pub_id": "abc:pub3"},
        ]
        count = enqueue_publications(pubs, delay=0)
        assert count == 2
        assert mock_client.create_task.call_count == 3

    @mock.patch("crawler.task_enqueuer.time.sleep")
    @mock.patch("crawler.task_enqueuer._get_client")
    def test_all_failures_raises(self, mock_get_client, mock_sleep, _mock_partial, _mock_resolve):
        """If every enqueue fails, raise so the parent task is retried."""
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.create_task.side_effect = Exception("service unavailable")

        pubs = [
            {"author_pub_id": "abc:pub1"},
            {"author_pub_id": "abc:pub2"},
        ]
        with pytest.raises(RuntimeError, match="All 2 publication enqueue attempts failed"):
            enqueue_publications(pubs, delay=0)

    @mock.patch("crawler.task_enqueuer.time.sleep")
    @mock.patch("crawler.task_enqueuer._get_client")
    def test_partial_failure_does_not_raise(self, mock_get_client, mock_sleep, _mock_partial, _mock_resolve):
        """If some enqueues succeed, return count without raising."""
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.create_task.side_effect = [
            mock.MagicMock(),  # pub1 succeeds
            Exception("service unavailable"),  # pub2 fails
        ]

        pubs = [
            {"author_pub_id": "abc:pub1"},
            {"author_pub_id": "abc:pub2"},
        ]
        count = enqueue_publications(pubs, delay=0)
        assert count == 1

    @mock.patch("crawler.task_enqueuer.time.sleep")
    @mock.patch("crawler.task_enqueuer._get_client")
    def test_null_author_pub_id_does_not_raise(self, mock_get_client, mock_sleep, mock_partial, _mock_resolve):
        """A publication with None author_pub_id should not cause TypeError in scholar_id derivation."""
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        # enqueue_publication will fail on None id, but the scholar_id
        # derivation afterwards must not raise TypeError.
        pubs = [
            {"author_pub_id": "abc:pub1"},
            {"author_pub_id": None},
        ]
        mock_client.create_task.side_effect = [
            mock.MagicMock(),  # pub1 succeeds
            AttributeError("NoneType"),  # pub2 fails inside enqueue_publication
        ]
        count = enqueue_publications(pubs, delay=0)
        assert count == 1
        mock_partial.assert_called_once()

    @mock.patch("crawler.task_enqueuer.time.sleep")
    @mock.patch("crawler.task_enqueuer._get_client")
    def test_priority_flag_passed_through(self, mock_get_client, mock_sleep, _mock_partial, _mock_resolve):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        pubs = [{"author_pub_id": "abc:pub1"}]
        enqueue_publications(pubs, delay=0, priority=True)

        parent = mock_client.create_task.call_args[1]["parent"]
        assert "process-pub-priority" in parent
        task = mock_client.create_task.call_args[1]["task"]
        body = json.loads(task["http_request"]["body"])
        assert body["priority"] is True
