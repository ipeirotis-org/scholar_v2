"""Tests for task_enqueuer module."""

from unittest import mock

from google.api_core.exceptions import AlreadyExists

from v3.crawler.task_enqueuer import enqueue_author, enqueue_publication, enqueue_publications


class TestEnqueueAuthor:
    @mock.patch("v3.crawler.task_enqueuer._get_client")
    def test_enqueue_success(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        result = enqueue_author("abc123")
        assert result is True
        mock_client.create_task.assert_called_once()

    @mock.patch("v3.crawler.task_enqueuer._get_client")
    def test_enqueue_duplicate(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.create_task.side_effect = AlreadyExists("exists")

        result = enqueue_author("abc123")
        assert result is False

    @mock.patch("v3.crawler.task_enqueuer._get_client")
    def test_enqueue_payload(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        enqueue_author("abc123")

        call_kwargs = mock_client.create_task.call_args[1]
        task = call_kwargs["task"]
        assert b"abc123" in task["http_request"]["body"]


class TestEnqueuePublication:
    @mock.patch("v3.crawler.task_enqueuer._get_client")
    def test_enqueue_success(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        pub = {"author_pub_id": "abc:pub1"}
        result = enqueue_publication(pub)
        assert result is True

    @mock.patch("v3.crawler.task_enqueuer._get_client")
    def test_enqueue_duplicate(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.create_task.side_effect = AlreadyExists("exists")

        result = enqueue_publication({"author_pub_id": "abc:pub1"})
        assert result is False


class TestEnqueuePublications:
    @mock.patch("v3.crawler.task_enqueuer.time.sleep")
    @mock.patch("v3.crawler.task_enqueuer._get_client")
    def test_enqueues_all_with_delay(self, mock_get_client, mock_sleep):
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

    @mock.patch("v3.crawler.task_enqueuer.time.sleep")
    @mock.patch("v3.crawler.task_enqueuer._get_client")
    def test_counts_only_new_tasks(self, mock_get_client, mock_sleep):
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
