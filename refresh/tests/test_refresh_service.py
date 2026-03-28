"""Tests for refresh service orchestration."""

from unittest import mock


class TestRefreshStaleAuthors:
    @mock.patch("refresh.refresh_service.task_enqueuer")
    @mock.patch("refresh.refresh_service.bq")
    def test_finds_and_enqueues(self, mock_bq, mock_enqueuer):
        mock_bq.get_stale_authors.return_value = ["a1", "a2"]
        mock_enqueuer.enqueue_authors.return_value = {
            "enqueued": 2, "duplicates": 0, "errors": [],
        }

        from refresh.refresh_service import refresh_stale_authors
        result = refresh_stale_authors()

        assert result["source"] == "stale"
        assert result["found"] == 2
        assert result["enqueued"] == 2
        mock_enqueuer.enqueue_authors.assert_called_once_with(["a1", "a2"])

    @mock.patch("refresh.refresh_service.task_enqueuer")
    @mock.patch("refresh.refresh_service.bq")
    def test_no_stale_authors(self, mock_bq, mock_enqueuer):
        mock_bq.get_stale_authors.return_value = []

        from refresh.refresh_service import refresh_stale_authors
        result = refresh_stale_authors()

        assert result["found"] == 0
        assert result["enqueued"] == 0
        mock_enqueuer.enqueue_authors.assert_not_called()

    @mock.patch("refresh.refresh_service.task_enqueuer")
    @mock.patch("refresh.refresh_service.bq")
    def test_passes_limit(self, mock_bq, mock_enqueuer):
        mock_bq.get_stale_authors.return_value = []

        from refresh.refresh_service import refresh_stale_authors
        refresh_stale_authors(limit=20)

        mock_bq.get_stale_authors.assert_called_once_with(limit=20)


class TestRefreshErrorAuthors:
    @mock.patch("refresh.refresh_service.task_enqueuer")
    @mock.patch("refresh.refresh_service.bq")
    def test_finds_and_enqueues(self, mock_bq, mock_enqueuer):
        mock_bq.get_error_authors.return_value = ["err1"]
        mock_enqueuer.enqueue_authors.return_value = {
            "enqueued": 1, "duplicates": 0, "errors": [],
        }

        from refresh.refresh_service import refresh_error_authors
        result = refresh_error_authors()

        assert result["source"] == "errors"
        assert result["found"] == 1
        assert result["enqueued"] == 1

    @mock.patch("refresh.refresh_service.task_enqueuer")
    @mock.patch("refresh.refresh_service.bq")
    def test_no_errors(self, mock_bq, mock_enqueuer):
        mock_bq.get_error_authors.return_value = []

        from refresh.refresh_service import refresh_error_authors
        result = refresh_error_authors()

        assert result["found"] == 0
        mock_enqueuer.enqueue_authors.assert_not_called()


class TestExpandCoauthors:
    @mock.patch("refresh.refresh_service.task_enqueuer")
    @mock.patch("refresh.refresh_service.bq")
    def test_finds_and_enqueues(self, mock_bq, mock_enqueuer):
        mock_bq.get_coauthors_to_add.return_value = ["co1", "co2", "co3"]
        mock_enqueuer.enqueue_authors.return_value = {
            "enqueued": 3, "duplicates": 0, "errors": [],
        }

        from refresh.refresh_service import expand_coauthors
        result = expand_coauthors()

        assert result["source"] == "coauthors"
        assert result["found"] == 3
        assert result["enqueued"] == 3

    @mock.patch("refresh.refresh_service.task_enqueuer")
    @mock.patch("refresh.refresh_service.bq")
    def test_no_coauthors(self, mock_bq, mock_enqueuer):
        mock_bq.get_coauthors_to_add.return_value = []

        from refresh.refresh_service import expand_coauthors
        result = expand_coauthors()

        assert result["found"] == 0
        mock_enqueuer.enqueue_authors.assert_not_called()


class TestFetchAuthor:
    @mock.patch("refresh.refresh_service.task_enqueuer")
    @mock.patch("refresh.refresh_service.bq")
    def test_enqueue_new_author(self, mock_bq, mock_enqueuer):
        mock_bq.author_exists.return_value = False
        mock_enqueuer.enqueue_author.return_value = True

        from refresh.refresh_service import fetch_author
        result = fetch_author("new123")

        assert result["scholar_id"] == "new123"
        assert result["exists"] is False
        assert result["enqueued"] is True

    @mock.patch("refresh.refresh_service.task_enqueuer")
    @mock.patch("refresh.refresh_service.bq")
    def test_enqueue_existing_author(self, mock_bq, mock_enqueuer):
        mock_bq.author_exists.return_value = True
        mock_enqueuer.enqueue_author.return_value = True

        from refresh.refresh_service import fetch_author
        result = fetch_author("existing")

        assert result["exists"] is True
        assert result["enqueued"] is True

    @mock.patch("refresh.refresh_service.task_enqueuer")
    @mock.patch("refresh.refresh_service.bq")
    def test_enqueue_duplicate(self, mock_bq, mock_enqueuer):
        mock_bq.author_exists.return_value = True
        mock_enqueuer.enqueue_author.return_value = False

        from refresh.refresh_service import fetch_author
        result = fetch_author("dup123")

        assert result["enqueued"] is False

    @mock.patch("refresh.refresh_service.task_enqueuer")
    @mock.patch("refresh.refresh_service.bq")
    def test_enqueue_error(self, mock_bq, mock_enqueuer):
        mock_bq.author_exists.return_value = False
        mock_enqueuer.enqueue_author.side_effect = RuntimeError("fail")

        from refresh.refresh_service import fetch_author
        result = fetch_author("err123")

        assert result["enqueued"] is False
        assert "error" in result

    @mock.patch("refresh.refresh_service.task_enqueuer")
    @mock.patch("refresh.refresh_service.bq")
    def test_author_exists_error_still_enqueues(self, mock_bq, mock_enqueuer):
        """If author_exists() raises, fetch_author should still enqueue."""
        mock_bq.author_exists.side_effect = RuntimeError("BQ unavailable")
        mock_enqueuer.enqueue_author.return_value = True

        from refresh.refresh_service import fetch_author
        result = fetch_author("bq_err")

        assert result["scholar_id"] == "bq_err"
        assert result["exists"] is None
        assert result["enqueued"] is True
        mock_enqueuer.enqueue_author.assert_called_once_with("bq_err")


class TestFetchAuthors:
    @mock.patch("refresh.refresh_service.task_enqueuer")
    def test_enqueues_all(self, mock_enqueuer):
        mock_enqueuer.enqueue_authors.return_value = {
            "enqueued": 3, "duplicates": 0, "errors": [],
        }

        from refresh.refresh_service import fetch_authors
        result = fetch_authors(["a", "b", "c"])

        assert result["source"] == "user_request"
        assert result["found"] == 3
        assert result["enqueued"] == 3
