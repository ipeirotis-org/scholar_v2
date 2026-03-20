"""Tests for refresh BigQuery client."""

from unittest import mock


class TestGetStaleAuthors:
    @mock.patch("refresh.bigquery_client._query")
    def test_returns_scholar_ids(self, mock_query):
        mock_query.return_value = [
            {"scholar_id": "abc123", "last_updated": "2025-01-01"},
            {"scholar_id": "def456", "last_updated": "2025-01-02"},
        ]
        from refresh.bigquery_client import get_stale_authors
        result = get_stale_authors(limit=10)
        assert result == ["abc123", "def456"]
        mock_query.assert_called_once()

    @mock.patch("refresh.bigquery_client._query")
    def test_empty_result(self, mock_query):
        mock_query.return_value = []
        from refresh.bigquery_client import get_stale_authors
        result = get_stale_authors()
        assert result == []

    @mock.patch("refresh.bigquery_client._query")
    def test_uses_config_defaults(self, mock_query):
        mock_query.return_value = []
        from refresh.bigquery_client import get_stale_authors
        get_stale_authors()
        # Should have been called with params including threshold_days and limit
        call_args = mock_query.call_args
        sql = call_args[0][0]
        assert "INTERVAL @threshold_days DAY" in sql
        assert "LIMIT @limit" in sql


class TestGetErrorAuthors:
    @mock.patch("refresh.bigquery_client._query")
    def test_returns_scholar_ids(self, mock_query):
        mock_query.return_value = [
            {"scholar_id": "err001", "last_updated": "2025-06-01"},
        ]
        from refresh.bigquery_client import get_error_authors
        result = get_error_authors(limit=5)
        assert result == ["err001"]

    @mock.patch("refresh.bigquery_client._query")
    def test_empty_on_no_errors(self, mock_query):
        mock_query.return_value = []
        from refresh.bigquery_client import get_error_authors
        result = get_error_authors()
        assert result == []

    @mock.patch("refresh.bigquery_client._query")
    def test_respects_cooldown_in_sql(self, mock_query):
        mock_query.return_value = []
        from refresh.bigquery_client import get_error_authors
        get_error_authors()
        sql = mock_query.call_args[0][0]
        assert "INTERVAL @cooldown_hours HOUR" in sql
        assert "error" in sql.lower()


class TestGetCoauthorsToAdd:
    @mock.patch("refresh.bigquery_client._query")
    def test_returns_scholar_ids(self, mock_query):
        mock_query.return_value = [
            {"coauthor_scholar_id": "co1", "coauthor_name": "A", "total": 10},
            {"coauthor_scholar_id": "co2", "coauthor_name": "B", "total": 5},
        ]
        from refresh.bigquery_client import get_coauthors_to_add
        result = get_coauthors_to_add(limit=5)
        assert set(result) == {"co1", "co2"}

    @mock.patch("refresh.bigquery_client._query")
    def test_samples_when_more_than_limit(self, mock_query):
        mock_query.return_value = [
            {"coauthor_scholar_id": f"co{i}", "coauthor_name": f"Name{i}", "total": i}
            for i in range(20)
        ]
        from refresh.bigquery_client import get_coauthors_to_add
        result = get_coauthors_to_add(limit=5)
        assert len(result) == 5
        # All should be valid IDs
        for sid in result:
            assert sid.startswith("co")

    @mock.patch("refresh.bigquery_client._query")
    def test_empty_result(self, mock_query):
        mock_query.return_value = []
        from refresh.bigquery_client import get_coauthors_to_add
        result = get_coauthors_to_add()
        assert result == []

    @mock.patch("refresh.bigquery_client._query")
    def test_skips_null_ids(self, mock_query):
        mock_query.return_value = [
            {"coauthor_scholar_id": None, "coauthor_name": "X", "total": 10},
            {"coauthor_scholar_id": "valid1", "coauthor_name": "Y", "total": 5},
        ]
        from refresh.bigquery_client import get_coauthors_to_add
        result = get_coauthors_to_add(limit=5)
        assert result == ["valid1"]

    @mock.patch("refresh.bigquery_client._query")
    def test_deduplicates_ids(self, mock_query):
        mock_query.return_value = [
            {"coauthor_scholar_id": "same", "coauthor_name": "A", "total": 10},
            {"coauthor_scholar_id": "same", "coauthor_name": "B", "total": 5},
        ]
        from refresh.bigquery_client import get_coauthors_to_add
        result = get_coauthors_to_add(limit=5)
        assert result == ["same"]


class TestAuthorExists:
    @mock.patch("refresh.bigquery_client._query")
    def test_exists(self, mock_query):
        mock_query.return_value = [{"f0_": 1}]
        from refresh.bigquery_client import author_exists
        assert author_exists("abc123") is True

    @mock.patch("refresh.bigquery_client._query")
    def test_not_exists(self, mock_query):
        mock_query.return_value = []
        from refresh.bigquery_client import author_exists
        assert author_exists("nonexistent") is False
