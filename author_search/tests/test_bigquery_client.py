"""Tests for BigQuerySearchClient."""

from unittest import mock

from author_search.bigquery_client import BigQuerySearchClient


class TestGetAllAuthorNames:
    def test_returns_authors_for_index(self):
        mock_client = mock.MagicMock()
        mock_client.query.return_value.result.return_value = [
            {"scholar_id": "1", "name": "Alice", "affiliation": "MIT",
             "citedby": 100, "hindex": 10},
        ]

        bq = BigQuerySearchClient(client=mock_client)
        results = bq.get_all_author_names()

        assert len(results) == 1
        assert results[0]["name"] == "Alice"
        assert results[0]["hindex"] == 10

    def test_filters_by_activity(self):
        mock_client = mock.MagicMock()
        mock_client.query.return_value.result.return_value = []

        bq = BigQuerySearchClient(client=mock_client)
        bq.get_all_author_names()

        sql = mock_client.query.call_args[0][0]
        assert "hindex >= 20" in sql
        assert "citedby > 5000" in sql

    def test_returns_empty_on_exception(self):
        mock_client = mock.MagicMock()
        mock_client.query.side_effect = Exception("BQ error")

        bq = BigQuerySearchClient(client=mock_client)
        results = bq.get_all_author_names()

        assert results == []
