"""Tests for BigQuerySearchClient."""

from unittest import mock

import pytest

from v3.author_search.bigquery_client import BigQuerySearchClient


class TestSearchCrawledAuthors:
    def test_returns_matching_authors(self):
        mock_client = mock.MagicMock()
        mock_df = mock.MagicMock()
        mock_df.empty = False
        mock_df.to_dict.return_value = [
            {"scholar_id": "abc123", "name": "Alice Smith", "affiliation": "MIT",
             "email_domain": "mit.edu", "citedby": 500, "hindex": 15},
        ]
        mock_client.query.return_value.result.return_value.to_dataframe.return_value = mock_df

        bq = BigQuerySearchClient(client=mock_client)
        results = bq.search_crawled_authors("alice")

        assert len(results) == 1
        assert results[0]["scholar_id"] == "abc123"
        # Verify parameterized query was used
        call_args = mock_client.query.call_args
        assert "@pattern" in call_args[0][0]

    def test_returns_empty_on_no_results(self):
        mock_client = mock.MagicMock()
        mock_df = mock.MagicMock()
        mock_df.empty = True
        mock_client.query.return_value.result.return_value.to_dataframe.return_value = mock_df

        bq = BigQuerySearchClient(client=mock_client)
        results = bq.search_crawled_authors("nobody")

        assert results == []

    def test_returns_empty_on_exception(self):
        mock_client = mock.MagicMock()
        mock_client.query.side_effect = Exception("BQ error")

        bq = BigQuerySearchClient(client=mock_client)
        results = bq.search_crawled_authors("alice")

        assert results == []


class TestSearchCoauthorNetwork:
    def test_returns_coauthors(self):
        mock_client = mock.MagicMock()
        mock_df = mock.MagicMock()
        mock_df.empty = False
        mock_df.to_dict.return_value = [
            {"scholar_id": "co1", "name": "Bob Jones", "affiliation": "Stanford",
             "email_domain": "", "citedby": 0, "hindex": 0},
        ]
        mock_client.query.return_value.result.return_value.to_dataframe.return_value = mock_df

        bq = BigQuerySearchClient(client=mock_client)
        results = bq.search_coauthor_network("bob")

        assert len(results) == 1
        assert results[0]["scholar_id"] == "co1"

    def test_query_uses_coauthors_to_add_view(self):
        mock_client = mock.MagicMock()
        mock_df = mock.MagicMock()
        mock_df.empty = True
        mock_client.query.return_value.result.return_value.to_dataframe.return_value = mock_df

        bq = BigQuerySearchClient(client=mock_client)
        bq.search_coauthor_network("test")

        sql = mock_client.query.call_args[0][0]
        assert "coauthors_to_add" in sql
