"""Tests for BigQuerySearchClient."""

from unittest import mock

import pytest

from author_search.bigquery_client import BigQuerySearchClient


class TestSearchCrawledAuthors:
    def test_returns_matching_authors(self):
        mock_client = mock.MagicMock()
        mock_client.query.return_value.result.return_value = [
            {"scholar_id": "abc123", "name": "Alice Smith", "affiliation": "MIT",
             "email_domain": "", "citedby": 500, "hindex": 15},
        ]

        bq = BigQuerySearchClient(client=mock_client)
        results = bq.search_crawled_authors("alice")

        assert len(results) == 1
        assert results[0]["scholar_id"] == "abc123"
        # Verify parameterized query was used
        call_args = mock_client.query.call_args
        assert "@pattern" in call_args[0][0]

    def test_returns_empty_on_exception(self):
        mock_client = mock.MagicMock()
        mock_client.query.side_effect = Exception("BQ error")

        bq = BigQuerySearchClient(client=mock_client)
        results = bq.search_crawled_authors("alice")

        assert results == []


class TestSearchS2Universe:
    def test_returns_matching_s2_authors(self):
        mock_client = mock.MagicMock()
        mock_client.query.return_value.result.return_value = [
            {"scholar_id": "12345", "name": "Bob Jones", "affiliation": "Stanford",
             "email_domain": "", "citedby": 1000, "hindex": 20},
        ]

        bq = BigQuerySearchClient(client=mock_client)
        results = bq.search_s2_universe("bob")

        assert len(results) == 1
        assert results[0]["scholar_id"] == "12345"

    def test_query_uses_s2_data_authors_with_activity_filter(self):
        mock_client = mock.MagicMock()
        mock_client.query.return_value.result.return_value = []

        bq = BigQuerySearchClient(client=mock_client)
        bq.search_s2_universe("test")

        sql = mock_client.query.call_args[0][0]
        assert "s2_data" in sql
        assert "authors" in sql
        assert "author_paper_stats" in sql
        assert "total_publications >= 3" in sql

    def test_returns_empty_on_exception(self):
        mock_client = mock.MagicMock()
        mock_client.query.side_effect = Exception("BQ error")

        bq = BigQuerySearchClient(client=mock_client)
        results = bq.search_s2_universe("bob")

        assert results == []


class TestGetAllAuthorNames:
    def test_returns_authors_for_index(self):
        mock_client = mock.MagicMock()
        mock_client.query.return_value.result.return_value = [
            {"scholar_id": "1", "name": "Alice", "affiliation": "MIT", "citedby": 100},
        ]

        bq = BigQuerySearchClient(client=mock_client)
        results = bq.get_all_author_names()

        assert len(results) == 1
        assert results[0]["name"] == "Alice"
