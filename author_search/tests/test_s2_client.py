"""Tests for Semantic Scholar author search client."""

from unittest import mock

import pytest

from author_search import s2_client


@pytest.fixture(autouse=True)
def _reset_api_key():
    """Reset cached API key between tests."""
    s2_client._api_key = None
    with mock.patch.dict("os.environ", {"S2_API_KEY": "test-key"}):
        yield
    s2_client._api_key = None


class TestSearchAuthors:
    @mock.patch("author_search.s2_client.requests.get")
    def test_returns_normalized_authors(self, mock_get):
        mock_get.return_value.json.return_value = {
            "total": 2,
            "data": [
                {
                    "authorId": "12345",
                    "name": "Alice Smith",
                    "affiliations": ["MIT"],
                    "citationCount": 500,
                    "hIndex": 15,
                    "paperCount": 30,
                },
                {
                    "authorId": "67890",
                    "name": "Bob Jones",
                    "affiliations": ["Stanford", "Google"],
                    "citationCount": 200,
                    "hIndex": 8,
                    "paperCount": 12,
                },
            ],
        }
        mock_get.return_value.raise_for_status = mock.MagicMock()

        results = s2_client.search_authors("Smith")

        assert len(results) == 2
        assert results[0]["scholar_id"] == "12345"
        assert results[0]["name"] == "Alice Smith"
        assert results[0]["affiliation"] == "MIT"
        assert results[0]["citedby"] == 500
        assert results[0]["hindex"] == 15
        assert results[0]["email_domain"] == ""

        # Second author uses first affiliation
        assert results[1]["affiliation"] == "Stanford"

    @mock.patch("author_search.s2_client.requests.get")
    def test_sends_correct_request(self, mock_get):
        mock_get.return_value.json.return_value = {"data": []}
        mock_get.return_value.raise_for_status = mock.MagicMock()

        s2_client.search_authors("John Doe", max_results=5)

        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "author/search" in call_args.args[0]
        assert call_args.kwargs["params"]["query"] == "John Doe"
        assert call_args.kwargs["params"]["limit"] == 5
        assert call_args.kwargs["headers"]["x-api-key"] == "test-key"

    @mock.patch("author_search.s2_client.requests.get")
    def test_returns_empty_on_timeout(self, mock_get):
        import requests

        mock_get.side_effect = requests.exceptions.Timeout()

        results = s2_client.search_authors("Smith")
        assert results == []

    @mock.patch("author_search.s2_client.requests.get")
    def test_returns_empty_on_http_error(self, mock_get):
        response = mock.MagicMock()
        response.status_code = 429
        import requests

        mock_get.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(response=response)
        )

        results = s2_client.search_authors("Smith")
        assert results == []

    @mock.patch("author_search.s2_client.requests.get")
    def test_returns_empty_on_exception(self, mock_get):
        mock_get.side_effect = Exception("network error")

        results = s2_client.search_authors("Smith")
        assert results == []

    @mock.patch("author_search.s2_client.requests.get")
    def test_handles_empty_data(self, mock_get):
        mock_get.return_value.json.return_value = {"total": 0, "data": []}
        mock_get.return_value.raise_for_status = mock.MagicMock()

        results = s2_client.search_authors("Nonexistent")
        assert results == []

    @mock.patch("author_search.s2_client.requests.get")
    def test_handles_null_affiliations(self, mock_get):
        mock_get.return_value.json.return_value = {
            "data": [
                {
                    "authorId": "111",
                    "name": "No Affiliation",
                    "affiliations": None,
                    "citationCount": 10,
                    "hIndex": 2,
                },
            ],
        }
        mock_get.return_value.raise_for_status = mock.MagicMock()

        results = s2_client.search_authors("Test")
        assert results[0]["affiliation"] == ""

    @mock.patch("author_search.s2_client.requests.get")
    def test_handles_null_counts(self, mock_get):
        mock_get.return_value.json.return_value = {
            "data": [
                {
                    "authorId": "222",
                    "name": "New Author",
                    "affiliations": [],
                    "citationCount": None,
                    "hIndex": None,
                },
            ],
        }
        mock_get.return_value.raise_for_status = mock.MagicMock()

        results = s2_client.search_authors("Test")
        assert results[0]["citedby"] == 0
        assert results[0]["hindex"] == 0

    @mock.patch("author_search.s2_client.requests.get")
    def test_skips_none_entries_in_data(self, mock_get):
        mock_get.return_value.json.return_value = {
            "data": [
                None,
                {"authorId": "333", "name": "Valid"},
                None,
            ],
        }
        mock_get.return_value.raise_for_status = mock.MagicMock()

        results = s2_client.search_authors("Test")
        assert len(results) == 1
        assert results[0]["scholar_id"] == "333"


class TestGetApiKey:
    def test_prefers_env_var(self):
        s2_client._api_key = None
        with mock.patch.dict("os.environ", {"S2_API_KEY": "env-key"}):
            key = s2_client._get_api_key()
        assert key == "env-key"

    def test_caches_key(self):
        s2_client._api_key = "cached-key"
        key = s2_client._get_api_key()
        assert key == "cached-key"
