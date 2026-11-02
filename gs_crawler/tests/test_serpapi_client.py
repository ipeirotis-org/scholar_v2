"""Tests for serpapi_client.py."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from gs_crawler import serpapi_client
from gs_crawler.serpapi_client import SerpApiError, validate_author_page
from gs_crawler.tests.conftest import make_page


def _response(status, data):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = data
    return r


class TestValidateAuthorPage:
    def test_valid_first_page(self):
        validate_author_page(make_page(5), 0)

    def test_empty_first_page_is_invalid(self):
        page = make_page(0)
        with pytest.raises(SerpApiError, match="no articles"):
            validate_author_page(page, 0)

    def test_first_page_without_author_is_invalid(self):
        page = make_page(3)
        page["author"] = {}
        with pytest.raises(SerpApiError, match="no author"):
            validate_author_page(page, 0)

    def test_error_payload_is_invalid(self):
        with pytest.raises(SerpApiError) as exc:
            validate_author_page({"error": "Your account has run out of searches."}, 0)
        assert exc.value.transient

    def test_invalid_author_id_is_permanent(self):
        with pytest.raises(SerpApiError) as exc:
            validate_author_page({"error": "Invalid author_id"}, 0)
        assert not exc.value.transient

    def test_no_results_past_the_end_is_ok(self):
        validate_author_page({"error": "Google hasn't returned any results for this query."}, 100)

    def test_failed_search_status_is_invalid(self):
        page = make_page(3)
        page["search_metadata"]["status"] = "Error"
        with pytest.raises(SerpApiError, match="status Error"):
            validate_author_page(page, 0)


class TestFetchAuthorPage:
    def test_sends_expected_params(self, serpapi_key):
        with patch("requests.get", return_value=_response(200, make_page(3))) as get:
            serpapi_client.fetch_author_page("ABC", 100)
        params = get.call_args[1]["params"]
        assert params["engine"] == "google_scholar_author"
        assert params["author_id"] == "ABC"
        assert params["start"] == 100
        assert params["num"] == 100
        assert params["api_key"] == "test-key"

    def test_retries_empty_200_then_succeeds(self, serpapi_key):
        responses = [_response(200, make_page(0)), _response(200, make_page(3))]
        with patch("requests.get", side_effect=responses) as get:
            page = serpapi_client.fetch_author_page("ABC", 0)
        assert len(page["articles"]) == 3
        assert get.call_count == 2

    def test_gives_up_after_retries(self, serpapi_key):
        with patch("requests.get", return_value=_response(503, {})) as get:
            with pytest.raises(SerpApiError, match="HTTP 503"):
                serpapi_client.fetch_author_page("ABC", 0)
        assert get.call_count == 1 + serpapi_client.Config.SERPAPI_MAX_RETRIES

    def test_permanent_error_is_not_retried(self, serpapi_key):
        with patch("requests.get", return_value=_response(400, {"error": "Invalid author_id"})) as get:
            with pytest.raises(SerpApiError):
                serpapi_client.fetch_author_page("BAD", 0)
        assert get.call_count == 1

    def test_network_error_does_not_leak_key(self, serpapi_key):
        exc = requests.ConnectionError("https://serpapi.com/search.json?api_key=test-key failed")
        with patch("requests.get", side_effect=exc):
            with pytest.raises(SerpApiError) as err:
                serpapi_client.fetch_author_page("ABC", 0)
        assert "test-key" not in str(err.value)
