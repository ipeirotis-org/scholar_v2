"""Tests for scholarly_client module."""

import sys
from unittest import mock

import pytest

# Mock scholarly before importing scholarly_client
mock_scholarly_module = mock.MagicMock()
sys.modules["scholarly"] = mock_scholarly_module
mock_scholarly = mock_scholarly_module.scholarly

from crawler.scholarly_client import (
    ErrorKind,
    ScholarlyError,
    convert_large_integers,
    serialize_author,
    serialize_publication,
    fetch_author,
    fetch_publication,
    _classify_error,
)


class TestConvertLargeIntegers:
    def test_small_int_unchanged(self):
        assert convert_large_integers(42) == 42

    def test_large_int_to_string(self):
        big = 2**63
        assert convert_large_integers(big) == str(big)

    def test_negative_large_int(self):
        big = -(2**63)
        assert convert_large_integers(big) == str(big)

    def test_boundary_int_unchanged(self):
        boundary = 2**62
        assert convert_large_integers(boundary) == boundary

    def test_dict_recursive(self):
        data = {"a": 2**63, "b": {"c": 5}}
        result = convert_large_integers(data)
        assert result == {"a": str(2**63), "b": {"c": 5}}

    def test_list_recursive(self):
        data = [1, 2**63, [3]]
        result = convert_large_integers(data)
        assert result == [1, str(2**63), [3]]

    def test_non_int_passthrough(self):
        assert convert_large_integers("hello") == "hello"
        assert convert_large_integers(3.14) == 3.14
        assert convert_large_integers(None) is None
        assert convert_large_integers(True) is True


class TestSerializeAuthor:
    def test_simplifies_publications(self):
        author = {
            "scholar_id": "abc123",
            "name": "Test Author",
            "publications": [
                {
                    "author_pub_id": "abc123:pub1",
                    "num_citations": 10,
                    "filled": True,
                    "bib": {"title": "Paper One", "pub_year": "2020", "abstract": "long text"},
                    "extra_field": "should be dropped",
                },
            ],
        }
        result = serialize_author(author)
        assert result["scholar_id"] == "abc123"
        assert len(result["publications"]) == 1
        pub = result["publications"][0]
        assert pub["author_pub_id"] == "abc123:pub1"
        assert pub["num_citations"] == 10
        assert pub["filled"] is False
        assert pub["bib"] == {"title": "Paper One", "pub_year": "2020"}
        assert "extra_field" not in pub

    def test_handles_empty_publications(self):
        author = {"scholar_id": "abc", "publications": []}
        result = serialize_author(author)
        assert result["publications"] == []

    def test_does_not_mutate_input(self):
        author = {"publications": [{"author_pub_id": "x", "bib": {"title": "T"}}]}
        original_pubs = author["publications"][0].copy()
        serialize_author(author)
        assert author["publications"][0]["author_pub_id"] == original_pubs["author_pub_id"]


class TestSerializePublication:
    def test_basic_serialization(self):
        pub = {"author_pub_id": "abc:pub1", "num_citations": 5, "bib": {"title": "Test"}}
        result = serialize_publication(pub)
        assert result["author_pub_id"] == "abc:pub1"

    def test_handles_large_integers(self):
        pub = {"big_num": 2**63}
        result = serialize_publication(pub)
        assert result["big_num"] == str(2**63)


class TestClassifyError:
    def test_rate_limit_is_transient(self):
        assert _classify_error(Exception("429 Too Many Requests")) == ErrorKind.TRANSIENT

    def test_timeout_is_transient(self):
        assert _classify_error(Exception("connection timeout")) == ErrorKind.TRANSIENT

    def test_captcha_is_transient(self):
        assert _classify_error(Exception("captcha required")) == ErrorKind.TRANSIENT

    def test_cannot_fetch_is_transient(self):
        assert _classify_error(Exception("Cannot Fetch from Google Scholar.")) == ErrorKind.TRANSIENT

    def test_unknown_is_permanent(self):
        assert _classify_error(Exception("author not found")) == ErrorKind.PERMANENT


class TestFetchAuthor:
    def setup_method(self):
        mock_scholarly.reset_mock()

    def test_success(self):
        mock_scholarly.search_author_id.return_value = {"scholar_id": "abc"}
        mock_scholarly.fill.return_value = {"scholar_id": "abc", "name": "Test", "publications": []}

        result = fetch_author("abc", timeout=5)
        assert result["scholar_id"] == "abc"
        mock_scholarly.search_author_id.assert_called_once_with("abc")
        mock_scholarly.fill.assert_called_once()

    def test_permanent_error_no_retry(self):
        mock_scholarly.search_author_id.side_effect = Exception("author not found")

        with pytest.raises(ScholarlyError) as exc_info:
            fetch_author("abc", timeout=5)
        assert exc_info.value.kind == ErrorKind.PERMANENT
        assert mock_scholarly.search_author_id.call_count == 1

    @mock.patch("crawler.scholarly_client.time.sleep")
    def test_transient_error_retries(self, mock_sleep):
        mock_scholarly.search_author_id.side_effect = Exception("429 rate limit hit")

        with pytest.raises(ScholarlyError) as exc_info:
            fetch_author("abc", timeout=5)
        assert exc_info.value.kind == ErrorKind.TRANSIENT
        # 1 initial + 2 retries = 3 calls
        assert mock_scholarly.search_author_id.call_count == 3

    @mock.patch("crawler.scholarly_client.time.sleep")
    def test_transient_then_success(self, mock_sleep):
        mock_scholarly.search_author_id.side_effect = [
            Exception("429 rate limit"),
            {"scholar_id": "abc"},
        ]
        mock_scholarly.fill.return_value = {"scholar_id": "abc", "publications": []}

        result = fetch_author("abc", timeout=5)
        assert result["scholar_id"] == "abc"
        assert mock_scholarly.search_author_id.call_count == 2


class TestFetchPublication:
    def setup_method(self):
        mock_scholarly.reset_mock()

    def test_success(self):
        pub_data = {"author_pub_id": "abc:pub1"}
        mock_scholarly.fill.return_value = {"author_pub_id": "abc:pub1", "num_citations": 10}

        result = fetch_publication(pub_data, timeout=5)
        assert result["author_pub_id"] == "abc:pub1"

    def test_sets_source_if_missing(self):
        pub_data = {"author_pub_id": "abc:pub1"}
        mock_scholarly.fill.return_value = pub_data

        fetch_publication(pub_data, timeout=5)
        call_args = mock_scholarly.fill.call_args[0][0]
        assert "source" in call_args


class TestDirectTimeoutCap:
    """Verify Phase 1 direct attempts use a capped timeout."""

    def setup_method(self):
        mock_scholarly.reset_mock()

    @mock.patch("crawler.scholarly_client._enable_scraper_api", return_value=False)
    @mock.patch("crawler.scholarly_client._run_with_timeout")
    @mock.patch("crawler.scholarly_client.time.sleep")
    def test_direct_timeout_capped_for_short_timeout(self, mock_sleep, mock_run, mock_enable):
        """When overall timeout is 60s, direct attempts use min(60, max(15, 15))=15s."""
        mock_run.side_effect = Exception("429 rate limit")

        with pytest.raises(ScholarlyError):
            fetch_publication({"author_pub_id": "abc:pub1"}, timeout=60)

        # All Phase 1 calls should use 15s (60//4=15), not 60s
        for call in mock_run.call_args_list:
            assert call[0][1] == 15

    @mock.patch("crawler.scholarly_client._enable_scraper_api", return_value=False)
    @mock.patch("crawler.scholarly_client._run_with_timeout")
    @mock.patch("crawler.scholarly_client.time.sleep")
    def test_direct_timeout_scales_for_long_timeout(self, mock_sleep, mock_run, mock_enable):
        """When overall timeout is 300s (fetch_author), direct attempts use 75s."""
        mock_run.side_effect = Exception("429 rate limit")

        with pytest.raises(ScholarlyError):
            fetch_author("abc123", timeout=300)

        # All Phase 1 calls should use 75s (300//4), not 15s
        for call in mock_run.call_args_list:
            assert call[0][1] == 75

    @mock.patch("crawler.scholarly_client._enable_scraper_api", return_value=False)
    @mock.patch("crawler.scholarly_client._run_with_timeout")
    @mock.patch("crawler.scholarly_client.time.sleep")
    def test_short_timeout_not_capped(self, mock_sleep, mock_run, mock_enable):
        """When overall timeout is short (e.g. 5s), direct cap has no effect."""
        mock_run.side_effect = Exception("429 rate limit")

        with pytest.raises(ScholarlyError):
            fetch_publication({"author_pub_id": "abc:pub1"}, timeout=5)

        for call in mock_run.call_args_list:
            assert call[0][1] == 5

    @mock.patch("crawler.scholarly_client._enable_scraper_api", return_value=True)
    @mock.patch("crawler.scholarly_client._run_with_timeout")
    @mock.patch("crawler.scholarly_client.time.sleep")
    def test_scraper_api_uses_full_timeout(self, mock_sleep, mock_run, mock_enable):
        """Phase 2 ScraperAPI attempts should use the full timeout, not the cap."""
        direct_calls = 3
        mock_run.side_effect = [
            Exception("429 rate limit"),  # direct 1
            Exception("429 rate limit"),  # direct 2
            Exception("429 rate limit"),  # direct 3
            {"author_pub_id": "abc:pub1"},  # scraperapi 1 succeeds
        ]

        result = fetch_publication({"author_pub_id": "abc:pub1"}, timeout=60)
        assert result["author_pub_id"] == "abc:pub1"

        # First 3 calls (direct) should use 15s
        for i in range(direct_calls):
            assert mock_run.call_args_list[i][0][1] == 15
        # 4th call (ScraperAPI) should use full 60s
        assert mock_run.call_args_list[3][0][1] == 60
