"""Tests for refresh HTTP entry points."""

import json
from unittest import mock


def _make_request(json_data=None, args=None, headers=None):
    """Create a mock HTTP request."""
    req = mock.MagicMock()
    req.get_json.return_value = json_data
    req.args = args or {}
    req.headers = headers or {"Function-Execution-Id": "test-123"}
    return req


class TestRefreshStale:
    @mock.patch("refresh.main.refresh_service")
    def test_success(self, mock_service):
        mock_service.refresh_stale_authors.return_value = {
            "source": "stale", "found": 5, "enqueued": 5, "duplicates": 0, "errors": [],
        }

        from refresh.main import refresh_stale
        body, status = refresh_stale(_make_request())

        assert status == 200
        data = json.loads(body)
        assert data["source"] == "stale"
        assert data["enqueued"] == 5

    @mock.patch("refresh.main.refresh_service")
    def test_with_limit(self, mock_service):
        mock_service.refresh_stale_authors.return_value = {
            "source": "stale", "found": 0, "enqueued": 0, "duplicates": 0, "errors": [],
        }

        from refresh.main import refresh_stale
        refresh_stale(_make_request(args={"limit": "20"}))

        mock_service.refresh_stale_authors.assert_called_once_with(limit=20)


class TestRefreshErrors:
    @mock.patch("refresh.main.refresh_service")
    def test_success(self, mock_service):
        mock_service.refresh_error_authors.return_value = {
            "source": "errors", "found": 2, "enqueued": 2, "duplicates": 0, "errors": [],
        }

        from refresh.main import refresh_errors
        body, status = refresh_errors(_make_request())

        assert status == 200
        data = json.loads(body)
        assert data["source"] == "errors"


class TestExpandCoauthors:
    @mock.patch("refresh.main.refresh_service")
    def test_success(self, mock_service):
        mock_service.expand_coauthors.return_value = {
            "source": "coauthors", "found": 3, "enqueued": 3, "duplicates": 0, "errors": [],
        }

        from refresh.main import expand_coauthors
        body, status = expand_coauthors(_make_request())

        assert status == 200
        data = json.loads(body)
        assert data["source"] == "coauthors"

    @mock.patch("refresh.main.refresh_service")
    def test_with_limit_in_body(self, mock_service):
        mock_service.expand_coauthors.return_value = {
            "source": "coauthors", "found": 0, "enqueued": 0, "duplicates": 0, "errors": [],
        }

        from refresh.main import expand_coauthors
        expand_coauthors(_make_request(json_data={"limit": 5}))

        mock_service.expand_coauthors.assert_called_once_with(limit=5)


class TestFetchAuthor:
    @mock.patch("refresh.main.refresh_service")
    def test_success(self, mock_service):
        mock_service.fetch_author.return_value = {
            "scholar_id": "abc123", "exists": False, "enqueued": True,
        }

        from refresh.main import fetch_author
        body, status = fetch_author(_make_request(args={"scholar_id": "abc123"}))

        assert status == 200
        data = json.loads(body)
        assert data["scholar_id"] == "abc123"
        assert data["enqueued"] is True

    @mock.patch("refresh.main.refresh_service")
    def test_from_json_body(self, mock_service):
        mock_service.fetch_author.return_value = {
            "scholar_id": "def456", "exists": True, "enqueued": True,
        }

        from refresh.main import fetch_author
        body, status = fetch_author(_make_request(json_data={"scholar_id": "def456"}))

        assert status == 200
        mock_service.fetch_author.assert_called_once_with("def456")

    def test_missing_scholar_id(self):
        from refresh.main import fetch_author
        body, status = fetch_author(_make_request())

        assert status == 400
        data = json.loads(body)
        assert "error" in data

    def test_invalid_scholar_id(self):
        from refresh.main import fetch_author
        body, status = fetch_author(_make_request(args={"scholar_id": "ab"}))

        assert status == 400

    def test_injection_attempt(self):
        from refresh.main import fetch_author
        body, status = fetch_author(_make_request(args={"scholar_id": "'; DROP TABLE--"}))

        assert status == 400


class TestFetchAuthors:
    @mock.patch("refresh.main.refresh_service")
    def test_comma_separated(self, mock_service):
        mock_service.fetch_authors.return_value = {
            "source": "user_request", "found": 2, "enqueued": 2,
            "duplicates": 0, "errors": [],
        }

        from refresh.main import fetch_authors
        body, status = fetch_authors(
            _make_request(args={"scholar_ids": "abc123,def456"})
        )

        assert status == 200
        mock_service.fetch_authors.assert_called_once_with(["abc123", "def456"])

    @mock.patch("refresh.main.refresh_service")
    def test_list_in_body(self, mock_service):
        mock_service.fetch_authors.return_value = {
            "source": "user_request", "found": 2, "enqueued": 2,
            "duplicates": 0, "errors": [],
        }

        from refresh.main import fetch_authors
        body, status = fetch_authors(
            _make_request(json_data={"scholar_ids": ["abc123", "def456"]})
        )

        assert status == 200
        mock_service.fetch_authors.assert_called_once_with(["abc123", "def456"])

    def test_no_valid_ids(self):
        from refresh.main import fetch_authors
        body, status = fetch_authors(_make_request(args={"scholar_ids": "ab,x"}))

        assert status == 400

    def test_empty_scholar_ids(self):
        from refresh.main import fetch_authors
        body, status = fetch_authors(_make_request())

        assert status == 400

    @mock.patch("refresh.main.refresh_service")
    def test_filters_invalid_ids(self, mock_service):
        mock_service.fetch_authors.return_value = {
            "source": "user_request", "found": 1, "enqueued": 1,
            "duplicates": 0, "errors": [],
        }

        from refresh.main import fetch_authors
        # Mix of valid and invalid IDs
        body, status = fetch_authors(
            _make_request(args={"scholar_ids": "valid123,ab,good456"})
        )

        assert status == 200
        mock_service.fetch_authors.assert_called_once_with(["valid123", "good456"])


class TestValidateScholarId:
    def test_valid_ids(self):
        from refresh.main import _validate_scholar_id
        assert _validate_scholar_id("abc123") == "abc123"
        assert _validate_scholar_id("A-B_C-123") == "A-B_C-123"
        assert _validate_scholar_id("abcd") == "abcd"  # min length 4

    def test_invalid_ids(self):
        from refresh.main import _validate_scholar_id
        assert _validate_scholar_id("") is None
        assert _validate_scholar_id("ab") is None  # too short
        assert _validate_scholar_id("a" * 21) is None  # too long
        assert _validate_scholar_id(None) is None
        assert _validate_scholar_id("abc!@#") is None


class TestGetIntParam:
    def test_from_args(self):
        from refresh.main import _get_int_param
        req = _make_request(args={"limit": "10"})
        assert _get_int_param(req, "limit", 5) == 10

    def test_from_body(self):
        from refresh.main import _get_int_param
        req = _make_request(json_data={"limit": 15})
        assert _get_int_param(req, "limit", 5) == 15

    def test_default(self):
        from refresh.main import _get_int_param
        req = _make_request()
        assert _get_int_param(req, "limit", 5) == 5

    def test_invalid_value(self):
        from refresh.main import _get_int_param
        req = _make_request(args={"limit": "abc"})
        assert _get_int_param(req, "limit", 5) == 5

    def test_args_takes_precedence(self):
        from refresh.main import _get_int_param
        req = _make_request(json_data={"limit": 15}, args={"limit": "10"})
        assert _get_int_param(req, "limit", 5) == 10
