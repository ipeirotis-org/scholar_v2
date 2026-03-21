"""Tests for refresh HTTP entry points."""

import json
from unittest import mock

import pytest


@pytest.fixture
def client():
    """Create a Flask test client."""
    from refresh.main import app
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


class TestRefreshStale:
    @mock.patch("refresh.main.refresh_service")
    def test_success(self, mock_service, client):
        mock_service.refresh_stale_authors.return_value = {
            "source": "stale", "found": 5, "enqueued": 5, "duplicates": 0, "errors": [],
        }

        resp = client.get("/refresh_stale")

        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["source"] == "stale"
        assert data["enqueued"] == 5

    @mock.patch("refresh.main.refresh_service")
    def test_with_limit(self, mock_service, client):
        mock_service.refresh_stale_authors.return_value = {
            "source": "stale", "found": 0, "enqueued": 0, "duplicates": 0, "errors": [],
        }

        client.get("/refresh_stale?limit=20")

        mock_service.refresh_stale_authors.assert_called_once_with(limit=20)


class TestRefreshErrors:
    @mock.patch("refresh.main.refresh_service")
    def test_success(self, mock_service, client):
        mock_service.refresh_error_authors.return_value = {
            "source": "errors", "found": 2, "enqueued": 2, "duplicates": 0, "errors": [],
        }

        resp = client.get("/refresh_errors")

        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["source"] == "errors"


class TestExpandCoauthors:
    @mock.patch("refresh.main.refresh_service")
    def test_success(self, mock_service, client):
        mock_service.expand_coauthors.return_value = {
            "source": "coauthors", "found": 3, "enqueued": 3, "duplicates": 0, "errors": [],
        }

        resp = client.get("/expand_coauthors")

        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["source"] == "coauthors"

    @mock.patch("refresh.main.refresh_service")
    def test_with_limit_in_body(self, mock_service, client):
        mock_service.expand_coauthors.return_value = {
            "source": "coauthors", "found": 0, "enqueued": 0, "duplicates": 0, "errors": [],
        }

        resp = client.post(
            "/expand_coauthors",
            data=json.dumps({"limit": 5}),
            content_type="application/json",
        )

        mock_service.expand_coauthors.assert_called_once_with(limit=5)


class TestFetchAuthor:
    @mock.patch("refresh.main.refresh_service")
    def test_success(self, mock_service, client):
        mock_service.fetch_author.return_value = {
            "scholar_id": "abc123", "exists": False, "enqueued": True,
        }

        resp = client.post("/fetch_author?scholar_id=abc123")

        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["scholar_id"] == "abc123"
        assert data["enqueued"] is True

    @mock.patch("refresh.main.refresh_service")
    def test_from_json_body(self, mock_service, client):
        mock_service.fetch_author.return_value = {
            "scholar_id": "def456", "exists": True, "enqueued": True,
        }

        resp = client.post(
            "/fetch_author",
            data=json.dumps({"scholar_id": "def456"}),
            content_type="application/json",
        )

        assert resp.status_code == 200
        mock_service.fetch_author.assert_called_once_with("def456")

    def test_missing_scholar_id(self, client):
        resp = client.post("/fetch_author")

        assert resp.status_code == 400
        data = json.loads(resp.data)
        assert "error" in data

    def test_invalid_scholar_id(self, client):
        resp = client.post("/fetch_author?scholar_id=ab")

        assert resp.status_code == 400

    def test_injection_attempt(self, client):
        resp = client.post("/fetch_author?scholar_id=%27%3B+DROP+TABLE--")

        assert resp.status_code == 400


class TestFetchAuthors:
    @mock.patch("refresh.main.refresh_service")
    def test_comma_separated(self, mock_service, client):
        mock_service.fetch_authors.return_value = {
            "source": "user_request", "found": 2, "enqueued": 2,
            "duplicates": 0, "errors": [],
        }

        resp = client.post("/fetch_authors?scholar_ids=abc123,def456")

        assert resp.status_code == 200
        mock_service.fetch_authors.assert_called_once_with(["abc123", "def456"])

    @mock.patch("refresh.main.refresh_service")
    def test_list_in_body(self, mock_service, client):
        mock_service.fetch_authors.return_value = {
            "source": "user_request", "found": 2, "enqueued": 2,
            "duplicates": 0, "errors": [],
        }

        resp = client.post(
            "/fetch_authors",
            data=json.dumps({"scholar_ids": ["abc123", "def456"]}),
            content_type="application/json",
        )

        assert resp.status_code == 200
        mock_service.fetch_authors.assert_called_once_with(["abc123", "def456"])

    def test_no_valid_ids(self, client):
        resp = client.post("/fetch_authors?scholar_ids=ab,x")

        assert resp.status_code == 400

    def test_empty_scholar_ids(self, client):
        resp = client.post("/fetch_authors")

        assert resp.status_code == 400

    @mock.patch("refresh.main.refresh_service")
    def test_filters_invalid_ids(self, mock_service, client):
        mock_service.fetch_authors.return_value = {
            "source": "user_request", "found": 1, "enqueued": 1,
            "duplicates": 0, "errors": [],
        }

        resp = client.post("/fetch_authors?scholar_ids=valid123,ab,good456")

        assert resp.status_code == 200
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
    def test_from_args(self, client):
        from refresh.main import app, _get_int_param
        with app.test_request_context("/?limit=10"):
            assert _get_int_param("limit", 5) == 10

    def test_from_body(self, client):
        from refresh.main import app, _get_int_param
        with app.test_request_context(
            "/", method="POST",
            data=json.dumps({"limit": 15}),
            content_type="application/json",
        ):
            assert _get_int_param("limit", 5) == 15

    def test_default(self, client):
        from refresh.main import app, _get_int_param
        with app.test_request_context("/"):
            assert _get_int_param("limit", 5) == 5

    def test_invalid_value(self, client):
        from refresh.main import app, _get_int_param
        with app.test_request_context("/?limit=abc"):
            assert _get_int_param("limit", 5) == 5

    def test_args_takes_precedence(self, client):
        from refresh.main import app, _get_int_param
        with app.test_request_context(
            "/?limit=10", method="POST",
            data=json.dumps({"limit": 15}),
            content_type="application/json",
        ):
            assert _get_int_param("limit", 5) == 10


class TestHealth:
    def test_health_check(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["status"] == "ok"
