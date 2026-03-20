"""Tests for v3 frontend route handlers."""

import datetime
from unittest import mock

import pytest


class TestIndexRoute:
    def test_index_returns_200(self, client):
        response = client.get("/")
        assert response.status_code == 200
        assert b"Scholar Analytics" in response.data

    def test_index_alias(self, client):
        response = client.get("/index")
        assert response.status_code == 200


class TestResultsRoute:
    @mock.patch("v3.frontend.routes.BigQueryClient")
    @mock.patch("v3.frontend.routes.FirestoreCache")
    def test_results_missing_author_id_redirects(self, mock_cache, mock_bq, client):
        response = client.get("/results")
        assert response.status_code == 302

    @mock.patch("v3.frontend.routes.BigQueryClient")
    @mock.patch("v3.frontend.routes.FirestoreCache")
    def test_results_invalid_author_id_redirects(self, mock_cache, mock_bq, client):
        response = client.get("/results?author_id=<script>alert(1)</script>")
        assert response.status_code == 302

    @mock.patch("v3.frontend.routes.BigQueryClient")
    @mock.patch("v3.frontend.routes.FirestoreCache")
    def test_results_nonexistent_author_shows_redirect_page(self, mock_cache_cls, mock_bq_cls, client):
        # The BigQueryClient is instantiated inside register_routes at import time
        # We need to patch at the instance level
        with mock.patch("v3.frontend.routes.BigQueryClient") as bq_cls:
            bq_instance = bq_cls.return_value
            bq_instance.author_exists.return_value = False
            # Re-register routes with patched client
            from v3.frontend.app import create_app
            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/results?author_id=abc123def456")
            assert response.status_code == 200
            assert b"processing" in response.data.lower() or b"queued" in response.data.lower()


class TestInputValidation:
    def test_scholar_id_validation_accepts_valid(self, client):
        from v3.frontend.routes import _validate_scholar_id
        assert _validate_scholar_id("abc123def456") == "abc123def456"
        assert _validate_scholar_id("A-B_c123") == "A-B_c123"

    def test_scholar_id_validation_rejects_invalid(self, client):
        from v3.frontend.routes import _validate_scholar_id
        assert _validate_scholar_id("") is None
        assert _validate_scholar_id(None) is None
        assert _validate_scholar_id("<script>") is None
        assert _validate_scholar_id("ab") is None  # too short

    def test_pub_id_validation_accepts_valid(self, client):
        from v3.frontend.routes import _validate_author_pub_id
        assert _validate_author_pub_id("abc123:def456") == "abc123:def456"

    def test_pub_id_validation_rejects_invalid(self, client):
        from v3.frontend.routes import _validate_author_pub_id
        assert _validate_author_pub_id("") is None
        assert _validate_author_pub_id(None) is None
        assert _validate_author_pub_id("<script>") is None


class TestDataRoute:
    def test_data_page_returns_200(self, client):
        response = client.get("/data")
        assert response.status_code == 200
        assert b"Data Export" in response.data


class TestHelpRoute:
    def test_help_page_returns_200(self, client):
        response = client.get("/help")
        assert response.status_code == 200
        assert b"PiP-AUC" in response.data


class TestSecurityHeaders:
    def test_security_headers_present(self, client):
        response = client.get("/")
        assert response.headers.get("X-Content-Type-Options") == "nosniff"
        assert response.headers.get("X-Frame-Options") == "DENY"
        assert response.headers.get("X-XSS-Protection") == "1; mode=block"
        assert response.headers.get("Referrer-Policy") == "strict-origin-when-cross-origin"
        assert "geolocation=()" in response.headers.get("Permissions-Policy", "")


class TestApiRoutes:
    def test_get_similar_authors_empty_name(self, client):
        response = client.get("/get_similar_authors?author_name=")
        assert response.status_code == 200
        assert response.json == []

    def test_api_refresh_stale_not_configured(self, client):
        response = client.get("/api/refresh_stale_authors")
        assert response.status_code == 200
        assert response.json["status"] == "not_configured"

    def test_api_add_coauthors_not_configured(self, client):
        response = client.get("/api/add_coauthors")
        assert response.status_code == 200
        assert response.json["status"] == "not_configured"

    def test_api_fetch_authors_no_ids(self, client):
        response = client.get("/api/fetch_authors?scholar_ids=")
        assert response.status_code == 400


class TestSpeedCheck:
    def test_speed_check_invalid_author_id(self, client):
        response = client.get("/api/speed_check?author_id=<bad>")
        assert response.status_code == 400
        assert response.json["status"] == "error"

    def test_speed_check_returns_timings(self):
        """Speed check with a mocked BQ/cache returns timing data."""
        from v3.frontend.app import create_app

        with mock.patch("v3.frontend.routes.BigQueryClient") as bq_cls, \
             mock.patch("v3.frontend.routes.FirestoreCache") as cache_cls:
            bq_instance = bq_cls.return_value
            cache_instance = cache_cls.return_value

            # Simulate cached freshness
            def cache_get(collection, doc_id, valid_after=None):
                if collection == "v3_author_freshness":
                    return {"exists": True, "last_updated": datetime.datetime(2025, 1, 1)}
                if collection == "v3_author_stats":
                    return {"name": "Test Author"}
                if collection == "v3_author_pub_stats":
                    return [{"pub_year": 2020, "num_citations_percentile": 0.5}]
                if collection == "v3_author_temporal":
                    return [{"state_year": 2020}]
                return None

            cache_instance.get.side_effect = cache_get
            cache_instance.set.return_value = True

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/api/speed_check?author_id=evnr-MwAAAAJ")
            assert response.status_code == 200
            data = response.json
            assert data["status"] == "ok"
            assert "timings" in data
            assert "total_ms" in data["timings"]
            assert data["has_author_stats"] is True


class TestErrorHandlers:
    def test_404_handler(self, client):
        response = client.get("/nonexistent-page")
        assert response.status_code == 404
        assert b"Page not found" in response.data
