"""Tests for v3 frontend route handlers.

The frontend is cache-read-only — it reads from Firestore and enqueues
tasks to the Cache Layer on cache miss. No BigQuery dependency.
"""

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
    def test_results_missing_author_id_redirects(self, client):
        response = client.get("/results")
        assert response.status_code == 302

    def test_results_invalid_author_id_redirects(self, client):
        response = client.get("/results?author_id=<script>alert(1)</script>")
        assert response.status_code == 302

    def test_results_cache_miss_shows_loading(self):
        """When freshness cache is empty, enqueue and show redirect/loading."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate") as mock_enqueue, \
             mock.patch("frontend.routes.enqueue_author_crawl") as mock_crawl:
            cache_instance = cache_cls.return_value
            cache_instance.get.return_value = None  # Everything is a cache miss

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/results?author_id=abc123def456")
            assert response.status_code == 200
            assert b"processing" in response.data.lower() or b"fetched" in response.data.lower()
            mock_enqueue.assert_called()
            mock_crawl.assert_called_once_with("abc123def456")


class TestInputValidation:
    def test_scholar_id_validation_accepts_valid(self, client):
        from frontend.routes import _validate_scholar_id
        assert _validate_scholar_id("abc123def456") == "abc123def456"
        assert _validate_scholar_id("A-B_c123") == "A-B_c123"

    def test_scholar_id_validation_rejects_invalid(self, client):
        from frontend.routes import _validate_scholar_id
        assert _validate_scholar_id("") is None
        assert _validate_scholar_id(None) is None
        assert _validate_scholar_id("<script>") is None
        assert _validate_scholar_id("ab") is None  # too short

    def test_pub_id_validation_accepts_valid(self, client):
        from frontend.routes import _validate_author_pub_id
        assert _validate_author_pub_id("abc123:def456") == "abc123:def456"

    def test_pub_id_validation_rejects_invalid(self, client):
        from frontend.routes import _validate_author_pub_id
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
        """Speed check reads from cache only — no BigQuery."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls:
            cache_instance = cache_cls.return_value

            def cache_get(collection, doc_id):
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
