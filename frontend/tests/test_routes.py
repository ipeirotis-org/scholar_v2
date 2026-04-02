"""Tests for v3 frontend route handlers.

The frontend is cache-read-only — it reads from Firestore and enqueues
tasks to the Cache Layer on cache miss. No BigQuery dependency.
Visualization is client-side via Plotly.js — no matplotlib.
"""

import datetime
from unittest import mock

import pytest


class TestIndexRoute:
    def test_index_returns_200(self, client):
        response = client.get("/")
        assert response.status_code == 200
        assert b"PiP Score" in response.data

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
        """When freshness cache is empty, enqueue cache populate and show redirect."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate") as mock_enqueue:
            cache_instance = cache_cls.return_value
            cache_instance.get.return_value = None  # Everything is a cache miss

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/results?author_id=abc123def456")
            assert response.status_code == 200
            assert b"processing" in response.data.lower() or b"prepared" in response.data.lower()
            mock_enqueue.assert_called()

    def test_results_with_cached_data_renders_chart_divs(self):
        """When cache has data, render page with Plotly chart containers."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate"):
            cache_instance = cache_cls.return_value

            def cache_get(collection, doc_id):
                if collection == "v3_author_freshness":
                    return {"exists": True, "last_updated": datetime.datetime(2025, 1, 1)}
                if collection == "v3_author_stats":
                    return {
                        "scholar_id": "123",
                        "name": "Test Author",
                        "affiliation": "Test U",
                        "hindex": 10,
                        "citedby": 500,
                        "citedby_percentile": 0.8,
                        "hindex_percentile": 0.7,
                        "total_publications_with_citations": 30,
                        "total_publications_with_citations_percentile": 0.6,
                        "i10index": 5,
                        "i10index_percentile": 0.5,
                        "pip_auc_score": 0.75,
                        "pip_auc_score_percentile": 0.9,
                        "year_of_first_pub": 2010,
                    }
                if collection == "v3_author_pub_stats":
                    return [
                        {
                            "publication_rank": 1,
                            "num_citations_percentile": 0.9,
                            "num_papers_percentile": 0.8,
                            "pub_year": 2020,
                            "title": "Test Paper",
                            "num_citations": 50,
                        },
                    ]
                if collection == "v3_author_temporal":
                    return [
                        {
                            "state_year": 2020,
                            "h_index": 10,
                            "h_index_percentile": 0.7,
                            "total_citations": 500,
                            "total_citations_percentile": 0.8,
                            "i10_index": 5,
                            "i10_index_percentile": 0.5,
                        },
                    ]
                return None

            cache_instance.get.side_effect = cache_get
            cache_instance.get_timestamp.return_value = datetime.datetime(2025, 1, 1)
            cache_instance.set.return_value = True

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/results?author_id=123")
            assert response.status_code == 200
            # Should contain Plotly chart containers (not base64 images)
            assert b"percentileRankPlot" in response.data
            assert b"pipPlot" in response.data
            assert b"hIndexPlot" in response.data
            # Should contain JSON data for charts
            assert b"renderPercentileRankPlot" in response.data
            # Should NOT contain base64 PNG images
            assert b"data:image/png;base64" not in response.data


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

    def test_scholar_id_validation_accepts_s2_numeric_ids(self, client):
        from frontend.routes import _validate_scholar_id
        assert _validate_scholar_id("2942126") == "2942126"
        assert _validate_scholar_id("2242100447") == "2242100447"

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

    def test_api_rebuild_statistics_no_ids(self, client):
        response = client.post("/api/rebuild_statistics", data={"scholar_ids": ""})
        assert response.status_code == 400

    def test_api_rebuild_statistics_rejects_get(self, client):
        response = client.get("/api/rebuild_statistics?scholar_ids=123")
        assert response.status_code == 405


class TestApiAuthorData:
    def test_api_author_data_invalid_id(self, client):
        response = client.get("/api/author/<bad>/data")
        assert response.status_code == 400
        assert response.json["error"] == "Invalid author ID"

    def test_api_author_data_cache_miss_returns_202(self):
        """When author data is not cached, return 202 and enqueue."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate") as mock_enqueue:
            cache_instance = cache_cls.return_value
            cache_instance.get.return_value = None

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/api/author/2942126/data")
            assert response.status_code == 202
            assert response.json["status"] == "loading"
            mock_enqueue.assert_called()

    def test_api_author_data_not_found_returns_404(self):
        """When freshness says author doesn't exist, return 404."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate"):
            cache_instance = cache_cls.return_value

            def cache_get(collection, doc_id):
                if collection == "v3_author_freshness":
                    return {"exists": False}
                return None

            cache_instance.get.side_effect = cache_get
            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/api/author/9999999/data")
            assert response.status_code == 404
            assert response.json["status"] == "not_found"

    def test_api_author_data_returns_json(self):
        """When cache has data, return structured JSON."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate"):
            cache_instance = cache_cls.return_value

            def cache_get(collection, doc_id):
                if collection == "v3_author_freshness":
                    return {"exists": True, "last_updated": datetime.datetime(2025, 1, 1)}
                if collection == "v3_author_stats":
                    return {"scholar_id": "2942126", "name": "Test", "hindex": 10,
                            "year_of_first_pub": 2010}
                if collection == "v3_author_pub_stats":
                    return [{"publication_rank": 1, "num_citations_percentile": 0.9,
                             "num_papers_percentile": 0.8, "pub_year": 2020,
                             "title": "Paper", "num_citations": 50}]
                if collection == "v3_author_temporal":
                    return [{"state_year": 2020, "h_index": 10,
                             "h_index_percentile": 0.7, "total_citations": 500,
                             "total_citations_percentile": 0.8,
                             "i10_index": 5, "i10_index_percentile": 0.5}]
                return None

            cache_instance.get.side_effect = cache_get
            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/api/author/2942126/data")
            assert response.status_code == 200
            data = response.json
            assert "author" in data
            assert "publications" in data
            assert "temporal" in data
            assert data["author"]["name"] == "Test"
            assert len(data["publications"]) == 1
            assert data["publications"][0]["num_citations_percentile"] == 90.0


class TestApiPublicationData:
    def test_api_publication_data_invalid_id(self, client):
        response = client.get("/api/publication/ab/data")
        assert response.status_code == 400

    def test_api_publication_data_cache_miss_returns_202(self):
        """When pub data is not cached, return 202."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate") as mock_enqueue:
            cache_instance = cache_cls.return_value
            cache_instance.get.return_value = None

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/api/publication/abc123:def456/data")
            assert response.status_code == 202
            mock_enqueue.assert_called()


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


class TestPrepareChartData:
    def test_prepare_pub_chart_data_empty(self):
        from frontend.routes import _prepare_pub_chart_data
        assert _prepare_pub_chart_data(None, {}) == []
        assert _prepare_pub_chart_data([], {}) == []

    def test_prepare_pub_chart_data_transforms(self):
        from frontend.routes import _prepare_pub_chart_data
        pub_stats = [
            {
                "publication_rank": 1,
                "num_citations_percentile": 0.9,
                "num_papers_percentile": 0.8,
                "pub_year": 2020,
                "title": "Test Paper",
                "num_citations": 50,
            },
        ]
        result = _prepare_pub_chart_data(pub_stats, {})
        assert len(result) == 1
        assert result[0]["num_citations_percentile"] == 90.0
        assert result[0]["num_papers_percentile"] == 80.0
        assert result[0]["age"] > 0

    def test_prepare_pub_chart_data_skips_invalid_year(self):
        from frontend.routes import _prepare_pub_chart_data
        pub_stats = [
            {"publication_rank": 1, "num_citations_percentile": 0.9,
             "num_papers_percentile": 0.8, "pub_year": "bad", "title": "X",
             "num_citations": 5},
        ]
        result = _prepare_pub_chart_data(pub_stats, {})
        assert len(result) == 0

    def test_prepare_temporal_chart_data_empty(self):
        from frontend.routes import _prepare_temporal_chart_data
        assert _prepare_temporal_chart_data(None, {}) == []
        assert _prepare_temporal_chart_data([], {}) == []

    def test_prepare_temporal_chart_data_filters_old_years(self):
        from frontend.routes import _prepare_temporal_chart_data
        temporal = [
            {"state_year": 1940, "h_index": 1, "h_index_percentile": 0.1,
             "total_citations": 5, "total_citations_percentile": 0.1,
             "i10_index": 0, "i10_index_percentile": 0},
            {"state_year": 2020, "h_index": 10, "h_index_percentile": 0.7,
             "total_citations": 500, "total_citations_percentile": 0.8,
             "i10_index": 5, "i10_index_percentile": 0.5},
        ]
        result = _prepare_temporal_chart_data(temporal, {})
        assert len(result) == 1
        assert result[0]["state_year"] == 2020

    def test_prepare_temporal_chart_data_respects_first_pub_year(self):
        from frontend.routes import _prepare_temporal_chart_data
        temporal = [
            {"state_year": 2000, "h_index": 1, "h_index_percentile": 0.1,
             "total_citations": 5, "total_citations_percentile": 0.1,
             "i10_index": 0, "i10_index_percentile": 0},
            {"state_year": 2015, "h_index": 10, "h_index_percentile": 0.7,
             "total_citations": 500, "total_citations_percentile": 0.8,
             "i10_index": 5, "i10_index_percentile": 0.5},
        ]
        result = _prepare_temporal_chart_data(temporal, {"year_of_first_pub": 2010})
        assert len(result) == 1
        assert result[0]["state_year"] == 2015


class TestErrorHandlers:
    def test_404_handler(self, client):
        response = client.get("/nonexistent-page")
        assert response.status_code == 404
        assert b"Page not found" in response.data
