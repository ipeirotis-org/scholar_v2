"""Tests for cache_layer HTTP entry points."""

import json
from unittest import mock

from cache_layer.main import app


class TestHealthEndpoint:
    def test_health(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.get_json()["status"] == "ok"


class TestPriorityTaskEndpoint:
    @mock.patch("cache_layer.main.service")
    def test_valid_task(self, mock_service, client):
        mock_service.dispatch.return_value = {"status": "ok", "scholar_id": "abc123"}

        response = client.post("/tasks/priority", json={
            "type": "populate_author_profile",
            "scholar_id": "abc123",
        })

        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "ok"
        mock_service.dispatch.assert_called_once_with(
            "populate_author_profile",
            {"type": "populate_author_profile", "scholar_id": "abc123"},
        )

    def test_missing_type(self, client):
        response = client.post("/tasks/priority", json={"scholar_id": "abc123"})
        assert response.status_code == 400

    def test_invalid_json(self, client):
        response = client.post(
            "/tasks/priority",
            data="not json",
            content_type="text/plain",
        )
        assert response.status_code == 400

    @mock.patch("cache_layer.main.service")
    def test_error_response(self, mock_service, client):
        mock_service.dispatch.return_value = {"status": "error", "message": "Something broke"}
        response = client.post("/tasks/priority", json={"type": "bad_type"})
        assert response.status_code == 400


class TestBatchTaskEndpoint:
    @mock.patch("cache_layer.main.service")
    def test_valid_task(self, mock_service, client):
        mock_service.dispatch.return_value = {"status": "ok", "authors_cached": 20}

        response = client.post("/tasks/batch", json={
            "type": "populate_recent_authors",
        })

        assert response.status_code == 200
        assert response.get_json()["authors_cached"] == 20


class TestAdminEndpoints:
    @mock.patch("cache_layer.main.service")
    def test_rebuild(self, mock_service, client):
        mock_service.dispatch.return_value = {
            "status": "ok", "total_authors": 100, "enqueued": 100,
        }
        response = client.post("/admin/rebuild")
        assert response.status_code == 200
        mock_service.dispatch.assert_called_once_with("rebuild_all", {})

    @mock.patch("cache_layer.main.service")
    def test_populate(self, mock_service, client):
        mock_service.dispatch.return_value = {"status": "ok", "scholar_id": "abc123"}
        response = client.post("/admin/populate", json={"scholar_id": "abc123"})
        assert response.status_code == 200

    def test_populate_missing_scholar_id(self, client):
        response = client.post("/admin/populate", json={})
        assert response.status_code == 400

    @mock.patch("cache_layer.main.service")
    def test_flush_cache(self, mock_service, client):
        mock_service.dispatch.return_value = {"status": "ok", "total_deleted": 50}
        response = client.post("/admin/flush_cache")
        assert response.status_code == 200
        mock_service.dispatch.assert_called_once_with("flush_cache", {})

    @mock.patch("cache_layer.main.service")
    def test_flush_cache_partial_failure(self, mock_service, client):
        mock_service.dispatch.return_value = {
            "status": "partial_failure",
            "total_deleted": 30,
            "failed_collections": ["v3_author_stats"],
        }
        response = client.post("/admin/flush_cache")
        assert response.status_code == 207


class TestAdminAuth:
    """Test admin endpoint authentication when ADMIN_AUTH_TOKEN is set."""

    @mock.patch("cache_layer.main.Config.ADMIN_AUTH_TOKEN", "secret-token-123")
    def test_rebuild_rejects_no_auth(self, client):
        response = client.post("/admin/rebuild")
        assert response.status_code == 401

    @mock.patch("cache_layer.main.Config.ADMIN_AUTH_TOKEN", "secret-token-123")
    def test_rebuild_rejects_wrong_token(self, client):
        response = client.post(
            "/admin/rebuild",
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert response.status_code == 401

    @mock.patch("cache_layer.main.Config.ADMIN_AUTH_TOKEN", "secret-token-123")
    @mock.patch("cache_layer.main.service")
    def test_rebuild_accepts_correct_token(self, mock_service, client):
        mock_service.dispatch.return_value = {"status": "ok"}
        response = client.post(
            "/admin/rebuild",
            headers={"Authorization": "Bearer secret-token-123"},
        )
        assert response.status_code == 200

    @mock.patch("cache_layer.main.Config.ADMIN_AUTH_TOKEN", "secret-token-123")
    def test_populate_rejects_no_auth(self, client):
        response = client.post("/admin/populate", json={"scholar_id": "abc123"})
        assert response.status_code == 401

    @mock.patch("cache_layer.main.Config.ADMIN_AUTH_TOKEN", "secret-token-123")
    @mock.patch("cache_layer.main.service")
    def test_populate_accepts_correct_token(self, mock_service, client):
        mock_service.dispatch.return_value = {"status": "ok", "scholar_id": "abc123"}
        response = client.post(
            "/admin/populate",
            json={"scholar_id": "abc123"},
            headers={"Authorization": "Bearer secret-token-123"},
        )
        assert response.status_code == 200

    @mock.patch("cache_layer.main.Config.ADMIN_AUTH_TOKEN", "")
    @mock.patch("cache_layer.main.service")
    def test_no_token_configured_allows_all(self, mock_service, client):
        """When ADMIN_AUTH_TOKEN is empty, all requests are allowed."""
        mock_service.dispatch.return_value = {"status": "ok"}
        response = client.post("/admin/rebuild")
        assert response.status_code == 200

    @mock.patch("cache_layer.main.Config.ADMIN_AUTH_TOKEN", "secret-token-123")
    def test_task_endpoints_not_affected(self, client):
        """Task endpoints should NOT require admin auth."""
        response = client.post(
            "/tasks/priority",
            json={"type": "populate_author_profile", "scholar_id": "abc123"},
        )
        # Should get 400 (missing service mock) not 401
        assert response.status_code != 401
