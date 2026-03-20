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
