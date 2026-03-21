"""Tests for health dashboard route and service."""

from unittest import mock

import pytest


class TestHealthDashboardRoute:
    def test_health_dashboard_returns_200(self):
        """Health dashboard renders even when BQ/Tasks queries fail gracefully."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.HealthService") as mock_cls:
            instance = mock_cls.return_value
            instance.get_dashboard_data.return_value = {
                "timestamp": "2025-01-15T12:00:00+00:00",
                "authors": None,
                "publications": None,
                "fetch_histogram": [],
                "age_distribution": [],
                "error_authors": [],
                "queues": {},
            }
            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            client = app.test_client()
            response = client.get("/health-dashboard")
            assert response.status_code == 200
            assert b"System Health Dashboard" in response.data
            instance.get_dashboard_data.assert_called_once()

    def test_health_dashboard_with_full_data(self):
        """Health dashboard renders correctly with all data populated."""
        from frontend.app import create_app

        full_data = {
            "timestamp": "2025-01-15T12:00:00+00:00",
            "authors": {
                "total_authors": 15000,
                "stale_authors": 200,
                "error_authors": 15,
                "fetched_1d": 50,
                "fetched_7d": 350,
                "fetched_30d": 1200,
                "fetched_90d": 14800,
                "oldest_fetch": "2022-01-01T00:00:00+00:00",
                "newest_fetch": "2025-01-15T11:00:00+00:00",
            },
            "publications": {
                "total_publications": 500000,
                "oldest_fetch": "2022-01-01T00:00:00+00:00",
                "newest_fetch": "2025-01-15T11:30:00+00:00",
            },
            "fetch_histogram": [
                {"week": "2025-01-06", "count": 100},
                {"week": "2025-01-13", "count": 150},
            ],
            "age_distribution": [
                {"bucket": "< 1 day", "count": 50},
                {"bucket": "1-7 days", "count": 300},
                {"bucket": "30-90 days", "count": 5000},
            ],
            "error_authors": [
                {
                    "scholar_id": "abc123def456",
                    "error": "Author not found",
                    "timestamp": "2025-01-15T10:00:00+00:00",
                },
            ],
            "queues": {
                "process-authors": {
                    "state": "RUNNING",
                    "rate_limits": {
                        "max_dispatches_per_second": 1.0,
                        "max_concurrent_dispatches": 10,
                    },
                },
                "cache-priority": {
                    "state": "PAUSED",
                    "rate_limits": None,
                },
            },
        }

        with mock.patch("frontend.routes.HealthService") as mock_cls:
            instance = mock_cls.return_value
            instance.get_dashboard_data.return_value = full_data
            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            client = app.test_client()
            response = client.get("/health-dashboard")
            assert response.status_code == 200
            assert b"15,000" in response.data  # formatted total_authors
            assert b"500,000" in response.data  # formatted total_publications
            assert b"process-authors" in response.data
            assert b"RUNNING" in response.data
            assert b"abc123def456" in response.data

    def test_api_health_returns_json(self):
        """JSON API returns dashboard data."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.HealthService") as mock_cls:
            instance = mock_cls.return_value
            instance.get_dashboard_data.return_value = {
                "timestamp": "2025-01-15T12:00:00+00:00",
                "authors": None,
                "publications": None,
                "fetch_histogram": [],
                "age_distribution": [],
                "error_authors": [],
                "queues": {},
            }
            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            client = app.test_client()
            response = client.get("/api/health")
            assert response.status_code == 200
            assert response.content_type == "application/json"
            data = response.get_json()
            assert "timestamp" in data
            assert "authors" in data
            assert "queues" in data


class TestHealthService:
    def test_get_author_stats_success(self):
        from frontend.health_service import HealthService

        mock_row = mock.MagicMock()
        mock_row.total_authors = 15000
        mock_row.stale_authors = 200
        mock_row.error_authors = 15
        mock_row.fetched_1d = 50
        mock_row.fetched_7d = 350
        mock_row.fetched_30d = 1200
        mock_row.fetched_90d = 14800
        mock_row.oldest_fetch = "2022-01-01"
        mock_row.newest_fetch = "2025-01-15"

        mock_bq = mock.MagicMock()
        mock_bq.query.return_value.result.return_value = [mock_row]

        svc = HealthService(bq_client=mock_bq)
        result = svc.get_author_stats()
        assert result["total_authors"] == 15000
        assert result["stale_authors"] == 200
        mock_bq.query.assert_called_once()

    def test_get_author_stats_handles_error(self):
        from frontend.health_service import HealthService

        mock_bq = mock.MagicMock()
        mock_bq.query.side_effect = Exception("BQ error")

        svc = HealthService(bq_client=mock_bq)
        result = svc.get_author_stats()
        assert result is None

    def test_get_publication_stats_success(self):
        from frontend.health_service import HealthService

        mock_row = mock.MagicMock()
        mock_row.total_publications = 500000
        mock_row.oldest_fetch = "2022-01-01"
        mock_row.newest_fetch = "2025-01-15"

        mock_bq = mock.MagicMock()
        mock_bq.query.return_value.result.return_value = [mock_row]

        svc = HealthService(bq_client=mock_bq)
        result = svc.get_publication_stats()
        assert result["total_publications"] == 500000

    def test_get_fetch_date_histogram(self):
        from frontend.health_service import HealthService

        mock_row = mock.MagicMock()
        mock_row.week_start.isoformat.return_value = "2025-01-06"
        mock_row.author_count = 100

        mock_bq = mock.MagicMock()
        mock_bq.query.return_value.result.return_value = [mock_row]

        svc = HealthService(bq_client=mock_bq)
        result = svc.get_fetch_date_histogram()
        assert len(result) == 1
        assert result[0]["week"] == "2025-01-06"
        assert result[0]["count"] == 100

    def test_get_queue_stats_success(self):
        from frontend.health_service import HealthService

        mock_queue = mock.MagicMock()
        mock_queue.state.name = "RUNNING"
        mock_queue.rate_limits.max_dispatches_per_second = 1.0
        mock_queue.rate_limits.max_concurrent_dispatches = 10

        mock_tasks = mock.MagicMock()
        mock_tasks.get_queue.return_value = mock_queue

        svc = HealthService(tasks_client=mock_tasks)
        result = svc.get_queue_stats()
        assert "process-authors" in result
        assert result["process-authors"]["state"] == "RUNNING"

    def test_get_queue_stats_handles_error(self):
        from frontend.health_service import HealthService

        mock_tasks = mock.MagicMock()
        mock_tasks.get_queue.side_effect = Exception("Queue not found")

        svc = HealthService(tasks_client=mock_tasks)
        result = svc.get_queue_stats()
        assert result["process-authors"]["state"] == "ERROR"
        assert "Queue not found" in result["process-authors"]["error"]

    def test_get_dashboard_data_aggregates_all(self):
        from frontend.health_service import HealthService

        mock_bq = mock.MagicMock()
        mock_bq.query.return_value.result.return_value = []

        mock_tasks = mock.MagicMock()
        mock_tasks.get_queue.side_effect = Exception("unavailable")

        svc = HealthService(bq_client=mock_bq, tasks_client=mock_tasks)
        result = svc.get_dashboard_data()
        assert "timestamp" in result
        assert "authors" in result
        assert "publications" in result
        assert "queues" in result
        assert "fetch_histogram" in result
        assert "age_distribution" in result
        assert "error_authors" in result

    def test_get_error_authors_sample(self):
        from frontend.health_service import HealthService

        mock_row = mock.MagicMock()
        mock_row.scholar_id = "abc123"
        mock_row.error = "Not found"
        mock_row.timestamp.isoformat.return_value = "2025-01-15T10:00:00"

        mock_bq = mock.MagicMock()
        mock_bq.query.return_value.result.return_value = [mock_row]

        svc = HealthService(bq_client=mock_bq)
        result = svc.get_error_authors_sample()
        assert len(result) == 1
        assert result[0]["scholar_id"] == "abc123"
        assert result[0]["error"] == "Not found"
