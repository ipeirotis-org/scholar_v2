"""Tests for health dashboard route and service."""

from collections import defaultdict
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
                "recent_fetched_authors": [],
                "recent_analyzed_authors": [],
                "fetch_histogram": [],
                "age_distribution": [],
                "error_authors": [],
                "queues": {},
                "function_executions": None,
                "function_errors": None,
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
            "recent_fetched_authors": [
                {
                    "scholar_id": "xyz789",
                    "name": "Jane Doe",
                    "affiliation": "MIT",
                    "timestamp": "2025-01-15T11:30:00+00:00",
                },
            ],
            "recent_analyzed_authors": [
                {
                    "scholar_id": "abc456",
                    "name": "John Smith",
                    "affiliation": "Stanford",
                    "hindex": 25,
                    "citedby": 3000,
                    "pip_auc_score": 0.6543,
                    "pip_auc_percentile": 0.75,
                    "last_updated": "2025-01-15T10:00:00+00:00",
                },
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
                    "task_count": 42,
                    "rate_limits": {
                        "max_dispatches_per_second": 1.0,
                        "max_concurrent_dispatches": 10,
                    },
                },
                "cache-priority": {
                    "state": "PAUSED",
                    "task_count": 0,
                    "rate_limits": None,
                },
            },
            "function_executions": {
                "totals": {
                    "v3_fetch_author": {
                        "1h": {"ok": 120, "error": 3, "timeout": 1, "total": 124},
                        "3h": {"ok": 350, "error": 8, "timeout": 2, "total": 360},
                        "24h": {"ok": 2800, "error": 45, "timeout": 12, "total": 2857},
                    },
                    "v3_fetch_publication": {
                        "1h": {"ok": 5000, "error": 10, "timeout": 0, "total": 5010},
                        "3h": {"ok": 15000, "error": 30, "timeout": 2, "total": 15032},
                        "24h": {"ok": 120000, "error": 200, "timeout": 15, "total": 120215},
                    },
                },
                "by_region": {
                    "us-central1": {
                        "v3_fetch_author": {"ok": 310, "error": 5, "timeout": 1, "total": 316},
                        "v3_fetch_publication": {"ok": 13000, "error": 20, "timeout": 1, "total": 13021},
                    },
                },
            },
            "function_errors": {
                "v3_fetch_author": {
                    "1h": {429: 3, 500: 1},
                    "3h": {429: 7, 500: 3},
                    "24h": {429: 40, 500: 17},
                },
                "v3_fetch_publication": {
                    "1h": {},
                    "3h": {429: 2},
                    "24h": {429: 10, 500: 5},
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
            # Recently fetched/analyzed authors
            assert b"Recently Fetched Authors" in response.data
            assert b"Recently Analyzed Authors" in response.data
            assert b"xyz789" in response.data
            assert b"Jane Doe" in response.data
            assert b"abc456" in response.data
            assert b"0.6543" in response.data
            # Cloud Function execution data
            assert b"fetch_author" in response.data
            assert b"fetch_publication" in response.data
            assert b"Cloud Function Execution Status" in response.data
            # Per-region data
            assert b"us-central1" in response.data

    def test_api_health_returns_json(self):
        """JSON API returns dashboard data."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.HealthService") as mock_cls:
            instance = mock_cls.return_value
            instance.get_dashboard_data.return_value = {
                "timestamp": "2025-01-15T12:00:00+00:00",
                "authors": None,
                "publications": None,
                "recent_fetched_authors": [],
                "recent_analyzed_authors": [],
                "fetch_histogram": [],
                "age_distribution": [],
                "error_authors": [],
                "queues": {},
                "function_executions": None,
                "function_errors": None,
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
            assert "function_executions" in data
            assert "function_errors" in data


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
        mock_queue.stats.tasks_count = 42

        mock_tasks = mock.MagicMock()
        mock_tasks.get_queue.return_value = mock_queue

        svc = HealthService(tasks_client=mock_tasks)
        result = svc.get_queue_stats()
        assert "process-authors" in result
        assert result["process-authors"]["state"] == "RUNNING"
        assert result["process-authors"]["task_count"] == 42

    def test_get_queue_stats_no_stats(self):
        """Queue without stats field returns task_count=None."""
        from frontend.health_service import HealthService

        mock_queue = mock.MagicMock()
        mock_queue.state.name = "RUNNING"
        mock_queue.stats = None
        mock_queue.rate_limits.max_dispatches_per_second = 1.0
        mock_queue.rate_limits.max_concurrent_dispatches = 10

        mock_tasks = mock.MagicMock()
        mock_tasks.get_queue.return_value = mock_queue

        svc = HealthService(tasks_client=mock_tasks)
        result = svc.get_queue_stats()
        assert result["process-authors"]["task_count"] is None

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

        mock_monitoring = mock.MagicMock()
        mock_monitoring.list_time_series.return_value = iter([])

        mock_logging = mock.MagicMock()
        mock_logging.list_entries.return_value = iter([])

        svc = HealthService(
            bq_client=mock_bq,
            tasks_client=mock_tasks,
            monitoring_client=mock_monitoring,
            logging_client=mock_logging,
        )
        result = svc.get_dashboard_data()
        assert "timestamp" in result
        assert "authors" in result
        assert "publications" in result
        assert "queues" in result
        assert "fetch_histogram" in result
        assert "age_distribution" in result
        assert "error_authors" in result
        assert "function_executions" in result
        assert "function_errors" in result
        assert "recent_fetched_authors" in result
        assert "recent_analyzed_authors" in result

    def test_get_recent_fetched_authors_success(self):
        from frontend.health_service import HealthService

        mock_row = mock.MagicMock()
        mock_row.scholar_id = "xyz789"
        mock_row.name = "Jane Doe"
        mock_row.affiliation = "MIT"
        mock_row.timestamp.isoformat.return_value = "2025-01-15T11:30:00"

        mock_bq = mock.MagicMock()
        mock_bq.query.return_value.result.return_value = [mock_row]

        svc = HealthService(bq_client=mock_bq)
        result = svc.get_recent_fetched_authors()
        assert len(result) == 1
        assert result[0]["scholar_id"] == "xyz789"
        assert result[0]["name"] == "Jane Doe"
        assert result[0]["affiliation"] == "MIT"

    def test_get_recent_fetched_authors_handles_error(self):
        from frontend.health_service import HealthService

        mock_bq = mock.MagicMock()
        mock_bq.query.side_effect = Exception("BQ error")

        svc = HealthService(bq_client=mock_bq)
        result = svc.get_recent_fetched_authors()
        assert result == []

    def test_get_recent_analyzed_authors_success(self):
        from frontend.health_service import HealthService

        mock_row = mock.MagicMock()
        mock_row.scholar_id = "abc456"
        mock_row.name = "John Smith"
        mock_row.affiliation = "Stanford"
        mock_row.hindex = 25
        mock_row.citedby = 3000
        mock_row.pip_auc_score = 0.6543
        mock_row.pip_auc_percentile = 0.75
        mock_row.last_updated.isoformat.return_value = "2025-01-15T10:00:00"

        mock_bq = mock.MagicMock()
        mock_bq.query.return_value.result.return_value = [mock_row]

        svc = HealthService(bq_client=mock_bq)
        result = svc.get_recent_analyzed_authors()
        assert len(result) == 1
        assert result[0]["scholar_id"] == "abc456"
        assert result[0]["pip_auc_score"] == 0.6543
        assert result[0]["pip_auc_percentile"] == 0.75

    def test_get_recent_analyzed_authors_handles_error(self):
        from frontend.health_service import HealthService

        mock_bq = mock.MagicMock()
        mock_bq.query.side_effect = Exception("BQ error")

        svc = HealthService(bq_client=mock_bq)
        result = svc.get_recent_analyzed_authors()
        assert result == []

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


class TestFunctionExecutionStats:
    """Tests for Cloud Monitoring-based function execution metrics."""

    def _make_time_series(self, function_name, status, region, value):
        """Create a mock time series object."""
        ts = mock.MagicMock()
        ts.resource.labels = {"function_name": function_name, "region": region}
        ts.metric.labels = {"status": status}
        point = mock.MagicMock()
        point.value.int64_value = value
        ts.points = [point]
        return ts

    def test_get_function_execution_stats_success(self):
        from frontend.health_service import HealthService

        mock_monitoring = mock.MagicMock()

        # Return different time series for each call (1h, 3h, 24h)
        call_count = [0]

        def mock_list_time_series(request):
            call_count[0] += 1
            return [
                self._make_time_series("v3_fetch_author", "ok", "us-central1", 50),
                self._make_time_series("v3_fetch_author", "error", "us-central1", 2),
                self._make_time_series("v3_fetch_author", "ok", "us-east1", 30),
                self._make_time_series("v3_fetch_publication", "ok", "us-central1", 200),
                self._make_time_series("v3_fetch_publication", "timeout", "us-east1", 1),
            ]

        mock_monitoring.list_time_series.side_effect = mock_list_time_series

        svc = HealthService(monitoring_client=mock_monitoring)
        result = svc.get_function_execution_stats()

        assert result is not None
        assert "totals" in result
        assert "by_region" in result

        # Check totals for each window (same mock data for all 3 calls)
        for window in ["1h", "3h", "24h"]:
            author = result["totals"]["v3_fetch_author"][window]
            assert author["ok"] == 80  # 50 + 30
            assert author["error"] == 2
            assert author["timeout"] == 0
            assert author["total"] == 82

            pub = result["totals"]["v3_fetch_publication"][window]
            assert pub["ok"] == 200
            assert pub["timeout"] == 1
            assert pub["total"] == 201

        # Check per-region (24h window only)
        assert "us-central1" in result["by_region"]
        assert result["by_region"]["us-central1"]["v3_fetch_author"]["ok"] == 50
        assert result["by_region"]["us-central1"]["v3_fetch_author"]["error"] == 2
        assert result["by_region"]["us-east1"]["v3_fetch_author"]["ok"] == 30

        # Monitoring API called 3 times (once per window)
        assert mock_monitoring.list_time_series.call_count == 3

    def test_get_function_execution_stats_handles_error(self):
        from frontend.health_service import HealthService

        mock_monitoring = mock.MagicMock()
        mock_monitoring.list_time_series.side_effect = Exception("Monitoring unavailable")

        svc = HealthService(monitoring_client=mock_monitoring)
        result = svc.get_function_execution_stats()
        assert result is None

    def test_get_function_execution_stats_empty(self):
        from frontend.health_service import HealthService

        mock_monitoring = mock.MagicMock()
        mock_monitoring.list_time_series.return_value = iter([])

        svc = HealthService(monitoring_client=mock_monitoring)
        result = svc.get_function_execution_stats()

        assert result is not None
        for fn in ["v3_fetch_author", "v3_fetch_publication"]:
            for window in ["1h", "3h", "24h"]:
                assert result["totals"][fn][window]["total"] == 0


class TestFunctionErrorBreakdown:
    """Tests for Cloud Logging-based error type breakdown."""

    def test_get_function_error_breakdown_success(self):
        from frontend.health_service import HealthService

        mock_logging = mock.MagicMock()

        # Create mock log entries with HTTP status codes
        entry_429 = mock.MagicMock()
        entry_429.http_request = mock.MagicMock()
        entry_429.http_request.status = 429

        entry_500 = mock.MagicMock()
        entry_500.http_request = mock.MagicMock()
        entry_500.http_request.status = 500

        # list_entries called per window then per function:
        # 1h/author, 1h/pub, 3h/author, 3h/pub, 24h/author, 24h/pub
        mock_logging.list_entries.side_effect = [
            [entry_429, entry_429, entry_500],  # 1h, fetch_author
            [],                                  # 1h, fetch_publication
            [entry_429, entry_500],              # 3h, fetch_author
            [entry_429],                         # 3h, fetch_publication
            [entry_429],                         # 24h, fetch_author
            [entry_500, entry_500],              # 24h, fetch_publication
        ]

        svc = HealthService(logging_client=mock_logging)
        result = svc.get_function_error_breakdown()

        assert result is not None
        assert result["v3_fetch_author"]["1h"] == {429: 2, 500: 1}
        assert result["v3_fetch_author"]["3h"] == {429: 1, 500: 1}
        assert result["v3_fetch_author"]["24h"] == {429: 1}
        assert result["v3_fetch_publication"]["1h"] == {}
        assert result["v3_fetch_publication"]["3h"] == {429: 1}
        assert result["v3_fetch_publication"]["24h"] == {500: 2}

        # 6 calls total: 3 windows x 2 functions
        assert mock_logging.list_entries.call_count == 6

    def test_get_function_error_breakdown_handles_error(self):
        from frontend.health_service import HealthService

        mock_logging = mock.MagicMock()
        mock_logging.list_entries.side_effect = Exception("Logging unavailable")

        svc = HealthService(logging_client=mock_logging)
        result = svc.get_function_error_breakdown()
        assert result is None

    def test_get_function_error_breakdown_empty(self):
        from frontend.health_service import HealthService

        mock_logging = mock.MagicMock()
        mock_logging.list_entries.return_value = []

        svc = HealthService(logging_client=mock_logging)
        result = svc.get_function_error_breakdown()

        assert result is not None
        for fn in ["v3_fetch_author", "v3_fetch_publication"]:
            for window in ["1h", "3h", "24h"]:
                assert result[fn][window] == {}
