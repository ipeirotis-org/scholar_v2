"""Tests for CacheService orchestration."""

from datetime import datetime, timezone
from unittest import mock

from cache_layer.cache_service import CacheService
from cache_layer.config import Config


def _make_service(bq=None, writer=None):
    bq = bq or mock.MagicMock()
    writer = writer or mock.MagicMock()
    return CacheService(bq=bq, writer=writer)


class TestDispatch:
    def test_unknown_type_returns_error(self):
        svc = _make_service()
        result = svc.dispatch("nonexistent_type", {})
        assert result["status"] == "error"
        assert "Unknown" in result["message"]

    def test_dispatch_routes_to_handler(self):
        svc = _make_service()
        svc.bq.get_author_freshness.return_value = (True, datetime.now(timezone.utc))
        svc.bq.get_author_stats.return_value = {"name": "Test"}
        svc.bq.get_author_pub_stats.return_value = []
        svc.bq.get_author_temporal_stats.return_value = []
        svc.writer.write_batch.return_value = 1

        result = svc.dispatch("populate_author_profile", {"scholar_id": "abc123"})
        assert result["status"] == "ok"


class TestPopulateAuthorProfile:
    def test_missing_scholar_id(self):
        svc = _make_service()
        result = svc.dispatch("populate_author_profile", {})
        assert result["status"] == "error"
        assert "Missing" in result["message"]

    def test_author_not_found(self):
        svc = _make_service()
        svc.bq.get_author_freshness.return_value = (False, None)

        result = svc.dispatch("populate_author_profile", {"scholar_id": "unknown"})
        assert result["status"] == "not_found"

    def test_full_population(self):
        bq = mock.MagicMock()
        writer = mock.MagicMock()
        svc = CacheService(bq=bq, writer=writer)

        last_updated = datetime(2026, 1, 15, tzinfo=timezone.utc)
        bq.get_author_freshness.return_value = (True, last_updated)
        bq.get_author_stats.return_value = {"name": "Test Author", "hindex": 10}
        bq.get_author_pub_stats.return_value = [
            {"author_pub_id": "abc:1", "title": "Paper 1"},
        ]
        bq.get_author_temporal_stats.return_value = [
            {"state_year": 2020, "hindex": 5},
        ]
        writer.write.return_value = True
        writer.write_batch.return_value = 3

        result = svc.dispatch("populate_author_profile", {"scholar_id": "abc123"})

        assert result["status"] == "ok"
        assert result["scholar_id"] == "abc123"
        assert result["cached"]["author_stats"] is True
        assert result["cached"]["pub_stats"] is True
        assert result["cached"]["temporal_stats"] is True

        # Freshness written separately
        writer.write.assert_called_once_with(
            Config.CACHE_AUTHOR_FRESHNESS, "abc123",
            {"exists": True, "last_updated": last_updated},
        )

        # Batch write for the three data types
        writer.write_batch.assert_called_once()
        writes = writer.write_batch.call_args[0][0]
        assert len(writes) == 3
        collections = [w[0] for w in writes]
        assert Config.CACHE_AUTHOR_STATS in collections
        assert Config.CACHE_AUTHOR_PUB_STATS in collections
        assert Config.CACHE_AUTHOR_TEMPORAL in collections

    def test_partial_data(self):
        bq = mock.MagicMock()
        writer = mock.MagicMock()
        svc = CacheService(bq=bq, writer=writer)

        bq.get_author_freshness.return_value = (True, datetime.now(timezone.utc))
        bq.get_author_stats.return_value = {"name": "Test"}
        bq.get_author_pub_stats.return_value = []  # No publications
        bq.get_author_temporal_stats.return_value = []  # No temporal data
        writer.write.return_value = True
        writer.write_batch.return_value = 1

        result = svc.dispatch("populate_author_profile", {"scholar_id": "abc123"})

        assert result["status"] == "ok"
        assert result["cached"]["pub_stats"] is False
        assert result["cached"]["temporal_stats"] is False

        writes = writer.write_batch.call_args[0][0]
        assert len(writes) == 1  # Only author_stats


class TestPopulatePublicationDetail:
    def test_missing_pub_id(self):
        svc = _make_service()
        result = svc.dispatch("populate_publication_detail", {})
        assert result["status"] == "error"

    def test_publication_not_found(self):
        svc = _make_service()
        svc.bq.get_publication_stats.return_value = []

        result = svc.dispatch("populate_publication_detail", {"author_pub_id": "abc:123"})
        assert result["status"] == "not_found"

    def test_success(self):
        bq = mock.MagicMock()
        writer = mock.MagicMock()
        svc = CacheService(bq=bq, writer=writer)

        bq.get_publication_stats.return_value = [
            {"citation_year": 2020, "yearly_citations": 5},
            {"citation_year": 2021, "yearly_citations": 8},
        ]
        writer.write.return_value = True

        result = svc.dispatch("populate_publication_detail", {"author_pub_id": "abc:123"})

        assert result["status"] == "ok"
        assert result["records"] == 2
        writer.write.assert_called_once_with(
            Config.CACHE_PUB_STATS, "abc:123", bq.get_publication_stats.return_value,
        )


class TestPopulateRecentAuthors:
    def test_success(self):
        bq = mock.MagicMock()
        writer = mock.MagicMock()
        svc = CacheService(bq=bq, writer=writer)

        bq.get_recently_analyzed_authors.return_value = [
            {"scholar_id": "a1", "name": "Author 1"},
            {"scholar_id": "a2", "name": "Author 2"},
        ]
        writer.write.return_value = True

        result = svc.dispatch("populate_recent_authors", {})

        assert result["status"] == "ok"
        assert result["authors_cached"] == 2
        writer.write.assert_called_once_with(
            Config.CACHE_RECENT_AUTHORS, "recent",
            bq.get_recently_analyzed_authors.return_value,
        )

    def test_custom_limit(self):
        svc = _make_service()
        svc.bq.get_recently_analyzed_authors.return_value = []
        svc.writer.write.return_value = True

        svc.dispatch("populate_recent_authors", {"limit": 50})
        svc.bq.get_recently_analyzed_authors.assert_called_once_with(limit=50)


class TestInvalidateAuthor:
    def test_delegates_to_populate(self):
        bq = mock.MagicMock()
        writer = mock.MagicMock()
        svc = CacheService(bq=bq, writer=writer)

        bq.get_author_freshness.return_value = (True, datetime.now(timezone.utc))
        bq.get_author_stats.return_value = {"name": "Test"}
        bq.get_author_pub_stats.return_value = []
        bq.get_author_temporal_stats.return_value = []
        writer.write.return_value = True
        writer.write_batch.return_value = 1

        result = svc.dispatch("invalidate_author", {"scholar_id": "abc123"})

        assert result["status"] == "ok"
        bq.get_author_freshness.assert_called_once_with("abc123")


class TestRebuildAll:
    def test_no_cache_layer_url(self):
        svc = _make_service()
        svc.bq.get_all_author_ids.return_value = ["a1", "a2"]

        with mock.patch.object(Config, "CACHE_LAYER_URL", ""):
            result = svc.dispatch("rebuild_all", {})

        assert result["status"] == "error"
        assert "CACHE_LAYER_URL" in result["message"]

    def test_no_authors(self):
        svc = _make_service()
        svc.bq.get_all_author_ids.return_value = []

        result = svc.dispatch("rebuild_all", {})
        assert result["status"] == "error"

    @mock.patch("cache_layer.cache_service._get_tasks_client")
    def test_enqueues_tasks(self, mock_get_client):
        svc = _make_service()
        svc.bq.get_all_author_ids.return_value = ["a1", "a2", "a3"]

        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client

        with mock.patch.object(Config, "CACHE_LAYER_URL", "https://cache-layer.example.com"):
            result = svc.dispatch("rebuild_all", {})

        assert result["status"] == "ok"
        assert result["total_authors"] == 3
        assert result["enqueued"] == 3
        # 3 author tasks + 1 recent authors task
        assert mock_client.create_task.call_count == 4
