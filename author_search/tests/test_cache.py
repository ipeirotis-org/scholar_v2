"""Tests for SearchCache."""

from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

from author_search.cache import SearchCache
from author_search.config import Config


class TestCacheGet:
    def test_returns_none_when_doc_missing(self):
        mock_db = mock.MagicMock()
        mock_doc = mock.MagicMock()
        mock_doc.exists = False
        mock_db.collection.return_value.document.return_value.get.return_value = mock_doc

        cache = SearchCache(client=mock_db)
        assert cache.get("alice") is None

    def test_returns_data_when_fresh(self):
        mock_db = mock.MagicMock()
        mock_doc = mock.MagicMock()
        mock_doc.exists = True
        mock_doc.to_dict.return_value = {
            "timestamp": datetime.now(timezone.utc),
            "data": [{"scholar_id": "abc", "name": "Alice"}],
        }
        mock_db.collection.return_value.document.return_value.get.return_value = mock_doc

        cache = SearchCache(client=mock_db)
        result = cache.get("alice")
        assert result == [{"scholar_id": "abc", "name": "Alice"}]

    def test_returns_none_when_stale(self):
        mock_db = mock.MagicMock()
        mock_doc = mock.MagicMock()
        mock_doc.exists = True
        mock_doc.to_dict.return_value = {
            "timestamp": datetime.now(timezone.utc) - timedelta(hours=Config.CACHE_TTL_HOURS + 1),
            "data": [{"scholar_id": "abc"}],
        }
        mock_db.collection.return_value.document.return_value.get.return_value = mock_doc

        cache = SearchCache(client=mock_db)
        assert cache.get("alice") is None

    def test_returns_none_on_exception(self):
        mock_db = mock.MagicMock()
        mock_db.collection.side_effect = Exception("Firestore down")

        cache = SearchCache(client=mock_db)
        assert cache.get("alice") is None


class TestCacheSet:
    def test_writes_with_timestamp(self):
        mock_db = mock.MagicMock()
        cache = SearchCache(client=mock_db)
        cache.set("alice", [{"scholar_id": "abc"}])

        mock_db.collection.return_value.document.return_value.set.assert_called_once()
        call_data = mock_db.collection.return_value.document.return_value.set.call_args[0][0]
        assert "timestamp" in call_data
        assert call_data["data"] == [{"scholar_id": "abc"}]
        assert call_data["query"] == "alice"


class TestSafeDocId:
    def test_lowercases_and_strips(self):
        assert SearchCache._safe_doc_id("  Alice Smith  ") == "alice smith"

    def test_replaces_slashes(self):
        assert SearchCache._safe_doc_id("a/b/c") == "a_b_c"

    def test_hashes_long_strings(self):
        long_name = "x" * 2000
        doc_id = SearchCache._safe_doc_id(long_name)
        assert len(doc_id) == 64  # SHA-256 hex digest
