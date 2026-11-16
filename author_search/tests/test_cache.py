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


class TestLiveSearchAllowance:
    """reserve_live_search counts paid lookups per UTC day, in a transaction."""

    @pytest.fixture(autouse=True)
    def _passthrough_transactional(self):
        # The real decorator needs a real Transaction; run the wrapped function directly.
        with mock.patch("author_search.cache.firestore.transactional", side_effect=lambda fn: fn):
            yield

    def _cache(self, used=None):
        db = mock.MagicMock()
        doc = db.collection.return_value.document.return_value
        doc.get.return_value = mock.MagicMock(exists=used is not None,
                                              to_dict=lambda: {"count": used})
        return SearchCache(client=db), db, doc

    def test_first_lookup_of_the_day(self):
        cache, db, doc = self._cache()
        assert cache.reserve_live_search(25) is True
        db.collection.assert_called_with("v3_gs_live_search_usage")
        assert len(db.collection.return_value.document.call_args[0][0]) == 10  # YYYY-MM-DD
        transaction = db.transaction.return_value
        written = transaction.set.call_args
        assert written.args[0] is doc and written.args[1]["count"] == 1
        assert written.kwargs == {"merge": True}

    def test_check_and_increment_share_one_transaction(self):
        """Two requests reading 24 of 25 must not both proceed: the read is transactional."""
        cache, db, doc = self._cache(used=24)
        assert cache.reserve_live_search(25) is True
        transaction = db.transaction.return_value
        doc.get.assert_called_once_with(transaction=transaction)
        assert transaction.set.call_args.args[1]["count"] == 25
        doc.set.assert_not_called()

    def test_at_the_limit(self):
        cache, db, doc = self._cache(used=25)
        assert cache.reserve_live_search(25) is False
        db.transaction.return_value.set.assert_not_called()

    def test_unreadable_counter_means_no(self):
        cache, db, doc = self._cache()
        doc.get.side_effect = RuntimeError("firestore down")
        assert cache.reserve_live_search(25) is False

    def test_failed_commit_means_no(self):
        """A transaction that cannot commit (contention, outage) grants nothing."""
        cache, db, doc = self._cache(used=3)
        with mock.patch("author_search.cache.firestore.transactional",
                        side_effect=lambda fn: mock.MagicMock(side_effect=RuntimeError("aborted"))):
            assert cache.reserve_live_search(25) is False


class TestCacheTtl:
    def test_longer_ttl_keeps_older_entries(self):
        from datetime import datetime, timedelta, timezone
        from author_search.cache import SearchCache
        db = mock.MagicMock()
        doc = db.collection.return_value.document.return_value.get.return_value
        doc.exists = True
        doc.to_dict.return_value = {"timestamp": datetime.now(timezone.utc) - timedelta(hours=48),
                                    "data": [{"scholar_id": "x"}]}
        cache = SearchCache(client=db)
        assert cache.get("gs:someone") is None
        assert cache.get("gs:someone", ttl_hours=168) == [{"scholar_id": "x"}]
