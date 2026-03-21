"""Tests for FirestoreCache, including recent-author tracking."""

from unittest import mock

from frontend.cache import FirestoreCache, MAX_RECENT_AUTHORS


def _make_cache():
    """Create a FirestoreCache with a mocked Firestore client."""
    client = mock.MagicMock()
    return FirestoreCache(client=client), client


def _author(scholar_id, name="Author"):
    return {
        "scholar_id": scholar_id,
        "name": name,
        "affiliation": "Univ",
        "hindex": 10,
        "citedby": 100,
        "pip_auc_score": 0.5,
        "pip_auc_score_percentile": 0.75,
    }


class TestRecordRecentAuthor:
    def test_adds_author_to_empty_list(self):
        cache, client = _make_cache()

        # Simulate empty recent list
        doc_mock = mock.MagicMock()
        doc_mock.exists = False
        client.collection.return_value.document.return_value.get.return_value = doc_mock

        cache.record_recent_author(_author("a1", "Alice"))

        # Verify set was called with list containing the author
        client.collection.return_value.document.return_value.set.assert_called_once()
        written = client.collection.return_value.document.return_value.set.call_args[0][0]
        assert len(written["data"]) == 1
        assert written["data"][0]["scholar_id"] == "a1"

    def test_moves_existing_author_to_front(self):
        cache, client = _make_cache()

        existing = [_author("a2", "Bob"), _author("a1", "Alice")]
        doc_mock = mock.MagicMock()
        doc_mock.exists = True
        doc_mock.to_dict.return_value = {"data": existing}
        client.collection.return_value.document.return_value.get.return_value = doc_mock

        cache.record_recent_author(_author("a1", "Alice Updated"))

        written = client.collection.return_value.document.return_value.set.call_args[0][0]
        assert written["data"][0]["scholar_id"] == "a1"
        assert written["data"][0]["name"] == "Alice Updated"
        assert written["data"][1]["scholar_id"] == "a2"
        assert len(written["data"]) == 2

    def test_truncates_to_max(self):
        cache, client = _make_cache()

        existing = [_author(f"a{i}") for i in range(MAX_RECENT_AUTHORS)]
        doc_mock = mock.MagicMock()
        doc_mock.exists = True
        doc_mock.to_dict.return_value = {"data": existing}
        client.collection.return_value.document.return_value.get.return_value = doc_mock

        cache.record_recent_author(_author("new", "New Author"))

        written = client.collection.return_value.document.return_value.set.call_args[0][0]
        assert len(written["data"]) == MAX_RECENT_AUTHORS
        assert written["data"][0]["scholar_id"] == "new"

    def test_skips_if_no_scholar_id(self):
        cache, client = _make_cache()
        cache.record_recent_author({"name": "No ID"})
        client.collection.return_value.document.return_value.set.assert_not_called()

    def test_handles_exception_gracefully(self):
        cache, client = _make_cache()
        client.collection.return_value.document.return_value.get.side_effect = Exception("boom")
        # Should not raise
        cache.record_recent_author(_author("a1"))
