"""Tests for FirestoreCache."""

from unittest import mock

from frontend.cache import FirestoreCache


def _make_cache():
    """Create a FirestoreCache with a mocked Firestore client."""
    client = mock.MagicMock()
    return FirestoreCache(client=client), client


class TestGet:
    def test_returns_data_when_exists(self):
        cache, client = _make_cache()
        doc_mock = mock.MagicMock()
        doc_mock.exists = True
        doc_mock.to_dict.return_value = {"data": {"key": "value"}}
        client.collection.return_value.document.return_value.get.return_value = doc_mock

        result = cache.get("collection", "doc_id")
        assert result == {"key": "value"}

    def test_returns_none_when_missing(self):
        cache, client = _make_cache()
        doc_mock = mock.MagicMock()
        doc_mock.exists = False
        client.collection.return_value.document.return_value.get.return_value = doc_mock

        result = cache.get("collection", "doc_id")
        assert result is None

    def test_returns_none_on_exception(self):
        cache, client = _make_cache()
        client.collection.return_value.document.return_value.get.side_effect = Exception("boom")

        result = cache.get("collection", "doc_id")
        assert result is None


class TestDelete:
    def test_delete_returns_true(self):
        cache, client = _make_cache()
        assert cache.delete("collection", "doc_id") is True
        client.collection.return_value.document.return_value.delete.assert_called_once()

    def test_delete_returns_false_on_exception(self):
        cache, client = _make_cache()
        client.collection.return_value.document.return_value.delete.side_effect = Exception("boom")
        assert cache.delete("collection", "doc_id") is False
