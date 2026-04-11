"""Tests for FirestoreCache, including recent-author tracking."""

from unittest import mock

import pytest
from google.api_core.exceptions import ServiceUnavailable

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
    """Tests for the transactional recent-author tracker.

    `record_recent_author` uses `@firestore.transactional`, which requires
    a real Transaction instance to run the retry loop. We patch the
    decorator with a passthrough so the wrapped function runs directly
    against the mocked transaction, then assert that reads and writes go
    through the transaction (not the plain document reference).
    """

    @pytest.fixture(autouse=True)
    def _passthrough_transactional(self):
        with mock.patch(
            "frontend.cache.firestore.transactional",
            side_effect=lambda fn: fn,
        ):
            yield

    @staticmethod
    def _setup_transaction(client, existing=None):
        """Wire client mocks so the transactional reader sees `existing`.

        Returns (transaction_mock, doc_ref_mock) for assertions.
        """
        doc_ref = client.collection.return_value.document.return_value
        snapshot = mock.MagicMock()
        if existing is None:
            snapshot.exists = False
        else:
            snapshot.exists = True
            snapshot.to_dict.return_value = {"data": existing}
        doc_ref.get.return_value = snapshot

        transaction = mock.MagicMock(name="transaction")
        client.transaction.return_value = transaction
        return transaction, doc_ref

    def test_adds_author_to_empty_list(self):
        cache, client = _make_cache()
        transaction, doc_ref = self._setup_transaction(client, existing=None)

        cache.record_recent_author(_author("a1", "Alice"))

        # Read must go through the transaction (transactional read).
        doc_ref.get.assert_called_once_with(transaction=transaction)
        # Write must go through the transaction, not doc_ref.set directly.
        doc_ref.set.assert_not_called()
        transaction.set.assert_called_once()
        written_ref, written_payload = transaction.set.call_args[0]
        assert written_ref is doc_ref
        assert len(written_payload["data"]) == 1
        assert written_payload["data"][0]["scholar_id"] == "a1"
        assert "timestamp" in written_payload

    def test_moves_existing_author_to_front(self):
        cache, client = _make_cache()
        existing = [_author("a2", "Bob"), _author("a1", "Alice")]
        transaction, _ = self._setup_transaction(client, existing=existing)

        cache.record_recent_author(_author("a1", "Alice Updated"))

        written = transaction.set.call_args[0][1]
        assert written["data"][0]["scholar_id"] == "a1"
        assert written["data"][0]["name"] == "Alice Updated"
        assert written["data"][1]["scholar_id"] == "a2"
        assert len(written["data"]) == 2

    def test_truncates_to_max(self):
        cache, client = _make_cache()
        existing = [_author(f"a{i}") for i in range(MAX_RECENT_AUTHORS)]
        transaction, _ = self._setup_transaction(client, existing=existing)

        cache.record_recent_author(_author("new", "New Author"))

        written = transaction.set.call_args[0][1]
        assert len(written["data"]) == MAX_RECENT_AUTHORS
        assert written["data"][0]["scholar_id"] == "new"

    def test_skips_if_no_scholar_id(self):
        cache, client = _make_cache()
        self._setup_transaction(client, existing=None)

        cache.record_recent_author({"name": "No ID"})

        # No transaction opened, no read, no write.
        client.transaction.assert_not_called()
        client.collection.return_value.document.return_value.set.assert_not_called()

    def test_ignores_non_list_existing_data(self):
        cache, client = _make_cache()
        doc_ref = client.collection.return_value.document.return_value
        snapshot = mock.MagicMock()
        snapshot.exists = True
        snapshot.to_dict.return_value = {"data": "corrupted"}
        doc_ref.get.return_value = snapshot
        transaction = mock.MagicMock(name="transaction")
        client.transaction.return_value = transaction

        cache.record_recent_author(_author("a1", "Alice"))

        written = transaction.set.call_args[0][1]
        assert len(written["data"]) == 1
        assert written["data"][0]["scholar_id"] == "a1"

    def test_handles_exception_gracefully(self):
        cache, client = _make_cache()
        transaction = mock.MagicMock(name="transaction")
        client.transaction.return_value = transaction
        doc_ref = client.collection.return_value.document.return_value
        doc_ref.get.side_effect = ServiceUnavailable("boom")

        # Should not raise
        cache.record_recent_author(_author("a1"))

    def test_opens_a_new_transaction_per_call(self):
        cache, client = _make_cache()
        self._setup_transaction(client, existing=None)

        cache.record_recent_author(_author("a1", "Alice"))
        cache.record_recent_author(_author("a2", "Bob"))

        assert client.transaction.call_count == 2
