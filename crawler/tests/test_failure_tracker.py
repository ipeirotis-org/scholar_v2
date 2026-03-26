"""Tests for failure_tracker module."""

from unittest import mock

from crawler.failure_tracker import (
    _doc_id,
    _sanitize_doc_id,
    record_failure,
    record_partial_enqueue_failure,
    resolve_failure,
)


class TestDocIdHelpers:
    def test_sanitize_doc_id_colons(self):
        assert _sanitize_doc_id("abc:def") == "abc__def"

    def test_sanitize_doc_id_slashes(self):
        assert _sanitize_doc_id("abc/def") == "abc___def"

    def test_sanitize_doc_id_mixed(self):
        assert _sanitize_doc_id("a:b/c") == "a__b___c"

    def test_doc_id_format(self):
        assert _doc_id("fetch_author", "abc123") == "fetch_author_abc123"

    def test_doc_id_with_special_chars(self):
        assert _doc_id("fetch_publication", "abc:pub1") == "fetch_publication_abc__pub1"


class TestRecordFailure:
    @mock.patch("crawler.failure_tracker._get_db")
    def test_creates_new_failure_record(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db
        mock_doc_ref = mock.MagicMock()
        mock_db.collection.return_value.document.return_value = mock_doc_ref
        mock_doc = mock.MagicMock()
        mock_doc.exists = False
        mock_doc_ref.get.return_value = mock_doc

        record_failure(
            task_type="fetch_author",
            identifier="abc123",
            priority=True,
            source_subscription="test-sub",
            scholar_id="abc123",
        )

        mock_doc_ref.set.assert_called_once()
        data = mock_doc_ref.set.call_args[0][0]
        assert data["task_type"] == "fetch_author"
        assert data["identifier"] == "abc123"
        assert data["scholar_id"] == "abc123"
        assert data["priority"] is True
        assert data["failure_count"] == 1
        assert data["status"] == "failed"
        assert data["resolved_at"] is None

    @mock.patch("crawler.failure_tracker._get_db")
    def test_increments_existing_failure(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db
        mock_doc_ref = mock.MagicMock()
        mock_db.collection.return_value.document.return_value = mock_doc_ref
        mock_doc = mock.MagicMock()
        mock_doc.exists = True
        mock_doc_ref.get.return_value = mock_doc

        record_failure(
            task_type="fetch_author",
            identifier="abc123",
        )

        mock_doc_ref.update.assert_called_once()
        update_data = mock_doc_ref.update.call_args[0][0]
        assert update_data["status"] == "failed"
        # Increment sentinel used for failure_count
        assert "failure_count" in update_data

    @mock.patch("crawler.failure_tracker._get_db")
    def test_empty_identifier_skipped(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db

        record_failure(task_type="unknown", identifier="")

        mock_db.collection.assert_not_called()

    @mock.patch("crawler.failure_tracker._get_db")
    def test_firestore_error_logged_not_raised(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db
        mock_db.collection.side_effect = Exception("Firestore unavailable")

        # Should not raise
        record_failure(task_type="fetch_author", identifier="abc123")

    @mock.patch("crawler.failure_tracker._get_db")
    def test_correct_collection_and_doc_id(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db
        mock_doc_ref = mock.MagicMock()
        mock_doc = mock.MagicMock()
        mock_doc.exists = False
        mock_doc_ref.get.return_value = mock_doc
        mock_db.collection.return_value.document.return_value = mock_doc_ref

        record_failure(
            task_type="fetch_publication",
            identifier="abc:pub1",
            scholar_id="",
            author_pub_id="abc:pub1",
        )

        mock_db.collection.assert_called_with("task_failures")
        mock_db.collection.return_value.document.assert_called_with(
            "fetch_publication_abc__pub1"
        )


class TestResolveFailure:
    @mock.patch("crawler.failure_tracker._get_db")
    def test_resolves_existing_failed_record(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db
        mock_doc_ref = mock.MagicMock()
        mock_db.collection.return_value.document.return_value = mock_doc_ref
        mock_doc = mock.MagicMock()
        mock_doc.exists = True
        mock_doc.to_dict.return_value = {"status": "failed"}
        mock_doc_ref.get.return_value = mock_doc

        resolve_failure("fetch_author", "abc123")

        mock_doc_ref.update.assert_called_once()
        update_data = mock_doc_ref.update.call_args[0][0]
        assert update_data["status"] == "resolved"
        assert "resolved_at" in update_data

    @mock.patch("crawler.failure_tracker._get_db")
    def test_skips_already_resolved(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db
        mock_doc_ref = mock.MagicMock()
        mock_db.collection.return_value.document.return_value = mock_doc_ref
        mock_doc = mock.MagicMock()
        mock_doc.exists = True
        mock_doc.to_dict.return_value = {"status": "resolved"}
        mock_doc_ref.get.return_value = mock_doc

        resolve_failure("fetch_author", "abc123")

        mock_doc_ref.update.assert_not_called()

    @mock.patch("crawler.failure_tracker._get_db")
    def test_skips_nonexistent_record(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db
        mock_doc_ref = mock.MagicMock()
        mock_db.collection.return_value.document.return_value = mock_doc_ref
        mock_doc = mock.MagicMock()
        mock_doc.exists = False
        mock_doc_ref.get.return_value = mock_doc

        resolve_failure("fetch_author", "abc123")

        mock_doc_ref.update.assert_not_called()

    @mock.patch("crawler.failure_tracker._get_db")
    def test_empty_identifier_skipped(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db

        resolve_failure("fetch_author", "")

        mock_db.collection.assert_not_called()

    @mock.patch("crawler.failure_tracker._get_db")
    def test_firestore_error_logged_not_raised(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db
        mock_db.collection.side_effect = Exception("Firestore unavailable")

        # Should not raise
        resolve_failure("fetch_author", "abc123")


class TestRecordPartialEnqueueFailure:
    @mock.patch("crawler.failure_tracker._get_db")
    def test_records_failed_pub_ids(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db
        mock_doc_ref = mock.MagicMock()
        mock_db.collection.return_value.document.return_value = mock_doc_ref

        record_partial_enqueue_failure("abc123", ["abc123:pub1", "abc123:pub2"])

        mock_doc_ref.set.assert_called_once()
        data = mock_doc_ref.set.call_args[0][0]
        assert data["task_type"] == "enqueue_publications"
        assert data["scholar_id"] == "abc123"
        assert data["failed_pub_ids"] == ["abc123:pub1", "abc123:pub2"]
        assert data["failure_count"] == 2
        assert data["status"] == "failed"

    @mock.patch("crawler.failure_tracker._get_db")
    def test_empty_list_skipped(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db

        record_partial_enqueue_failure("abc123", [])

        mock_db.collection.assert_not_called()

    @mock.patch("crawler.failure_tracker._get_db")
    def test_firestore_error_logged_not_raised(self, mock_get_db):
        mock_db = mock.MagicMock()
        mock_get_db.return_value = mock_db
        mock_db.collection.side_effect = Exception("Firestore unavailable")

        # Should not raise
        record_partial_enqueue_failure("abc123", ["abc123:pub1"])
