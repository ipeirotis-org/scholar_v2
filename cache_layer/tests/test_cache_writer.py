"""Tests for CacheWriter."""

from unittest import mock

from cache_layer.cache_writer import CacheWriter


def _make_writer():
    mock_db = mock.MagicMock()
    return CacheWriter(client=mock_db), mock_db


class TestWrite:
    def test_writes_with_timestamp(self):
        writer, db = _make_writer()
        result = writer.write("v3_author_stats", "abc123", {"name": "Test"})
        assert result is True
        db.collection.assert_called_once_with("v3_author_stats")
        db.collection().document.assert_called_once_with("abc123")
        call_args = db.collection().document().set.call_args[0][0]
        assert call_args["data"] == {"name": "Test"}
        assert "timestamp" in call_args

    def test_empty_doc_id_returns_false(self):
        writer, db = _make_writer()
        assert writer.write("col", "", {"data": 1}) is False
        assert writer.write("col", "  ", {"data": 1}) is False
        db.collection.assert_not_called()

    def test_firestore_error_returns_false(self):
        writer, db = _make_writer()
        db.collection().document().set.side_effect = Exception("Firestore down")
        result = writer.write("col", "doc", {"data": 1})
        assert result is False


class TestWriteBatch:
    def test_empty_writes(self):
        writer, _ = _make_writer()
        assert writer.write_batch([]) == 0

    def test_batch_writes(self):
        writer, db = _make_writer()
        writes = [
            ("col1", "doc1", {"a": 1}),
            ("col2", "doc2", {"b": 2}),
            ("col3", "doc3", {"c": 3}),
        ]
        result = writer.write_batch(writes)
        assert result == 3
        db.batch().commit.assert_called_once()

    def test_skips_empty_doc_ids(self):
        writer, db = _make_writer()
        writes = [
            ("col1", "", {"a": 1}),
            ("col2", "doc2", {"b": 2}),
        ]
        result = writer.write_batch(writes)
        assert result == 1

    def test_batch_commits_at_500(self):
        writer, db = _make_writer()
        writes = [("col", f"doc{i}", {"i": i}) for i in range(501)]
        result = writer.write_batch(writes)
        assert result == 501
        # Should commit twice: once at 500, once for the final 1
        assert db.batch().commit.call_count == 2
