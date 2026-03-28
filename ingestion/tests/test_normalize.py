"""Tests for document ID normalization."""

from ingestion.normalize import normalize_document_id


class TestNormalizeDocumentId:
    def test_strips_json_suffix(self):
        assert normalize_document_id("abc123.json") == "abc123"

    def test_leaves_bare_id_unchanged(self):
        assert normalize_document_id("abc123") == "abc123"

    def test_strips_suffix_with_colon(self):
        assert normalize_document_id("scholar_id:pub_id.json") == "scholar_id:pub_id"

    def test_empty_string(self):
        assert normalize_document_id("") == ""

    def test_none_returns_none(self):
        assert normalize_document_id(None) is None

    def test_only_json(self):
        assert normalize_document_id(".json") == ""

    def test_json_in_middle_not_stripped(self):
        assert normalize_document_id("file.json.bak") == "file.json.bak"
