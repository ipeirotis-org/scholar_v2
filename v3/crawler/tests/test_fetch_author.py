"""Tests for fetch_author Cloud Function entry point."""

import json
from unittest import mock

from v3.crawler.fetch_author import v3_fetch_author as handle
from v3.crawler.scholarly_client import ErrorKind, ScholarlyError


def _make_request(json_data=None, args=None):
    req = mock.MagicMock()
    req.get_json.return_value = json_data
    req.args = args or {}
    req.headers = {"Function-Execution-Id": "test-123"}
    return req


class TestHandleFetchAuthor:
    def test_missing_scholar_id(self):
        req = _make_request()
        body, status = handle(req)
        assert status == 400
        assert "scholar_id" in json.loads(body)["error"]

    @mock.patch("v3.crawler.fetch_author.enqueue_publications")
    @mock.patch("v3.crawler.fetch_author.upload_json")
    @mock.patch("v3.crawler.fetch_author.author_blob_path", return_value="authors_json/2026/03/19/abc.json")
    @mock.patch("v3.crawler.fetch_author._fetch_author")
    def test_success(self, mock_fetch, mock_path, mock_upload, mock_enqueue):
        mock_fetch.return_value = {
            "scholar_id": "abc",
            "name": "Test",
            "publications": [{"author_pub_id": "abc:p1"}],
        }

        req = _make_request(json_data={"scholar_id": "abc"})
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["scholar_id"] == "abc"
        mock_upload.assert_called_once()
        mock_enqueue.assert_called_once()

    @mock.patch("v3.crawler.fetch_author.upload_json")
    @mock.patch("v3.crawler.fetch_author.author_blob_path", return_value="test.json")
    @mock.patch("v3.crawler.fetch_author._fetch_author")
    def test_skip_pubs(self, mock_fetch, mock_path, mock_upload):
        mock_fetch.return_value = {"scholar_id": "abc", "publications": []}

        req = _make_request(json_data={"scholar_id": "abc", "skip_pubs": True})
        body, status = handle(req)

        assert status == 200

    @mock.patch("v3.crawler.fetch_author._fetch_author")
    def test_transient_error_returns_429(self, mock_fetch):
        mock_fetch.side_effect = ScholarlyError("rate limited", ErrorKind.TRANSIENT)

        req = _make_request(json_data={"scholar_id": "abc"})
        body, status = handle(req)

        assert status == 429

    @mock.patch("v3.crawler.fetch_author._fetch_author")
    def test_permanent_error_returns_500(self, mock_fetch):
        mock_fetch.side_effect = ScholarlyError("not found", ErrorKind.PERMANENT)

        req = _make_request(json_data={"scholar_id": "abc"})
        body, status = handle(req)

        assert status == 500
