"""Tests for fetch_publication Cloud Function entry point."""

import json
from unittest import mock

from crawler.fetch_publication import v3_fetch_publication as handle
from crawler.scholarly_client import ErrorKind, ScholarlyError


def _make_request(json_data=None):
    req = mock.MagicMock()
    req.get_json.return_value = json_data
    req.headers = {"Function-Execution-Id": "test-456"}
    return req


class TestHandleFetchPublication:
    def test_missing_pub(self):
        req = _make_request(json_data={})
        body, status = handle(req)
        assert status == 400

    def test_missing_author_pub_id(self):
        req = _make_request(json_data={"pub": {"no_id": True}})
        body, status = handle(req)
        assert status == 400

    @mock.patch("crawler.fetch_publication.upload_json")
    @mock.patch("crawler.fetch_publication.publication_blob_path", return_value="pubs/test.json")
    @mock.patch("crawler.fetch_publication._fetch_publication")
    def test_success(self, mock_fetch, mock_path, mock_upload):
        mock_fetch.return_value = {"author_pub_id": "abc:pub1", "num_citations": 42}

        req = _make_request(json_data={"pub": {"author_pub_id": "abc:pub1"}})
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["author_pub_id"] == "abc:pub1"
        mock_upload.assert_called_once()

    @mock.patch("crawler.fetch_publication._fetch_publication")
    def test_transient_error_returns_429(self, mock_fetch):
        mock_fetch.side_effect = ScholarlyError("rate limited", ErrorKind.TRANSIENT)

        req = _make_request(json_data={"pub": {"author_pub_id": "abc:pub1"}})
        body, status = handle(req)

        assert status == 429

    @mock.patch("crawler.fetch_publication._fetch_publication")
    def test_permanent_error_returns_500(self, mock_fetch):
        mock_fetch.side_effect = ScholarlyError("not found", ErrorKind.PERMANENT)

        req = _make_request(json_data={"pub": {"author_pub_id": "abc:pub1"}})
        body, status = handle(req)

        assert status == 500
