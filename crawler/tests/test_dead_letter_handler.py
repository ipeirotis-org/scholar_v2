"""Tests for dead_letter_handler Cloud Function."""

import base64
import json
from unittest import mock

from crawler.dead_letter_handler import v3_dead_letter_handler as handle


def _make_request(json_data=None):
    req = mock.MagicMock()
    req.get_json.return_value = json_data
    return req


def _pubsub_envelope(task_body):
    """Create a Pub/Sub push envelope with the given task body."""
    data_b64 = base64.b64encode(json.dumps(task_body).encode()).decode()
    return {
        "message": {
            "data": data_b64,
            "attributes": {"queue": "process-authors-priority"},
        },
        "subscription": "projects/scholar-version2/subscriptions/crawler-task-deadletter-push",
    }


class TestDeadLetterHandler:
    def test_no_envelope_returns_400(self):
        req = _make_request(json_data=None)
        body, status = handle(req)
        assert status == 400

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_author_task_logged(self, mock_record):
        envelope = _pubsub_envelope({"scholar_id": "abc123", "priority": True})
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["task_type"] == "fetch_author"
        assert data["identifier"] == "abc123"

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_publication_task_logged(self, mock_record):
        envelope = _pubsub_envelope({
            "pub": {"author_pub_id": "abc:pub1"},
            "priority": True,
        })
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["task_type"] == "fetch_publication"
        assert data["identifier"] == "abc:pub1"

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_unknown_task_type(self, mock_record):
        envelope = _pubsub_envelope({"something": "else"})
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["task_type"] == "unknown"

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_malformed_data_handled(self, mock_record):
        envelope = {
            "message": {
                "data": base64.b64encode(b"not json").decode(),
            },
        }
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["task_type"] == "unknown"

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_unknown_task_uses_message_id_fallback(self, mock_record):
        """When task can't be classified, use Pub/Sub message ID as identifier."""
        envelope = {
            "message": {
                "data": base64.b64encode(json.dumps({"something": "else"}).encode()).decode(),
                "messageId": "12345678",
                "attributes": {},
            },
            "subscription": "projects/scholar-version2/subscriptions/test-sub",
        }
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 200
        mock_record.assert_called_once()
        assert mock_record.call_args[1]["identifier"] == "msg_12345678"

    # ── Firestore integration tests ──

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_record_failure_called_for_author_task(self, mock_record):
        envelope = _pubsub_envelope({"scholar_id": "abc123", "priority": True})
        req = _make_request(json_data=envelope)
        handle(req)

        mock_record.assert_called_once_with(
            task_type="fetch_author",
            identifier="abc123",
            priority=True,
            source_subscription="projects/scholar-version2/subscriptions/crawler-task-deadletter-push",
            scholar_id="abc123",
            author_pub_id="",
            attributes={"queue": "process-authors-priority"},
        )

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_record_failure_called_for_publication_task(self, mock_record):
        envelope = _pubsub_envelope({
            "pub": {"author_pub_id": "abc:pub1"},
            "priority": False,
        })
        req = _make_request(json_data=envelope)
        handle(req)

        mock_record.assert_called_once_with(
            task_type="fetch_publication",
            identifier="abc:pub1",
            priority=False,
            source_subscription="projects/scholar-version2/subscriptions/crawler-task-deadletter-push",
            scholar_id="",
            author_pub_id="abc:pub1",
            attributes={"queue": "process-authors-priority"},
        )

    # ── Cache task type tests ──

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_cache_populate_author_profile_task(self, mock_record):
        envelope = _pubsub_envelope({
            "type": "populate_author_profile",
            "scholar_id": "xyz789",
        })
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["task_type"] == "populate_author_profile"
        assert data["identifier"] == "xyz789"

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_cache_invalidate_author_task(self, mock_record):
        envelope = _pubsub_envelope({
            "type": "invalidate_author",
            "scholar_id": "xyz789",
        })
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["task_type"] == "invalidate_author"

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_cache_warm_author_task(self, mock_record):
        envelope = _pubsub_envelope({
            "type": "warm_author",
            "scholar_id": "xyz789",
        })
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["task_type"] == "warm_author"

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_cache_populate_publication_detail_task(self, mock_record):
        envelope = _pubsub_envelope({
            "type": "populate_publication_detail",
            "author_pub_id": "abc:pub1",
        })
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["task_type"] == "populate_publication_detail"
        assert data["identifier"] == "abc:pub1"

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_firestore_error_returns_500(self, mock_record):
        """When Firestore write fails, return 500 so Pub/Sub retries delivery."""
        mock_record.side_effect = Exception("Firestore unavailable")
        envelope = _pubsub_envelope({"scholar_id": "abc123"})
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 500
        data = json.loads(body)
        assert data["error"] == "Firestore write failed"

    @mock.patch("crawler.dead_letter_handler.record_failure")
    def test_cache_populate_recent_authors_task(self, mock_record):
        envelope = _pubsub_envelope({
            "type": "populate_recent_authors",
        })
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["task_type"] == "populate_recent_authors"
        # Falls back to task type as identifier
        assert data["identifier"] == "populate_recent_authors"
