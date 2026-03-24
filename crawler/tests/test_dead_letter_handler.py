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

    def test_author_task_logged(self):
        envelope = _pubsub_envelope({"scholar_id": "abc123", "priority": True})
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["task_type"] == "fetch_author"
        assert data["identifier"] == "abc123"

    def test_publication_task_logged(self):
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

    def test_unknown_task_type(self):
        envelope = _pubsub_envelope({"something": "else"})
        req = _make_request(json_data=envelope)
        body, status = handle(req)

        assert status == 200
        data = json.loads(body)
        assert data["task_type"] == "unknown"

    def test_malformed_data_handled(self):
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
