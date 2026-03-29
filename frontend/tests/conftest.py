"""Test fixtures for v3 frontend tests."""

import sys
from unittest import mock

# Build a proper mock hierarchy for google.cloud so submodule imports work.
# Each level needs to be a MagicMock so `from google.cloud import X` resolves.
_google_mock = mock.MagicMock()
_google_cloud_mock = mock.MagicMock()
_google_mock.cloud = _google_cloud_mock

for mod_name, mod_mock in [
    ("google", _google_mock),
    ("google.cloud", _google_cloud_mock),
]:
    sys.modules.setdefault(mod_name, mod_mock)

# Mock individual GCP packages
for mod_name in [
    "google.cloud.bigquery",
    "google.cloud.firestore",
    "google.cloud.firestore_v1",
    "google.cloud.firestore_v1.base_query",
    "google.cloud.storage",
    "google.cloud.tasks_v2",
    "google.cloud.monitoring_v3",
    "google.cloud.logging",
    "google.protobuf",
    "google.protobuf.duration_pb2",
    "google.api_core",
    "google.api_core.exceptions",
    "google.cloud.secretmanager",
]:
    sys.modules.setdefault(mod_name, mock.MagicMock())

# Wire up google.protobuf to the google namespace mock
_google_mock.protobuf = sys.modules["google.protobuf"]

# Wire up google.cloud.monitoring_v3 to the google.cloud namespace mock
_google_cloud_mock.monitoring_v3 = sys.modules["google.cloud.monitoring_v3"]

# Wire up google.cloud.logging to the google.cloud namespace mock
_google_cloud_mock.logging = sys.modules["google.cloud.logging"]

# Make ScalarQueryParameter a real class (used by author_search)
sys.modules["google.cloud.bigquery"].ScalarQueryParameter = type(
    "ScalarQueryParameter", (), {"__init__": lambda self, *a, **kw: None}
)

# Make HttpMethod an object with POST attribute
sys.modules["google.cloud.tasks_v2"].HttpMethod = mock.MagicMock()
sys.modules["google.cloud.tasks_v2"].HttpMethod.POST = 1

# Make AlreadyExists a real exception class so except clauses work
class _AlreadyExists(Exception):
    pass

sys.modules["google.api_core.exceptions"].AlreadyExists = _AlreadyExists

import pytest

from frontend.app import create_app


@pytest.fixture
def app():
    """Create a test Flask application."""
    test_config = {
        "TESTING": True,
        "SECRET_KEY": "test-secret-key",
    }
    app = create_app(config=test_config)
    return app


@pytest.fixture
def client(app):
    """Create a test client."""
    return app.test_client()
