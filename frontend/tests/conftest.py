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
    "google.api_core",
    "google.api_core.exceptions",
    "scholarly",
    "matplotlib",
    "matplotlib.pyplot",
    "matplotlib.ticker",
    "matplotlib.colors",
    "matplotlib.patches",
    "matplotlib.figure",
]:
    sys.modules.setdefault(mod_name, mock.MagicMock())

# Make ScalarQueryParameter a real class (used by author_search)
sys.modules["google.cloud.bigquery"].ScalarQueryParameter = type(
    "ScalarQueryParameter", (), {"__init__": lambda self, *a, **kw: None}
)

# Make HttpMethod an object with POST attribute
sys.modules["google.cloud.tasks_v2"].HttpMethod = mock.MagicMock()
sys.modules["google.cloud.tasks_v2"].HttpMethod.POST = 1

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
