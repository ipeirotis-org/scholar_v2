"""Test fixtures for v3 frontend tests."""

import sys
from unittest import mock

# Mock the entire google.cloud hierarchy before any imports
_google = mock.MagicMock()
for mod_name in [
    "google",
    "google.cloud",
    "google.cloud.bigquery",
    "google.cloud.firestore",
    "google.cloud.firestore_v1",
    "google.cloud.firestore_v1.base_query",
    "google.cloud.storage",
    "scholarly",
]:
    sys.modules.setdefault(mod_name, _google)

# Make google.cloud.bigquery.ScalarQueryParameter a real class for parameterized queries
sys.modules["google.cloud.bigquery"].ScalarQueryParameter = type(
    "ScalarQueryParameter", (), {"__init__": lambda self, *a, **kw: None}
)

import pytest

from v3.frontend.app import create_app


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
