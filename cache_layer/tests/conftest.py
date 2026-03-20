"""Test fixtures for cache_layer tests."""

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
    "google.cloud.tasks_v2",
    "google.api_core",
    "google.api_core.exceptions",
]:
    sys.modules.setdefault(mod_name, _google)

# Make ScalarQueryParameter a real class for parameterized queries
sys.modules["google.cloud.bigquery"].ScalarQueryParameter = type(
    "ScalarQueryParameter", (), {"__init__": lambda self, *a, **kw: None}
)

# Make AlreadyExists a real exception class
sys.modules["google.api_core.exceptions"].AlreadyExists = type(
    "AlreadyExists", (Exception,), {}
)

# Make HttpMethod an object with POST attribute
sys.modules["google.cloud.tasks_v2"].HttpMethod = mock.MagicMock()
sys.modules["google.cloud.tasks_v2"].HttpMethod.POST = 1

import pytest

from cache_layer.main import app as flask_app


@pytest.fixture
def app():
    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()
