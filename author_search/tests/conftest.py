"""Test fixtures for v3 author_search tests."""

import sys
from unittest import mock

# Mock external dependencies before any imports
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
    "scholarly.scholarly",
]:
    sys.modules.setdefault(mod_name, _google)

# Make google.cloud.bigquery.ScalarQueryParameter a real class for parameterized queries
sys.modules["google.cloud.bigquery"].ScalarQueryParameter = type(
    "ScalarQueryParameter", (), {"__init__": lambda self, *a, **kw: None}
)
