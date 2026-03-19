"""Shared test fixtures — mock GCP clients before any ingestion imports."""

import sys
from unittest import mock

# google.cloud.bigquery and google.cloud.storage may not be fully functional
# in the test environment (cryptography/pyo3 issues). Mock them if needed.
for mod_name in [
    "google.cloud.bigquery",
    "google.cloud.storage",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = mock.MagicMock()

# Also mock functions_framework
if "functions_framework" not in sys.modules:
    ff_mock = mock.MagicMock()
    # Make @functions_framework.http a no-op decorator
    ff_mock.http = lambda f: f
    sys.modules["functions_framework"] = ff_mock
