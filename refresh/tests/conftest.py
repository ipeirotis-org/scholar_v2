"""Shared test fixtures — mock GCP clients before any refresh imports."""

import sys
from unittest import mock


# Create a real exception class for AlreadyExists so except clauses work
class _AlreadyExists(Exception):
    pass


# Ensure parent modules exist
for parent in ["google", "google.cloud", "google.api_core"]:
    if parent not in sys.modules:
        sys.modules[parent] = mock.MagicMock()

# Mock GCP clients
for mod_name in ["google.cloud.bigquery", "google.cloud.tasks_v2"]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = mock.MagicMock()

# Mock google.api_core.exceptions with a real AlreadyExists exception class
if "google.api_core.exceptions" not in sys.modules:
    exc_mock = mock.MagicMock()
    exc_mock.AlreadyExists = _AlreadyExists
    sys.modules["google.api_core.exceptions"] = exc_mock

# Mock flask (provide a real Flask for test client usage)
# Flask is available as a test dependency; no mock needed
