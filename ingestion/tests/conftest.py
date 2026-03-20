"""Shared test fixtures — mock GCP clients before any ingestion imports."""

import sys
from unittest import mock

# Ensure parent modules exist
for parent in ["google", "google.cloud", "google.api_core"]:
    if parent not in sys.modules:
        sys.modules[parent] = mock.MagicMock()

# google.cloud.bigquery, google.cloud.storage, and google.cloud.tasks_v2
# may not be fully functional in the test environment. Mock them if needed.
for mod_name in [
    "google.cloud.bigquery",
    "google.cloud.storage",
    "google.cloud.tasks_v2",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = mock.MagicMock()

# Make HttpMethod an object with POST attribute
sys.modules["google.cloud.tasks_v2"].HttpMethod = mock.MagicMock()
sys.modules["google.cloud.tasks_v2"].HttpMethod.POST = 1

# Also mock functions_framework
if "functions_framework" not in sys.modules:
    ff_mock = mock.MagicMock()
    # Make @functions_framework.http a no-op decorator
    ff_mock.http = lambda f: f
    sys.modules["functions_framework"] = ff_mock
