"""Shared test fixtures — mock scholarly before any crawler imports."""

import sys
from unittest import mock

# scholarly is not installed in the test environment, so mock it globally
if "scholarly" not in sys.modules:
    sys.modules["scholarly"] = mock.MagicMock()
