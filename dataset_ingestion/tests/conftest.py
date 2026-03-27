"""Shared test fixtures for dataset_ingestion tests."""

import pytest


@pytest.fixture
def mock_s2_api_key(monkeypatch):
    """Bypass Secret Manager for tests."""
    import dataset_ingestion.s2_api_client as client
    monkeypatch.setattr(client, "_api_key", "test-api-key")
