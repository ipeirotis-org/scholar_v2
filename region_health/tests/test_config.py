"""Tests for region_health.config."""

from region_health.config import (
    AVAILABLE_FUNCTION_REGIONS,
    CLOUD_FUNCTION_NAMES,
    DEFAULT_WEIGHT,
    MIN_WEIGHT,
)


def test_region_count():
    assert len(AVAILABLE_FUNCTION_REGIONS) == 15


def test_original_us_regions_present():
    original = [
        "us-central1", "us-east1", "us-east4", "us-east5",
        "us-south1", "us-west1", "us-west2", "us-west3", "us-west4",
    ]
    for r in original:
        assert r in AVAILABLE_FUNCTION_REGIONS


def test_new_regions_present():
    new_regions = [
        "northamerica-northeast1", "northamerica-northeast2",
        "europe-west1", "europe-west2",
        "asia-east1", "asia-northeast1",
    ]
    for r in new_regions:
        assert r in AVAILABLE_FUNCTION_REGIONS


def test_no_duplicates():
    assert len(AVAILABLE_FUNCTION_REGIONS) == len(set(AVAILABLE_FUNCTION_REGIONS))


def test_cloud_function_names():
    assert "v3_fetch_author" in CLOUD_FUNCTION_NAMES
    assert "v3_fetch_publication" in CLOUD_FUNCTION_NAMES


def test_weight_bounds():
    assert 0 < MIN_WEIGHT < DEFAULT_WEIGHT <= 1.0
