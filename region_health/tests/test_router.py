"""Tests for region_health.router."""

from collections import Counter
from unittest import mock

import pytest

from region_health.config import AVAILABLE_FUNCTION_REGIONS, DEFAULT_WEIGHT
from region_health.router import (
    get_region_weights,
    get_rotating_region,
    invalidate_cache,
    select_best_region,
    select_region,
)


@pytest.fixture(autouse=True)
def _clear_cache():
    """Clear the in-memory cache before each test."""
    invalidate_cache()
    yield
    invalidate_cache()


class TestGetRotatingRegion:
    def test_returns_valid_region(self):
        region = get_rotating_region()
        assert region in AVAILABLE_FUNCTION_REGIONS

    def test_deterministic_for_same_day(self):
        r1 = get_rotating_region()
        r2 = get_rotating_region()
        assert r1 == r2

    def test_custom_list(self):
        regions = ["a", "b", "c"]
        region = get_rotating_region(regions)
        assert region in regions

    def test_single_region(self):
        assert get_rotating_region(["only-one"]) == "only-one"


class TestGetRegionWeights:
    @mock.patch("region_health.router._load_weights_from_firestore")
    def test_returns_all_regions(self, mock_load):
        mock_load.return_value = {"us-central1": 0.9}
        weights = get_region_weights()
        assert len(weights) == len(AVAILABLE_FUNCTION_REGIONS)
        assert weights["us-central1"] == 0.9
        # Regions not in Firestore get DEFAULT_WEIGHT
        assert weights["us-east1"] == DEFAULT_WEIGHT

    @mock.patch("region_health.router._load_weights_from_firestore")
    def test_uses_cache(self, mock_load):
        mock_load.return_value = {}
        get_region_weights()
        get_region_weights()
        # Should only call Firestore once (cached)
        assert mock_load.call_count == 1

    @mock.patch("region_health.router._load_weights_from_firestore")
    def test_fallback_on_firestore_failure(self, mock_load):
        mock_load.return_value = {}
        weights = get_region_weights()
        # All regions get DEFAULT_WEIGHT
        for r in AVAILABLE_FUNCTION_REGIONS:
            assert weights[r] == DEFAULT_WEIGHT


class TestSelectRegion:
    @mock.patch("region_health.router._load_weights_from_firestore")
    def test_returns_valid_region(self, mock_load):
        mock_load.return_value = {}
        region = select_region()
        assert region in AVAILABLE_FUNCTION_REGIONS

    @mock.patch("region_health.router._load_weights_from_firestore")
    def test_weighted_selection_favors_healthy_regions(self, mock_load):
        # Give one region high weight, all others minimum
        weights = {r: 0.05 for r in AVAILABLE_FUNCTION_REGIONS}
        weights["us-central1"] = 1.0
        mock_load.return_value = weights

        counts = Counter()
        for _ in range(1000):
            invalidate_cache()
            counts[select_region()] += 1

        # us-central1 should be selected significantly more often
        assert counts["us-central1"] > 200  # ~58% expected

    @mock.patch("region_health.router._load_weights_from_firestore")
    def test_all_regions_can_be_selected(self, mock_load):
        """Even low-weight regions should occasionally be selected."""
        weights = {r: 0.05 for r in AVAILABLE_FUNCTION_REGIONS}
        weights["us-central1"] = 1.0
        mock_load.return_value = weights

        selected = set()
        for _ in range(5000):
            invalidate_cache()
            selected.add(select_region())

        # All regions should be reachable
        assert len(selected) == len(AVAILABLE_FUNCTION_REGIONS)


class TestSelectBestRegion:
    @mock.patch("region_health.router._load_weights_from_firestore")
    def test_returns_highest_weight(self, mock_load):
        weights = {r: 0.5 for r in AVAILABLE_FUNCTION_REGIONS}
        weights["europe-west1"] = 0.99
        mock_load.return_value = weights

        region = select_best_region()
        assert region == "europe-west1"

    @mock.patch("region_health.router._load_weights_from_firestore")
    def test_handles_ties(self, mock_load):
        mock_load.return_value = {}
        # All default weights — should return one of the regions
        region = select_best_region()
        assert region in AVAILABLE_FUNCTION_REGIONS
