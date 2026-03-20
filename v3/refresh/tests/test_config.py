"""Tests for refresh config."""

import os
from unittest import mock


class TestConfigDefaults:
    def test_project_id(self):
        from v3.refresh.config import Config
        assert Config.PROJECT_ID == "scholar-version2"

    def test_bucket_name(self):
        from v3.refresh.config import Config
        assert Config.BUCKET_NAME == "scholar_data_share"

    def test_bq_dataset(self):
        from v3.refresh.config import Config
        assert Config.BQ_DATASET == "scholar_raw_data"

    def test_bq_stats_dataset(self):
        from v3.refresh.config import Config
        assert Config.BQ_STATS_DATASET == "statistics"

    def test_queue_location(self):
        from v3.refresh.config import Config
        assert Config.QUEUE_LOCATION == "northamerica-northeast1"

    def test_queue_name_authors(self):
        from v3.refresh.config import Config
        assert Config.QUEUE_NAME_AUTHORS == "process-authors"

    def test_stale_threshold_days(self):
        from v3.refresh.config import Config
        assert Config.STALE_THRESHOLD_DAYS == 90

    def test_error_cooldown_hours(self):
        from v3.refresh.config import Config
        assert Config.ERROR_COOLDOWN_HOURS == 24

    def test_coauthor_batch_size(self):
        from v3.refresh.config import Config
        assert Config.COAUTHOR_BATCH_SIZE == 1

    def test_coauthor_oversample_factor(self):
        from v3.refresh.config import Config
        assert Config.COAUTHOR_OVERSAMPLE_FACTOR == 10


class TestConfigHelpers:
    def test_queue_path_default(self):
        from v3.refresh.config import Config
        path = Config.queue_path()
        assert path == (
            "projects/scholar-version2"
            "/locations/northamerica-northeast1"
            "/queues/process-authors"
        )

    def test_queue_path_custom(self):
        from v3.refresh.config import Config
        path = Config.queue_path("my-queue")
        assert path == (
            "projects/scholar-version2"
            "/locations/northamerica-northeast1"
            "/queues/my-queue"
        )

    def test_function_url(self):
        from v3.refresh.config import Config
        url = Config.function_url("fetch_author")
        assert url.startswith("https://")
        assert "scholar-version2" in url
        assert url.endswith("/fetch_author")

    def test_bq_raw(self):
        from v3.refresh.config import Config
        ref = Config.bq_raw("author_latest")
        assert ref == "`scholar-version2.scholar_raw_data.author_latest`"

    def test_bq_view(self):
        from v3.refresh.config import Config
        ref = Config.bq_view("coauthors_to_add")
        assert ref == "`scholar-version2.statistics.coauthors_to_add`"


class TestGetRotatingRegion:
    def test_returns_valid_region(self):
        from v3.refresh.config import get_rotating_region, AVAILABLE_FUNCTION_REGIONS
        region = get_rotating_region()
        assert region in AVAILABLE_FUNCTION_REGIONS

    def test_custom_regions(self):
        from v3.refresh.config import get_rotating_region
        regions = ["region-a", "region-b"]
        region = get_rotating_region(regions)
        assert region in regions

    def test_single_region(self):
        from v3.refresh.config import get_rotating_region
        region = get_rotating_region(["only-region"])
        assert region == "only-region"
