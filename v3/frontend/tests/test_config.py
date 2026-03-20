"""Tests for v3 frontend configuration."""

from v3.frontend.config import Config


class TestConfig:
    def test_project_id_default(self):
        assert Config.PROJECT_ID == "scholar-version2"

    def test_bq_view_format(self):
        result = Config.bq_view("ranked_author_current")
        assert result == "`scholar-version2.statistics.ranked_author_current`"

    def test_bq_raw_format(self):
        result = Config.bq_raw("author")
        assert result == "`scholar-version2.scholar_raw_data.author`"

    def test_secret_key_generated(self):
        assert Config.SECRET_KEY is not None
        assert len(Config.SECRET_KEY) >= 32

    def test_cache_collection_names(self):
        assert Config.CACHE_AUTHOR_STATS.startswith("v3_")
        assert Config.CACHE_AUTHOR_PUB_STATS.startswith("v3_")
        assert Config.CACHE_PUB_STATS.startswith("v3_")
