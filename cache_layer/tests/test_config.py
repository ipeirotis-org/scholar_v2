"""Tests for cache_layer configuration."""

from cache_layer.config import Config


class TestConfig:
    def test_bq_view(self):
        result = Config.bq_view("ranked_author_current")
        assert result == "`scholar-version2.statistics.ranked_author_current`"

    def test_bq_raw(self):
        result = Config.bq_raw("author")
        assert result == "`scholar-version2.scholar_raw_data.author`"

    def test_queue_path_default(self):
        result = Config.queue_path()
        assert "cache-priority" in result
        assert "scholar-version2" in result

    def test_queue_path_custom(self):
        result = Config.queue_path("cache-batch")
        assert "cache-batch" in result

    def test_cache_collection_names(self):
        assert Config.CACHE_AUTHOR_STATS == "v3_author_stats"
        assert Config.CACHE_AUTHOR_PUB_STATS == "v3_author_pub_stats"
        assert Config.CACHE_AUTHOR_TEMPORAL == "v3_author_temporal"
        assert Config.CACHE_PUB_STATS == "v3_pub_stats"
        assert Config.CACHE_AUTHOR_FRESHNESS == "v3_author_freshness"
