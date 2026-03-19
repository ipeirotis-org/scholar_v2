"""Tests for ingestion config module."""

import os
from unittest import mock


class TestConfig:
    def test_defaults(self):
        from v3.ingestion.config import Config

        assert Config.PROJECT_ID == "scholar-version2"
        assert Config.BUCKET_NAME == "scholar_data_share"
        assert Config.BQ_DATASET == "scholar_raw_data"
        assert Config.BQ_AUTHOR_TABLE == "author"
        assert Config.BQ_PUB_TABLE == "pub"
        assert Config.BATCH_SIZE == 50
        assert Config.MAX_FILES_PER_RUN == 500

    def test_bq_table_id(self):
        from v3.ingestion.config import Config

        result = Config.bq_table_id("author")
        assert result == "scholar-version2.scholar_raw_data.author"

    @mock.patch.dict(os.environ, {"GCP_PROJECT_ID": "test-project", "BQ_DATASET": "test_ds"})
    def test_env_overrides(self):
        # Re-import to pick up env vars
        import importlib
        import v3.ingestion.config as cfg_module
        importlib.reload(cfg_module)

        assert cfg_module.Config.PROJECT_ID == "test-project"
        assert cfg_module.Config.BQ_DATASET == "test_ds"
        assert cfg_module.Config.bq_table_id("pub") == "test-project.test_ds.pub"

        # Reload with defaults for other tests
        with mock.patch.dict(os.environ, {}, clear=True):
            importlib.reload(cfg_module)
