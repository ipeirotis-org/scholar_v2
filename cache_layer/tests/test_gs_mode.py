"""Tests for DATA_SOURCE=gs (Google Scholar profiles-only) behaviour."""

from datetime import datetime, timezone
from unittest import mock

import pytest

from cache_layer.bigquery_client import BigQueryClient
from cache_layer.cache_service import CacheService
from cache_layer.config import Config

GS_ID = "PA9La6oAAAAJ"


@pytest.fixture
def gs_mode(monkeypatch):
    monkeypatch.setattr(Config, "IS_GS", True)
    monkeypatch.setattr(Config, "DATA_SOURCE", "gs")
    monkeypatch.setattr(Config, "BQ_STATS_DATASET", "gs_statistics")
    monkeypatch.setattr(Config, "USE_MATERIALIZED_TABLES", False)


def _client(df=None):
    bq = mock.MagicMock()
    if df is not None:
        bq.query.return_value.result.return_value.to_dataframe.return_value = df
    return BigQueryClient(client=bq), bq


class TestGsQueries:
    def test_pub_stats_read_gs_views_not_s2(self, gs_mode):
        import pandas as pd
        client, bq = _client(pd.DataFrame([{"author_pub_id": f"{GS_ID}:x", "title": "T"}]))
        assert client.get_author_pub_stats(GS_ID)[0]["title"] == "T"
        sql = bq.query.call_args[0][0]
        assert "gs_statistics.stats_author_publication_pip_inputs_current" in sql
        assert "gs_statistics.stats_publication_current" in sql
        assert "s2_data" not in sql

    def test_author_stats_use_views_not_tables(self, gs_mode):
        client, bq = _client()
        client.get_author_stats(GS_ID)
        sql = bq.query.call_args[0][0]
        assert "gs_statistics.ranked_author_current`" in sql
        assert "_table" not in sql
        # Field columns come through P.* (pip scores view), so the query works
        # whether or not the deployed view has them yet.
        assert "gs_statistics.ranked_author_pip_scores_current`" in sql
        assert "P.* EXCEPT (scholar_id, year_of_first_pub)" in sql

    def test_freshness_reads_snapshot_view(self, gs_mode):
        import pandas as pd
        ts = datetime(2026, 11, 2, tzinfo=timezone.utc)
        client, bq = _client(pd.DataFrame([{"last_updated": ts}]))
        assert client.get_author_freshness(GS_ID) == (True, ts)
        assert "gs_data.authors_latest" in bq.query.call_args[0][0]

    def test_temporal_queries_are_skipped(self, gs_mode):
        client, bq = _client()
        assert client.get_author_temporal_stats(GS_ID) == []
        assert client.get_publication_stats(f"{GS_ID}:x") == []
        bq.query.assert_not_called()


class TestGsOnDemandCrawl:
    def _service(self, freshness, crawl_result=True):
        bq, writer = mock.MagicMock(), mock.MagicMock()
        bq.get_author_freshness.side_effect = freshness
        bq.get_author_stats.return_value = {"name": "A"}
        bq.get_author_pub_stats.return_value = [{"author_pub_id": "x"}]
        bq.get_author_temporal_stats.return_value = []
        writer.write_batch.return_value = 2
        crawl = mock.MagicMock(return_value=crawl_result)
        return CacheService(bq=bq, writer=writer, gs_crawl=crawl), crawl

    def test_unknown_author_is_crawled_then_cached(self, gs_mode):
        ts = datetime(2026, 11, 2, tzinfo=timezone.utc)
        svc, crawl = self._service([(False, None), (True, ts)])
        result = svc.dispatch("populate_author_profile", {"scholar_id": GS_ID})
        crawl.assert_called_once_with(GS_ID)
        assert result["status"] == "ok"

    def test_known_author_is_not_crawled(self, gs_mode):
        svc, crawl = self._service([(True, datetime.now(timezone.utc))])
        svc.dispatch("populate_author_profile", {"scholar_id": GS_ID})
        crawl.assert_not_called()

    def test_failed_crawl_is_not_found(self, gs_mode):
        svc, crawl = self._service([(False, None)], crawl_result=False)
        result = svc.dispatch("populate_author_profile", {"scholar_id": GS_ID})
        assert result["status"] == "not_found"

    def test_crawl_exception_is_not_found(self, gs_mode):
        svc, crawl = self._service([(False, None)])
        crawl.side_effect = RuntimeError("SerpApi down")
        result = svc.dispatch("populate_author_profile", {"scholar_id": GS_ID})
        assert result["status"] == "not_found"

    def test_non_gs_ids_are_not_crawled(self, gs_mode):
        svc, crawl = self._service([(False, None)])
        svc.dispatch("populate_author_profile", {"scholar_id": "2942126"})
        crawl.assert_not_called()

    def test_s2_mode_never_crawls(self):
        assert not Config.IS_GS
        svc, crawl = self._service([(False, None)])
        svc.dispatch("populate_author_profile", {"scholar_id": GS_ID})
        crawl.assert_not_called()


class TestGsPurgeDisabled:
    def test_purge_refused(self, gs_mode):
        writer = mock.MagicMock()
        svc = CacheService(bq=mock.MagicMock(), writer=writer)
        result = svc.dispatch("purge_legacy_cache", {})
        assert result["status"] == "error"
        writer.db.collection.assert_not_called()



class _FakeLock:
    def __init__(self, token="tok", error=None):
        self.token, self.error = token, error
        self.released = []

    def acquire(self, scholar_id):
        if self.error:
            raise self.error
        return self.token

    def release(self, scholar_id, token):
        self.released.append((scholar_id, token))
        return True


class TestGsCrawlLockUse:
    def _service(self, lock):
        bq, writer = mock.MagicMock(), mock.MagicMock()
        bq.get_author_freshness.side_effect = [(False, None), (True, datetime.now(timezone.utc))]
        bq.get_author_stats.return_value = {"name": "A"}
        bq.get_author_pub_stats.return_value = []
        bq.get_author_temporal_stats.return_value = []
        writer.write_batch.return_value = 1
        crawl = mock.MagicMock(return_value=True)
        return CacheService(bq=bq, writer=writer, gs_crawl=crawl, crawl_lock=lock), crawl

    def test_owner_crawls_and_releases_its_token(self, gs_mode):
        lock = _FakeLock("t1")
        svc, crawl = self._service(lock)
        assert svc.dispatch("populate_author_profile", {"scholar_id": GS_ID})["status"] == "ok"
        crawl.assert_called_once_with(GS_ID)
        assert lock.released == [(GS_ID, "t1")]

    def test_held_lock_skips_crawl(self, gs_mode):
        lock = _FakeLock(token=None)
        svc, crawl = self._service(lock)
        assert svc.dispatch("populate_author_profile", {"scholar_id": GS_ID})["status"] == "in_progress"
        crawl.assert_not_called()
        assert lock.released == []

    def test_released_when_crawl_raises(self, gs_mode):
        lock = _FakeLock("t2")
        svc, crawl = self._service(lock)
        crawl.side_effect = RuntimeError("SerpApi down")
        svc.bq.get_author_freshness.side_effect = [(False, None)]
        assert svc.dispatch("populate_author_profile", {"scholar_id": GS_ID})["status"] == "not_found"
        assert lock.released == [(GS_ID, "t2")]

    def test_firestore_failure_crawls_but_releases_nothing(self, gs_mode):
        lock = _FakeLock(error=RuntimeError("firestore down"))
        svc, crawl = self._service(lock)
        assert svc.dispatch("populate_author_profile", {"scholar_id": GS_ID})["status"] == "ok"
        crawl.assert_called_once()
        assert lock.released == []
