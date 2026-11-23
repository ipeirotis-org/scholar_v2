"""Tests for cache_invalidate.py and its use in main.crawl_and_store."""

from unittest.mock import MagicMock, patch

import pytest

from gs_crawler import cache_invalidate, main
from gs_crawler.crawler import CrawlResult
from gs_crawler.tests.conftest import make_page


def test_deletes_every_author_collection():
    db = MagicMock()
    n = cache_invalidate.invalidate_authors(["A", "B"], db=db)
    assert n == 8
    collections = [c.args[0] for c in db.collection.call_args_list]
    assert set(collections) == set(cache_invalidate.AUTHOR_CACHE_COLLECTIONS)
    docs = [c.args[0] for c in db.collection.return_value.document.call_args_list]
    assert docs.count("A") == 4 and docs.count("B") == 4


def test_failures_are_tolerated():
    db = MagicMock()
    db.collection.return_value.document.return_value.delete.side_effect = RuntimeError("boom")
    assert cache_invalidate.invalidate_authors(["A"], db=db) == 0


def test_nothing_to_do():
    assert cache_invalidate.invalidate_authors([], db=MagicMock()) == 0


def test_crawl_and_store_invalidates_only_stored_authors():
    ok = CrawlResult("GOODAAAAAAAJ", pages=[make_page(3, author_id="GOODAAAAAAAJ")], searches=1)
    bad = CrawlResult("BADAAAAAAAAJ", searches=1, error="HTTP 503")
    with patch.object(main, "crawl_author", side_effect=[ok, bad]), \
         patch.object(main.writer, "write_raw_pages"), \
         patch.object(main.writer, "load_snapshots") as load, \
         patch.object(main.cache_invalidate, "invalidate_authors") as inv:
        summary = main.crawl_and_store(["GOODAAAAAAAJ", "BADAAAAAAAAJ"], budget=10)
    assert summary["crawled"] == 1 and summary["failed"] == 1
    load.assert_called_once()
    inv.assert_called_once_with(["GOODAAAAAAAJ"])


def test_invalidate_missing_pip_targets_only_stale_gs_docs():
    def _doc(doc_id, data):
        d = MagicMock()
        d.id = doc_id
        d.to_dict.return_value = {"data": data}
        return d

    db = MagicMock()
    db.collection.return_value.stream.return_value = [
        _doc("STALEGSAAAAJ", {"pip_auc_score": 0.42}),                  # stale GS -> invalidate
        _doc("FRESHGSAAAAJ", {"pip_score": 0.51, "pip_auc_score": 0.4}),  # already has pip_score -> keep
        _doc("123456789012", {"pip_auc_score": 0.5}),                   # 12-digit S2 ID -> keep
        _doc("2942126", {"pip_auc_score": 0.5}),                        # short S2 ID -> keep
    ]
    count = cache_invalidate.invalidate_missing_pip(db=db)
    assert count == 1
    batch = db.batch.return_value
    assert batch.delete.call_count == len(cache_invalidate.AUTHOR_CACHE_COLLECTIONS)
    batch.commit.assert_called_once()
    deleted_docs = [c.args[0] for c in db.collection.return_value.document.call_args_list]
    assert deleted_docs == ["STALEGSAAAAJ"] * len(cache_invalidate.AUTHOR_CACHE_COLLECTIONS)


def test_invalidate_legacy_pip_mode(monkeypatch):
    monkeypatch.setenv("CRAWL_MODE", "invalidate_legacy_pip")
    with patch.object(main.cache_invalidate, "invalidate_missing_pip", return_value=3) as inv:
        main.main()
    inv.assert_called_once()


def test_crawl_and_store_starts_no_profile_after_the_time_limit(monkeypatch):
    """Snapshots load at the end of a run, so a run must end before the job's task timeout."""
    clock = iter([0.0, 10.0, 5000.0])  # deadline set at 0; first profile at 10 s; second after the limit
    monkeypatch.setattr(main.time, "monotonic", lambda: next(clock))
    ok = CrawlResult("GOODAAAAAAAJ", pages=[make_page(3, author_id="GOODAAAAAAAJ")], searches=1)
    with patch.object(main, "crawl_author", return_value=ok) as crawl, \
         patch.object(main.writer, "write_raw_pages"), \
         patch.object(main.writer, "load_snapshots") as load, \
         patch.object(main.cache_invalidate, "invalidate_authors"):
        summary = main.crawl_and_store(["GOODAAAAAAAJ", "LATEAAAAAAAJ"], budget=10)
    crawl.assert_called_once()
    assert summary["crawled"] == 1 and summary["skipped"] == 1
    load.assert_called_once()  # what was fetched in time is still stored


def test_invalidate_all_gs_drops_every_cached_gs_profile():
    def _doc(doc_id):
        d = MagicMock()
        d.id = doc_id
        return d

    db = MagicMock()
    db.collection.return_value.select.return_value.stream.side_effect = [
        [_doc("AAAAAAAAAAAJ"), _doc("2942126")],          # v3_author_freshness (one S2 leftover)
        [_doc("AAAAAAAAAAAJ"), _doc("BBBBBBBBBBBJ")],     # v3_author_stats
    ]
    assert cache_invalidate.invalidate_all_gs(db=db) == 2
    batch = db.batch.return_value
    assert batch.delete.call_count == 2 * len(cache_invalidate.AUTHOR_CACHE_COLLECTIONS)
    deleted_docs = {c.args[0] for c in db.collection.return_value.document.call_args_list}
    assert deleted_docs == {"AAAAAAAAAAAJ", "BBBBBBBBBBBJ"}


def test_invalidate_all_gs_with_empty_cache():
    db = MagicMock()
    db.collection.return_value.select.return_value.stream.return_value = []
    assert cache_invalidate.invalidate_all_gs(db=db) == 0
    db.batch.assert_not_called()


def test_percentiles_mode_rebuilds_tables_then_drops_cached_profiles(monkeypatch):
    monkeypatch.setenv("CRAWL_MODE", "percentiles")
    calls = []
    with patch.object(main.materialize, "run", side_effect=lambda **kw: calls.append("tables") or []), \
         patch.object(main.cache_invalidate, "invalidate_all_gs",
                      side_effect=lambda: calls.append("cache") or 0), \
         patch.object(main, "refresh_search_index") as index, \
         patch.object(main, "refresh_featured") as feat:
        main.main()
    assert calls == ["tables", "cache"]  # cached pages must not be rebuilt from the old tables
    index.assert_not_called()
    feat.assert_not_called()


def test_materialize_mode_also_drops_cached_profiles(monkeypatch):
    monkeypatch.setenv("CRAWL_MODE", "materialize")
    with patch.object(main.materialize, "run", return_value=[]), \
         patch.object(main.cache_invalidate, "invalidate_all_gs", return_value=0) as inv, \
         patch.object(main, "refresh_search_index"), \
         patch.object(main, "refresh_featured"):
        main.main()
    inv.assert_called_once()


class TestRunSurvivesFailures:
    """Every profile fetched is paid for, so a failure must not discard the rest of the run
    (2026-11-19: a dropped connection during a raw-page upload ended a run and lost ~115 searches)."""

    IDS = ["AAAAAAAAAAAJ", "BBBBBBBBBBBJ", "CCCCCCCCCCCJ"]

    @staticmethod
    def _ok(sid, max_searches=None):
        return CrawlResult(sid, pages=[make_page(3, author_id=sid)], searches=1)

    def test_failed_raw_upload_is_retried_then_the_profile_is_still_stored(self, monkeypatch):
        monkeypatch.setattr(main.time, "sleep", lambda s: None)
        with patch.object(main, "crawl_author", side_effect=self._ok), \
             patch.object(main.writer, "write_raw_pages", side_effect=ConnectionError("dropped")) as raw, \
             patch.object(main.writer, "load_snapshots") as load, \
             patch.object(main.cache_invalidate, "invalidate_authors") as inv:
            summary = main.crawl_and_store(self.IDS[:1], budget=10)
        assert raw.call_count == 3
        assert summary["crawled"] == 1 and summary["failed"] == 0
        assert len(load.call_args.args[0]) == 1
        inv.assert_called_once_with(self.IDS[:1])

    def test_raw_upload_that_succeeds_on_retry(self, monkeypatch):
        monkeypatch.setattr(main.time, "sleep", lambda s: None)
        with patch.object(main.writer, "write_raw_pages", side_effect=[ConnectionError("dropped"), None]) as raw:
            assert main.archive_raw_pages("AAAAAAAAAAAJ", "2026-11-23T00:00:00+00:00", [{}])
        assert raw.call_count == 2

    def test_snapshots_are_stored_in_chunks(self, monkeypatch):
        monkeypatch.setattr(main.Config, "STORE_EVERY_AUTHORS", 2)
        with patch.object(main, "crawl_author", side_effect=self._ok), \
             patch.object(main.writer, "write_raw_pages"), \
             patch.object(main.writer, "load_snapshots") as load, \
             patch.object(main.cache_invalidate, "invalidate_authors") as inv:
            summary = main.crawl_and_store(self.IDS, budget=10)
        assert summary["crawled"] == 3
        assert [len(c.args[0]) for c in load.call_args_list] == [2, 1]
        assert [c.args[0] for c in inv.call_args_list] == [self.IDS[:2], self.IDS[2:]]

    def test_unexpected_error_still_stores_what_was_fetched(self):
        def crawl(sid, max_searches=None):
            if sid == self.IDS[2]:
                raise RuntimeError("boom")
            return self._ok(sid)

        with patch.object(main, "crawl_author", side_effect=crawl), \
             patch.object(main.writer, "write_raw_pages"), \
             patch.object(main.writer, "load_snapshots") as load, \
             patch.object(main.cache_invalidate, "invalidate_authors") as inv, \
             pytest.raises(RuntimeError):
            main.crawl_and_store(self.IDS, budget=10)
        assert len(load.call_args.args[0]) == 2
        inv.assert_called_once_with(self.IDS[:2])

    def test_unparseable_profile_is_recorded_and_the_run_goes_on(self):
        def parse(pages, sid, fetched_at):
            if sid == self.IDS[0]:
                raise KeyError("articles")
            return {"scholar_id": sid}, [{"scholar_id": sid}]

        with patch.object(main, "crawl_author", side_effect=self._ok), \
             patch.object(main, "parse_author", side_effect=parse), \
             patch.object(main.writer, "write_raw_pages") as raw, \
             patch.object(main.writer, "load_snapshots") as load, \
             patch.object(main.writer, "record_failures") as rec, \
             patch.object(main.cache_invalidate, "invalidate_authors"):
            summary = main.crawl_and_store(self.IDS, budget=10)
        assert summary["crawled"] == 2 and summary["failed"] == 1
        assert [r["scholar_id"] for r in load.call_args.args[0]] == self.IDS[1:]
        recorded = rec.call_args.args[0]
        assert [f["scholar_id"] for f in recorded] == self.IDS[:1]
        assert recorded[0]["error"].startswith("parse error: KeyError")
        assert raw.call_count == 2  # nothing archived for the profile that was not stored
