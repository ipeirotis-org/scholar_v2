"""Tests for cache_invalidate.py and its use in main.crawl_and_store."""

from unittest.mock import MagicMock, patch

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
