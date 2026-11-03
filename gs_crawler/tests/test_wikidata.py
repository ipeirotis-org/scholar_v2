"""Tests for wikidata.py (GS IDs + pageviews) and the scheduler's new-author share."""

from unittest.mock import MagicMock, patch

import pytest

from gs_crawler import main, scheduler, wikidata
from gs_crawler.crawler import CrawlResult, crawl_author
from gs_crawler.serpapi_client import SerpApiError, validate_author_page
from gs_crawler.tests.conftest import make_page


def _resp(status=200, text="", json_data=None, headers=None):
    r = MagicMock()
    r.status_code = status
    r.text = text
    r.headers = headers or {}
    r.json.return_value = json_data
    return r


class TestNormalize:
    def test_keeps_valid_unique_ids(self):
        raw = [
            {"p": "http://www.wikidata.org/entity/Q1", "g": "iGzDl8IAAAAJ", "label": "Peyman Milanfar",
             "desc": "engineer", "links": "12", "enwiki": "Peyman Milanfar"},
            {"p": "http://www.wikidata.org/entity/Q2", "g": "iGzDl8IAAAAJ", "label": "Duplicate"},
            {"p": "http://www.wikidata.org/entity/Q3", "g": "too-short", "label": "Bad ID"},
            {"p": "http://www.wikidata.org/entity/Q4", "g": "AAAAAAAAAAAJ", "label": ""},
            {"p": "http://www.wikidata.org/entity/Q5", "g": "BBBBBBBBBBBJ", "label": "No Extras"},
        ]
        rows = wikidata.normalize(raw, "2026-11-03T00:00:00+00:00")
        assert [r["scholar_id"] for r in rows] == ["iGzDl8IAAAAJ", "BBBBBBBBBBBJ"]
        first, second = rows
        assert first["wikidata_id"] == "Q1" and first["name"] == "Peyman Milanfar"
        assert first["sitelinks"] == 12 and first["enwiki_title"] == "Peyman Milanfar"
        assert second["description"] is None and second["sitelinks"] is None and second["enwiki_title"] is None


class TestPartitions:
    def test_timed_out_prefix_is_split(self):
        header = "p,g,label,desc,links,enwiki\n"

        def get(url, params, headers, timeout):
            q = params["query"]
            if 'STRSTARTS(?g, "a")' in q:
                return _resp(200, header + "x\njava.util.concurrent.TimeoutException\n")
            if 'STRSTARTS(?g, "ab")' in q:
                return _resp(200, header + "http://www.wikidata.org/entity/Q9,abcdefghijkl,Ann,,1,\n")
            return _resp(200, header)

        session = MagicMock()
        session.get.side_effect = get
        rows = wikidata._fetch_partition("a", session)
        assert [r["g"] for r in rows] == ["abcdefghijkl"]
        assert session.get.call_count == 1 + len(wikidata.ID_CHARS)

    def test_client_error_raises(self):
        session = MagicMock()
        session.get.return_value = _resp(403, "forbidden")
        with pytest.raises(wikidata.WikidataError):
            wikidata._fetch("a", session)


class TestSources:
    HEADER = "p,g,label,desc,links,enwiki\n"

    def _rows(self, n):
        return self.HEADER + "".join(
            f"http://www.wikidata.org/entity/Q{i},{i:012d},Name {i},,1,\n" for i in range(n))

    def test_qlever_full_result_is_used(self, monkeypatch):
        monkeypatch.setattr(wikidata, "MIN_FULL_ROWS", 3)
        session = MagicMock()
        session.get.return_value = _resp(200, self._rows(5))
        with patch.object(wikidata, "fetch_pageviews", return_value={}):
            rows = wikidata.fetch_all(session)
        assert len(rows) == 5
        assert session.get.call_count == 1
        assert session.get.call_args.args[0] == wikidata.QLEVER_URL
        assert "STRSTARTS" not in session.get.call_args.kwargs["params"]["query"]

    def test_short_qlever_result_falls_back_to_wdqs(self, monkeypatch):
        monkeypatch.setattr(wikidata, "MIN_FULL_ROWS", 100)
        session = MagicMock()
        session.get.side_effect = lambda url, **kw: _resp(200, self._rows(5) if url == wikidata.QLEVER_URL
                                                          else self.HEADER)
        with patch.object(wikidata, "fetch_pageviews", return_value={}):
            wikidata.fetch_all(session)
        wdqs = [c for c in session.get.call_args_list if c.args[0] == wikidata.SPARQL_URL]
        assert len(wdqs) == len(wikidata.ID_CHARS)

    def test_throttled_small_partition_is_retried_not_failed(self):
        responses = [_resp(200, self.HEADER + "x\njava.util.concurrent.TimeoutException\n"),
                     _resp(200, self.HEADER + "http://www.wikidata.org/entity/Q1,l5kAAAAAAAAJ,Ann,,1,\n")]
        session = MagicMock()
        session.get.side_effect = responses
        with patch.object(wikidata.time, "sleep") as sleep:
            rows = wikidata._fetch_partition("l5k", session)
        assert [r["g"] for r in rows] == ["l5kAAAAAAAAJ"]
        sleep.assert_called_once()


class TestPageviews:
    def test_maps_normalized_and_redirected_titles_back(self):
        data = {"query": {
            "normalized": [{"from": "judea_Pearl", "to": "Judea pearl"}],
            "redirects": [{"from": "Judea pearl", "to": "Judea Pearl"}],
            "pages": [
                {"title": "Judea Pearl", "pageviews": {"2026-10-01": 100, "2026-10-02": None, "2026-10-03": 50}},
                {"title": "Peyman Milanfar", "pageviews": {"2026-10-01": 7}},
                {"title": "Missing Page", "missing": True},
            ]}}
        session = MagicMock()
        session.get.return_value = _resp(200, json_data=data)
        views = wikidata._pageview_batch(["judea_Pearl", "Peyman Milanfar", "Missing Page"], session)
        assert views == {"judea_Pearl": 150, "Peyman Milanfar": 7}

    def test_failures_leave_views_empty_without_raising(self):
        session = MagicMock()
        session.get.return_value = _resp(503, "down")
        with patch.object(wikidata.time, "sleep"):
            views = wikidata.fetch_pageviews([f"T{i}" for i in range(600)], session, pause=0)
        assert views == {}
        assert session.get.call_count == 10  # gives up after 10 failed batches with no data


class TestLoad:
    def test_refuses_to_shrink_table_by_more_than_half(self):
        client = MagicMock()
        client.get_table.return_value.num_rows = 1000
        with pytest.raises(wikidata.WikidataError):
            wikidata.load([{"scholar_id": "x"}] * 400, client=client)
        client.load_table_from_json.assert_not_called()

    def test_replaces_table(self):
        client = MagicMock()
        client.get_table.return_value.num_rows = 1000
        assert wikidata.load([{"scholar_id": "x"}] * 900, client=client) == 900
        job_config = client.load_table_from_json.call_args.kwargs["job_config"]
        assert job_config.write_disposition == wikidata.bigquery.WriteDisposition.WRITE_TRUNCATE


class _Client:
    def __init__(self, rows):
        self.rows = rows
        self.params = None

    def query(self, sql, job_config=None):
        self.params = {p.name: p.value for p in job_config.query_parameters}
        rows = self.rows

        class _Job:
            def result(self):
                return rows
        return _Job()


class TestNewAuthorShare:
    ROWS = [
        {"scholar_id": "queued", "num_publications": None, "priority": 0},
        {"scholar_id": "new1", "num_publications": None, "priority": 1},
        {"scholar_id": "new2", "num_publications": None, "priority": 1},
        {"scholar_id": "new3", "num_publications": None, "priority": 1},
        {"scholar_id": "stale1", "num_publications": 50, "priority": 2},
        {"scholar_id": "stale2", "num_publications": 50, "priority": 2},
    ]

    def test_new_authors_capped_by_share(self):
        client = _Client(self.ROWS)
        batch = scheduler.select_batch(20, client=client, new_author_share=0.3)
        ids = [sid for sid, _, _ in batch]
        # 0.3 * 20 = 6 searches for new authors: two unknown profiles at 3 each.
        assert ids == ["queued", "new1", "new2", "stale1", "stale2"]
        assert client.params["new_limit"] == 6 // scheduler.UNKNOWN_AUTHOR_SEARCHES + 1

    def test_zero_share_skips_new_authors(self):
        batch = scheduler.select_batch(20, client=_Client(self.ROWS), new_author_share=0)
        assert [sid for sid, _, _ in batch] == ["queued", "stale1", "stale2"]

    def test_query_orders_new_authors_by_pageviews_and_skips_failures(self):
        sql = scheduler.candidates_query()
        assert "enwiki_views_60d DESC NULLS LAST" in sql
        assert "crawl_failures" in sql and "@cooldown_days" in sql


class TestFailures:
    def test_missing_profile_is_permanent(self):
        with pytest.raises(SerpApiError) as exc:
            validate_author_page({"error": "Google hasn't returned any results for this query."}, 0)
        assert not exc.value.transient

    def test_crawl_marks_permanent_failure(self, serpapi_key):
        r = MagicMock(status_code=200)
        r.json.return_value = {"error": "Google hasn't returned any results for this query."}
        with patch("requests.get", return_value=r) as get:
            result = crawl_author("GONEAAAAAAAJ")
        assert not result.ok and result.permanent
        assert get.call_count == 1  # not retried

    def test_crawl_and_store_records_only_permanent_failures(self):
        ok = CrawlResult("GOODAAAAAAAJ", pages=[make_page(3, author_id="GOODAAAAAAAJ")], searches=1)
        gone = CrawlResult("GONEAAAAAAAJ", searches=1, error="no profile", permanent=True)
        flaky = CrawlResult("FLAKYAAAAAAJ", searches=1, error="HTTP 503")
        with patch.object(main, "crawl_author", side_effect=[ok, gone, flaky]), \
             patch.object(main.writer, "write_raw_pages"), \
             patch.object(main.writer, "load_snapshots"), \
             patch.object(main.writer, "record_failures") as rec, \
             patch.object(main.cache_invalidate, "invalidate_authors"):
            summary = main.crawl_and_store(["GOODAAAAAAAJ", "GONEAAAAAAAJ", "FLAKYAAAAAAJ"], budget=10)
        assert summary["failed"] == 2
        recorded = rec.call_args[0][0]
        assert [f["scholar_id"] for f in recorded] == ["GONEAAAAAAAJ"]


class TestNewAuthorBudgetEnforced:
    def test_new_authors_cannot_exceed_their_share(self):
        # Each new profile needs 8 searches; the share is 6 of a 20-search run.
        calls = []

        def fake_crawl(sid, max_searches):
            calls.append((sid, max_searches))
            if sid.startswith("NEW"):
                used = min(8, max_searches)
                if used < 8:
                    return CrawlResult(sid, searches=used, error=f"search limit {used} reached at start=100")
            return CrawlResult(sid, pages=[make_page(3, author_id=sid)], searches=1)

        with patch.object(main, "crawl_author", side_effect=fake_crawl), \
             patch.object(main.writer, "write_raw_pages"), \
             patch.object(main.writer, "load_snapshots"), \
             patch.object(main.writer, "record_failures") as rec, \
             patch.object(main.cache_invalidate, "invalidate_authors"):
            summary = main.crawl_and_store(["NEW1AAAAAAAJ", "NEW2AAAAAAAJ", "OLD1AAAAAAAJ"], budget=20,
                                           new_ids=["NEW1AAAAAAAJ", "NEW2AAAAAAAJ"], new_budget=6)
        assert calls == [("NEW1AAAAAAAJ", 6), ("OLD1AAAAAAAJ", 14)]  # NEW2 skipped: share used up
        assert summary["new_searches"] == 6 and summary["searches"] == 7
        # Too large for the share: cooled down instead of retried daily.
        assert [f["scholar_id"] for f in rec.call_args[0][0]] == ["NEW1AAAAAAAJ"]

    def test_run_refresh_passes_new_authors_and_share(self):
        with patch.object(main, "searches_left", return_value=2000), \
             patch.object(main.scheduler, "select_batch",
                          return_value=[("Q", 3, False), ("N", 3, True), ("S", 1, False)]), \
             patch.object(main, "crawl_and_store", return_value={}) as cas:
            main.run_refresh()
        args, kwargs = cas.call_args
        assert args[0] == ["Q", "N", "S"]
        assert kwargs["new_ids"] == ["N"]
        assert kwargs["new_budget"] == main.scheduler.new_author_budget(args[1])
