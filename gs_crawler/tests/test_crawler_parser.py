"""Tests for crawler.py, parser.py and scheduler estimates."""

import json
from unittest.mock import patch

from gs_crawler import crawler, scheduler
from gs_crawler.parser import parse_author
from gs_crawler.serpapi_client import SerpApiError
from gs_crawler.tests.conftest import make_article, make_page


def _pages(*sizes):
    pages, offset = [], 0
    for i, n in enumerate(sizes):
        pages.append(make_page(n, offset=offset, first=(i == 0)))
        offset += n
    return pages


class TestCrawlAuthor:
    def test_single_page(self):
        with patch.object(crawler.serpapi_client, "fetch_author_page", side_effect=_pages(61)) as f:
            r = crawler.crawl_author("TESTAAAAAAAJ")
        assert r.ok and r.searches == 1 and len(r.pages) == 1
        f.assert_called_once_with("TESTAAAAAAAJ", 0)

    def test_paginates_until_short_page(self):
        with patch.object(crawler.serpapi_client, "fetch_author_page", side_effect=_pages(100, 100, 26)) as f:
            r = crawler.crawl_author("TESTAAAAAAAJ")
        assert r.ok and r.searches == 3
        assert [c.args[1] for c in f.call_args_list] == [0, 100, 200]

    def test_exact_multiple_of_page_size_ends_on_empty_page(self):
        with patch.object(crawler.serpapi_client, "fetch_author_page", side_effect=_pages(100, 0)):
            r = crawler.crawl_author("TESTAAAAAAAJ")
        assert r.ok and r.searches == 2 and len(r.pages) == 1

    def test_failure_mid_profile_is_not_ok(self):
        side = [_pages(100)[0], SerpApiError("HTTP 503", transient=True)]
        with patch.object(crawler.serpapi_client, "fetch_author_page", side_effect=side):
            r = crawler.crawl_author("TESTAAAAAAAJ")
        assert not r.ok and "503" in r.error

    def test_respects_search_limit(self):
        with patch.object(crawler.serpapi_client, "fetch_author_page", side_effect=_pages(100, 100, 100)):
            r = crawler.crawl_author("TESTAAAAAAAJ", max_searches=2)
        assert not r.ok and r.searches == 2 and "limit" in r.error


class TestParseAuthor:
    def test_author_row(self):
        author, pubs = parse_author(_pages(100, 5), "TESTAAAAAAAJ", "2026-11-02T00:00:00+00:00")
        assert author["name"] == "Test Author"
        assert author["email_domain"] == "@test.edu"
        assert author["interests"] == ["Testing", "Data"]
        assert (author["citedby"], author["hindex"], author["i10index"]) == (1200, 15, 20)
        assert (author["citedby_since"], author["hindex_since"], author["i10index_since"]) == (500, 10, 12)
        assert author["since_year"] == 2021
        assert json.loads(author["cites_per_year"]) == {"2023": 100, "2024": 120}
        assert author["coauthors"] == [{"scholar_id": "COAUTHAAAAJ", "name": "Co Author", "affiliation": "Elsewhere"}]
        assert author["num_publications"] == 105 == len(pubs)

    def test_publication_rows(self):
        page = make_page(0)
        page["articles"] = [
            make_article(1, year="2010", cites=7),
            make_article(2, year="", cites=None),  # uncited, year only in venue string
            make_article(1, year="2010", cites=7),  # duplicate id across pages
        ]
        page["articles"][1]["publication"] = "Workshop, 2019"
        _, pubs = parse_author([page], "TESTAAAAAAAJ", "2026-11-02T00:00:00+00:00")
        assert [p["author_pub_id"] for p in pubs] == ["TESTAAAAAAAJ:pub0001", "TESTAAAAAAAJ:pub0002"]
        assert pubs[0]["num_citations"] == 7 and pubs[0]["pub_year"] == 2010
        assert pubs[1]["num_citations"] == 0 and pubs[1]["pub_year"] == 2019
        assert all(p["source"] == "serpapi" for p in pubs)


class TestEstimates:
    def test_estimate_searches(self):
        assert scheduler.estimate_searches(0) == 1
        assert scheduler.estimate_searches(61) == 1
        assert scheduler.estimate_searches(95) == 2  # padded for list growth
        assert scheduler.estimate_searches(1426) == 16
        assert scheduler.estimate_searches(None) == scheduler.UNKNOWN_AUTHOR_SEARCHES


class TestSelectBatch:
    class _Client:
        def __init__(self, rows):
            self.rows = rows

        def query(self, sql, job_config=None):
            rows = self.rows

            class _Job:
                def result(self):
                    return rows
            return _Job()

    def test_fills_budget_in_priority_order(self):
        rows = [
            {"scholar_id": "queued", "num_publications": None},
            {"scholar_id": "huge", "num_publications": 3000},
            {"scholar_id": "small", "num_publications": 40},
        ]
        batch = scheduler.select_batch(5, client=self._Client(rows))
        assert batch == [("queued", 3, False), ("small", 1, False)]

    def test_zero_budget(self):
        assert scheduler.select_batch(0, client=self._Client([])) == []
