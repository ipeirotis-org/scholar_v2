"""Tests for AuthorSearchService — the tiered search strategy."""

from unittest import mock

import pytest

from author_search.search_service import AuthorSearchService


@pytest.fixture(autouse=True)
def _reset_author_index():
    """Reset module-level index state and mock _ensure_index_loaded to avoid
    Firestore calls (get_index_chunk on MagicMock causes infinite loop)."""
    with mock.patch("author_search.search_service._ensure_index_loaded"), \
         mock.patch("author_search.search_service._search_in_memory", return_value=None):
        yield


def _make_author(scholar_id, name="Test Author", source=None, citedby=100):
    author = {
        "scholar_id": scholar_id,
        "name": name,
        "affiliation": "Test University",
        "email_domain": "test.edu",
        "citedby": citedby,
        "hindex": 10,
    }
    if source:
        author["source"] = source
    return author


class TestSearchEmpty:
    def test_empty_string_returns_empty(self):
        svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=mock.MagicMock())
        assert svc.search("") == []

    def test_single_char_returns_empty(self):
        svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=mock.MagicMock())
        assert svc.search("a") == []

    def test_none_returns_empty(self):
        svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=mock.MagicMock())
        assert svc.search(None) == []


class TestLocalSearch:
    """Default search (no scholar flag) queries index + BigQuery only."""

    def test_returns_crawled_authors(self):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        bq.search_crawled_authors.return_value = [
            _make_author(f"id{i}", f"Author {i}") for i in range(5)
        ]
        bq.search_coauthor_network.return_value = []
        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author")

        assert len(results) == 5
        assert all(r["source"] == "database" for r in results)

    def test_searches_both_crawled_and_coauthor(self):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        bq.search_crawled_authors.return_value = [_make_author("id1")]
        bq.search_coauthor_network.return_value = [
            _make_author(f"co{i}", f"Coauthor {i}") for i in range(3)
        ]
        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author")

        assert len(results) == 4  # 1 crawled + 3 coauthors
        bq.search_coauthor_network.assert_called_once()

    def test_never_calls_scholar(self):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        bq.search_crawled_authors.return_value = []
        bq.search_coauthor_network.return_value = []
        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author")

        assert results == []
        # Scholar cache should not be checked for local search
        cache.get.assert_not_called()

    def test_deduplicates_across_crawled_and_coauthor(self):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        bq.search_crawled_authors.return_value = [_make_author("id1")]
        bq.search_coauthor_network.return_value = [_make_author("id1")]
        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author")

        ids = [r["scholar_id"] for r in results]
        assert ids.count("id1") == 1

    def test_uses_cached_local_results(self):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        cached = [_make_author("cached1", source="database")]
        cache.get_search_results.return_value = cached
        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author")

        assert results == cached
        bq.search_crawled_authors.assert_not_called()

    def test_caches_local_results(self):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        bq.search_crawled_authors.return_value = [_make_author("id1")]
        bq.search_coauthor_network.return_value = []
        svc = AuthorSearchService(bq_client=bq, cache=cache)
        svc.search("Author")

        cache.set_search_results.assert_called_once()


class TestScholarSearch:
    """Scholar search (scholar=True) merges Scholar with local results."""

    @mock.patch("author_search.search_service.scholar_client")
    def test_scholar_flag_queries_scholar(self, mock_scholar):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        bq.search_crawled_authors.return_value = []
        bq.search_coauthor_network.return_value = []
        cache.get.return_value = None
        mock_scholar.search_scholar.return_value = [
            _make_author("scholar1", "Scholar Author")
        ]

        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author", scholar=True)

        assert len(results) == 1
        assert results[0]["source"] == "scholar"
        cache.set.assert_called_once()

    @mock.patch("author_search.search_service.scholar_client")
    def test_uses_cached_scholar_results(self, mock_scholar):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        bq.search_crawled_authors.return_value = []
        bq.search_coauthor_network.return_value = []
        cache.get.return_value = [_make_author("cached1", "Cached Author")]

        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author", scholar=True)

        assert len(results) == 1
        assert results[0]["source"] == "scholar_cached"
        mock_scholar.search_scholar.assert_not_called()

    @mock.patch("author_search.search_service.scholar_client")
    def test_merges_local_and_scholar_results(self, mock_scholar):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        bq.search_crawled_authors.return_value = [_make_author("id1")]
        bq.search_coauthor_network.return_value = []
        cache.get.return_value = None
        mock_scholar.search_scholar.return_value = [
            _make_author("id2", "New Author"),
        ]

        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author", scholar=True)

        ids = [r["scholar_id"] for r in results]
        assert "id1" in ids
        assert "id2" in ids

    @mock.patch("author_search.search_service.scholar_client")
    def test_deduplicates_scholar_with_local(self, mock_scholar):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        bq.search_crawled_authors.return_value = [_make_author("id1")]
        bq.search_coauthor_network.return_value = []
        cache.get.return_value = None
        mock_scholar.search_scholar.return_value = [
            _make_author("id1", "Duplicate"),
            _make_author("id2", "New Author"),
        ]

        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author", scholar=True)

        ids = [r["scholar_id"] for r in results]
        assert ids.count("id1") == 1
        assert "id2" in ids
