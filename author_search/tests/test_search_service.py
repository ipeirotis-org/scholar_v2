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


class TestSearchCrawledAuthorsOnly:
    def test_sufficient_crawled_results_skips_coauthor_and_scholar(self):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        # Return 5 results (>= threshold of 5)
        bq.search_crawled_authors.return_value = [
            _make_author(f"id{i}", f"Author {i}") for i in range(5)
        ]
        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author")

        assert len(results) == 5
        assert all(r["source"] == "database" for r in results)
        bq.search_coauthor_network.assert_not_called()
        cache.get.assert_not_called()

    def test_crawled_results_have_database_source(self):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        bq.search_crawled_authors.return_value = [
            _make_author("id1", "Alice Smith")
        ] * 5
        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Alice")
        # Deduplication: only 1 unique scholar_id
        assert len(results) == 1
        assert results[0]["source"] == "database"


class TestSearchCoauthorFallback:
    def test_insufficient_crawled_results_searches_coauthors(self):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        bq.search_crawled_authors.return_value = [_make_author("id1")]
        bq.search_coauthor_network.return_value = [
            _make_author(f"co{i}", f"Coauthor {i}") for i in range(5)
        ]
        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author")

        assert len(results) == 6  # 1 crawled + 5 coauthors
        assert results[0]["source"] == "database"
        assert results[1]["source"] == "coauthor_network"
        # Enough results now, so no Scholar call
        cache.get.assert_not_called()

    def test_deduplicates_across_crawled_and_coauthor(self):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        bq.search_crawled_authors.return_value = [_make_author("id1")]
        # Same scholar_id as crawled result
        bq.search_coauthor_network.return_value = [_make_author("id1")]
        cache.get.return_value = None

        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author")

        # Only 1 result (deduplicated), insufficient so continues to Scholar
        ids = [r["scholar_id"] for r in results]
        assert ids.count("id1") == 1


class TestSearchScholarFallback:
    @mock.patch("author_search.search_service.scholar_client")
    def test_falls_back_to_scholar_when_local_insufficient(self, mock_scholar):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        bq.search_crawled_authors.return_value = []
        bq.search_coauthor_network.return_value = []
        cache.get.return_value = None
        mock_scholar.search_scholar.return_value = [
            _make_author("scholar1", "Scholar Author")
        ]

        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author")

        assert len(results) == 1
        assert results[0]["source"] == "scholar"
        # Results should be cached
        cache.set.assert_called_once()

    @mock.patch("author_search.search_service.scholar_client")
    def test_uses_cached_scholar_results(self, mock_scholar):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        bq.search_crawled_authors.return_value = []
        bq.search_coauthor_network.return_value = []
        cache.get.return_value = [_make_author("cached1", "Cached Author")]

        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author")

        assert len(results) == 1
        assert results[0]["source"] == "scholar_cached"
        mock_scholar.search_scholar.assert_not_called()

    @mock.patch("author_search.search_service.scholar_client")
    def test_scholar_results_deduplicated_with_local(self, mock_scholar):
        bq = mock.MagicMock()
        cache = mock.MagicMock()
        bq.search_crawled_authors.return_value = [_make_author("id1")]
        bq.search_coauthor_network.return_value = []
        cache.get.return_value = None
        # Scholar returns same author as local + a new one
        mock_scholar.search_scholar.return_value = [
            _make_author("id1", "Duplicate"),
            _make_author("id2", "New Author"),
        ]

        svc = AuthorSearchService(bq_client=bq, cache=cache)
        results = svc.search("Author")

        ids = [r["scholar_id"] for r in results]
        assert ids.count("id1") == 1
        assert "id2" in ids
