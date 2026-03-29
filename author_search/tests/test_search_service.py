"""Tests for AuthorSearchService — in-memory index search."""

from unittest import mock

import pytest

from author_search.search_service import AuthorSearchService


@pytest.fixture(autouse=True)
def _reset_author_index():
    """Reset module-level index state and mock _ensure_index_loaded to avoid
    Firestore calls (get_index_chunk on MagicMock causes infinite loop)."""
    import author_search.search_service as ss
    ss._bootstrap_triggered = False
    with mock.patch("author_search.search_service._ensure_index_loaded"):
        yield
    ss._bootstrap_triggered = False


def _make_author(scholar_id, name="Test Author", source=None, citedby=100, hindex=10):
    author = {
        "scholar_id": scholar_id,
        "name": name,
        "affiliation": "Test University",
        "email_domain": "",
        "citedby": citedby,
        "hindex": hindex,
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


class TestTypeaheadSearch:
    @mock.patch("author_search.search_service._search_in_memory")
    def test_typeahead_uses_in_memory(self, mock_search):
        mock_search.return_value = [_make_author("id1", source="index")]
        cache = mock.MagicMock()
        svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
        results = svc.search("Author", typeahead=True)

        assert len(results) == 1
        mock_search.assert_called_once_with("Author", limit=10)
        # Typeahead should not check or set Firestore cache
        cache.get_search_results.assert_not_called()
        cache.set_search_results.assert_not_called()

    @mock.patch("author_search.search_service._search_in_memory", return_value=None)
    def test_typeahead_returns_empty_when_index_not_loaded(self, mock_search):
        svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=mock.MagicMock())
        results = svc.search("Author", typeahead=True)
        assert results == []


class TestFullSearch:
    @mock.patch("author_search.search_service._search_in_memory")
    def test_returns_results_from_index(self, mock_search):
        mock_search.return_value = [
            _make_author(f"id{i}", f"Author {i}") for i in range(5)
        ]
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
        results = svc.search("Author")

        assert len(results) == 5
        mock_search.assert_called_once_with("Author", limit=50)

    @mock.patch("author_search.search_service._search_in_memory")
    def test_caches_results(self, mock_search):
        mock_search.return_value = [_make_author("id1")]
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
        svc.search("Author")

        cache.set_search_results.assert_called_once()

    @mock.patch("author_search.search_service._search_in_memory", return_value=None)
    def test_does_not_cache_when_index_not_loaded(self, mock_search):
        """During bootstrap, don't cache empty results for 24h."""
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
        results = svc.search("Author")

        assert results == []
        cache.set_search_results.assert_not_called()

    def test_uses_cached_results(self):
        cache = mock.MagicMock()
        cached = [_make_author("cached1", source="index")]
        cache.get_search_results.return_value = cached
        svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
        results = svc.search("Author")

        assert results == cached

    @mock.patch("author_search.search_service._search_in_memory")
    def test_scholar_flag_triggers_s2_fallback_when_few_results(self, mock_search):
        """scholar=True queries S2 API when index returns few results."""
        mock_search.return_value = [_make_author("id1")]  # Only 1 result < 5
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        cache.get.return_value = None  # No cached S2 results

        with mock.patch("author_search.s2_client.search_authors") as mock_s2:
            mock_s2.return_value = [_make_author("s2_1", "S2 Author")]
            svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
            results = svc.search("Author", scholar=True)

        assert len(results) == 2
        ids = [r["scholar_id"] for r in results]
        assert "id1" in ids
        assert "s2_1" in ids

    @mock.patch("author_search.search_service._search_in_memory")
    def test_scholar_flag_skips_s2_when_enough_results(self, mock_search):
        """scholar=True skips S2 API when index returns enough results."""
        mock_search.return_value = [_make_author(f"id{i}") for i in range(10)]
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None

        with mock.patch("author_search.s2_client.search_authors") as mock_s2:
            svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
            results = svc.search("Author", scholar=True)

        mock_s2.assert_not_called()
        assert len(results) == 10

    @mock.patch("author_search.search_service._search_in_memory")
    def test_auto_s2_fallback_when_zero_local_results(self, mock_search):
        """Default search (scholar=False) auto-falls back to S2 API on 0 results."""
        mock_search.return_value = []  # No local results
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        cache.get.return_value = None

        with mock.patch("author_search.s2_client.search_authors") as mock_s2:
            mock_s2.return_value = [_make_author("2508783", "T. Cowen")]
            svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
            results = svc.search("T. Cowen")

        mock_s2.assert_called_once()
        assert len(results) == 1
        assert results[0]["scholar_id"] == "2508783"

    @mock.patch("author_search.search_service._search_in_memory")
    def test_no_auto_s2_fallback_when_local_results_exist(self, mock_search):
        """Default search does NOT fall back to S2 API when local results exist."""
        mock_search.return_value = [_make_author("id1")]  # 1 result (> 0)
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None

        with mock.patch("author_search.s2_client.search_authors") as mock_s2:
            svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
            results = svc.search("Author")

        mock_s2.assert_not_called()
        assert len(results) == 1


class TestBootstrap:
    """Test automatic index bootstrap when Firestore index is missing."""

    @pytest.fixture(autouse=True)
    def _reset_author_index(self):
        """Override the module-level autouse fixture — do NOT mock
        _ensure_index_loaded, because we're testing it directly."""
        import author_search.search_service as ss
        old_index, old_at, old_flag = ss._author_index, ss._index_loaded_at, ss._bootstrap_triggered
        ss._author_index = []
        ss._index_loaded_at = 0
        ss._bootstrap_triggered = False
        yield
        ss._author_index, ss._index_loaded_at, ss._bootstrap_triggered = old_index, old_at, old_flag

    def test_triggers_background_rebuild_when_index_missing(self):
        import author_search.search_service as ss

        cache = mock.MagicMock()
        cache.get_index_chunk.return_value = None

        with mock.patch("author_search.search_service._bootstrap_index_async") as mock_bootstrap:
            ss._ensure_index_loaded(cache, bq=mock.MagicMock())
            mock_bootstrap.assert_called_once()
            assert ss._bootstrap_triggered is True

    def test_only_triggers_once(self):
        import author_search.search_service as ss
        ss._bootstrap_triggered = True

        cache = mock.MagicMock()
        cache.get_index_chunk.return_value = None

        with mock.patch("author_search.search_service._bootstrap_index_async") as mock_bootstrap:
            ss._ensure_index_loaded(cache, bq=mock.MagicMock())
            mock_bootstrap.assert_not_called()


class TestSearchInMemory:
    """Test the _search_in_memory function directly."""

    def test_matches_all_tokens(self):
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "John Smith", "name_lower": "john smith",
                 "affiliation": "MIT", "citedby": 100, "hindex": 10},
                {"scholar_id": "2", "name": "Jane Smith", "name_lower": "jane smith",
                 "affiliation": "Stanford", "citedby": 200, "hindex": 15},
                {"scholar_id": "3", "name": "John Doe", "name_lower": "john doe",
                 "affiliation": "Harvard", "citedby": 50, "hindex": 5},
            ]
            results = ss._search_in_memory("john smith")
            assert len(results) == 1
            assert results[0]["scholar_id"] == "1"
        finally:
            ss._author_index = old_index

    def test_sorted_by_citations(self):
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "Smith A", "name_lower": "smith a",
                 "affiliation": "", "citedby": 50, "hindex": 5},
                {"scholar_id": "2", "name": "Smith B", "name_lower": "smith b",
                 "affiliation": "", "citedby": 200, "hindex": 15},
            ]
            results = ss._search_in_memory("smith")
            assert results[0]["scholar_id"] == "2"
            assert results[1]["scholar_id"] == "1"
        finally:
            ss._author_index = old_index

    def test_returns_hindex(self):
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "Test", "name_lower": "test",
                 "affiliation": "", "citedby": 100, "hindex": 25},
            ]
            results = ss._search_in_memory("test")
            assert results[0]["hindex"] == 25
        finally:
            ss._author_index = old_index

    def test_initial_to_full_name_matching(self):
        """Searching 'Tyler Cowen' should match 'T. Cowen'."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "2508783", "name": "T. Cowen", "name_lower": "t. cowen",
                 "affiliation": "George Mason University", "citedby": 5000, "hindex": 20},
            ]
            results = ss._search_in_memory("Tyler Cowen")
            assert len(results) == 1
            assert results[0]["scholar_id"] == "2508783"
        finally:
            ss._author_index = old_index

    def test_initial_query_matches_full_name(self):
        """Searching 'T. Cowen' should match 'Tyler Cowen'."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "Tyler Cowen", "name_lower": "tyler cowen",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("T. Cowen")
            assert len(results) == 1
            assert results[0]["scholar_id"] == "1"
        finally:
            ss._author_index = old_index

    def test_initial_without_period_matches(self):
        """Searching 'T Cowen' should match 'T. Cowen'."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "T. Cowen", "name_lower": "t. cowen",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("T Cowen")
            assert len(results) == 1
        finally:
            ss._author_index = old_index

    def test_initial_matching_does_not_over_match(self):
        """'Tyler' should NOT match initial 'j.' (different first letter)."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "J. Cowen", "name_lower": "j. cowen",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("Tyler Cowen")
            assert len(results) == 0
        finally:
            ss._author_index = old_index
