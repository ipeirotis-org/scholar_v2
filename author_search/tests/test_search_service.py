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
    def test_scholar_flag_triggers_s2_fallback_when_few_results(self, mock_search, monkeypatch):
        """scholar=True queries S2 API when index returns few results (fallback enabled)."""
        monkeypatch.setattr("author_search.config.Config.S2_SEARCH_FALLBACK", True)
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
    def test_auto_s2_fallback_when_zero_local_results(self, mock_search, monkeypatch):
        """Default search (scholar=False) auto-falls back to S2 API on 0 results (fallback enabled)."""
        monkeypatch.setattr("author_search.config.Config.S2_SEARCH_FALLBACK", True)
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
    def test_s2_fallback_off_by_default(self, mock_search):
        """No live S2 API calls unless S2_SEARCH_FALLBACK is enabled."""
        mock_search.return_value = []
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None

        with mock.patch("author_search.s2_client.search_authors") as mock_s2:
            svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
            assert svc.search("T. Cowen", scholar=True) == []

        mock_s2.assert_not_called()

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

    @mock.patch("author_search.search_service._search_in_memory")
    def test_empty_results_not_cached(self, mock_search):
        """Empty results (e.g., S2 API failure) should not be cached for 24h."""
        mock_search.return_value = []
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        cache.get.return_value = None

        with mock.patch("author_search.s2_client.search_authors") as mock_s2:
            mock_s2.return_value = []  # S2 API also returns nothing (timeout/error)
            svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
            results = svc.search("Unknown Author")

        assert results == []
        cache.set_search_results.assert_not_called()


class TestS2FallbackWithSuggestions:
    """Approximate suggestions mean the name was not found: they must not hold back the S2 lookup."""

    FUZZY = [dict(_make_author(f"id{i}", f"Hortan{i} Smith"), fuzzy=True) for i in range(5)]

    def _search(self, monkeypatch, local, **kwargs):
        monkeypatch.setattr("author_search.config.Config.S2_SEARCH_FALLBACK", True)
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        cache.get.return_value = None
        svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
        with mock.patch("author_search.search_service._search_in_memory", return_value=list(local)), \
             mock.patch("author_search.s2_client.search_authors",
                        return_value=[_make_author("s2id", "Hortun Smith")]) as s2:
            return svc.search("hortun smith", **kwargs), s2

    def test_five_suggestions_still_reach_s2_on_explicit_search(self, monkeypatch):
        results, s2 = self._search(monkeypatch, self.FUZZY, scholar=True)
        s2.assert_called_once()
        assert [r["scholar_id"] for r in results][-1] == "s2id" and len(results) == 6

    def test_only_suggestions_fall_back_like_no_results(self, monkeypatch):
        results, s2 = self._search(monkeypatch, self.FUZZY[:2])
        s2.assert_called_once()
        assert "s2id" in [r["scholar_id"] for r in results]

    def test_suggestions_alone_are_not_cached(self, monkeypatch):
        """If S2 fails, caching the suggestions would keep the name from S2 for a day."""
        monkeypatch.setattr("author_search.config.Config.S2_SEARCH_FALLBACK", True)
        cache = mock.MagicMock()
        cache.get_search_results.return_value = None
        cache.get.return_value = None
        svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)
        with mock.patch("author_search.search_service._search_in_memory", return_value=list(self.FUZZY)), \
             mock.patch("author_search.s2_client.search_authors", return_value=[]):
            assert len(svc.search("hortun smith")) == 5
        cache.set_search_results.assert_not_called()

    def test_five_exact_matches_still_skip_s2(self, monkeypatch):
        exact = [_make_author(f"id{i}", "Hortun Smith") for i in range(5)]
        results, s2 = self._search(monkeypatch, exact, scholar=True)
        s2.assert_not_called()
        assert len(results) == 5


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

    def test_all_initial_matches_rejected(self):
        """'Tyler Cowen' should NOT match 'T. C. Smith' (all-initial, no substring)."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "T. C. Smith", "name_lower": "t. c. smith",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("Tyler Cowen")
            assert len(results) == 0
        finally:
            ss._author_index = old_index

    def test_middle_initial_not_matched_by_wrong_position(self):
        """'Tyler Cowen' should NOT match 'Steven T. Cowen' (initial at wrong position)."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "Steven T. Cowen", "name_lower": "steven t. cowen",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("Tyler Cowen")
            assert len(results) == 0
        finally:
            ss._author_index = old_index

    def test_multi_initial_positional_matching(self):
        """'Joanne Kathleen Rowling' should match 'J. K. Rowling'."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "J. K. Rowling", "name_lower": "j. k. rowling",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("Joanne Kathleen Rowling")
            assert len(results) == 1
        finally:
            ss._author_index = old_index

    def test_initial_expansion_rejects_uncovered_name_words(self):
        """'Tyler Cowen' should NOT match 'Tyler C. Sloan' (sloan uncovered)."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "Tyler C. Sloan", "name_lower": "tyler c. sloan",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("Tyler Cowen")
            assert len(results) == 0
        finally:
            ss._author_index = old_index

    def test_substring_only_match_allows_extra_name_words(self):
        """'Smith' should still match 'Tyler C. Smith' (pure substring, no initials)."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "Tyler C. Smith", "name_lower": "tyler c. smith",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("Smith")
            assert len(results) == 1
        finally:
            ss._author_index = old_index

    def test_initial_search_matches_name_with_middle_name(self):
        """'J. Smith' should match 'John Adam Smith' (middle name allowed)."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "John Adam Smith", "name_lower": "john adam smith",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("J. Smith")
            assert len(results) == 1
        finally:
            ss._author_index = old_index

    def test_single_letter_token_does_not_cover_unrelated_words(self):
        """'J Smith' should NOT match 'John S. Jones' (j covers jones is spurious)."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "John S. Jones", "name_lower": "john s. jones",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("J Smith")
            assert len(results) == 0
        finally:
            ss._author_index = old_index

    def test_partial_substring_does_not_cover_boundary_word(self):
        """'Amy Li' should NOT match 'A. Williams' (li is substring of williams)."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "A. Williams", "name_lower": "a. williams",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("Amy Li")
            assert len(results) == 0
        finally:
            ss._author_index = old_index

    def test_name_suffix_not_treated_as_boundary(self):
        """'Tyler Cowen' should match 'T. Cowen Jr.' (suffix ignored in boundary)."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "T. Cowen Jr.", "name_lower": "t. cowen jr.",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("Tyler Cowen")
            assert len(results) == 1
        finally:
            ss._author_index = old_index

    def test_initial_first_name_with_middle_name(self):
        """'Tyler Cowen' should match 'T. Adam Cowen' (middle name after initial)."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "T. Adam Cowen", "name_lower": "t. adam cowen",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("Tyler Cowen")
            assert len(results) == 1
        finally:
            ss._author_index = old_index

    def test_comma_formatted_suffix_name(self):
        """'Tyler Cowen' should match 'T. Cowen, Jr.' (comma on surname)."""
        import author_search.search_service as ss
        old_index = ss._author_index
        try:
            ss._author_index = [
                {"scholar_id": "1", "name": "T. Cowen, Jr.", "name_lower": "t. cowen, jr.",
                 "affiliation": "", "citedby": 100, "hindex": 10},
            ]
            results = ss._search_in_memory("Tyler Cowen")
            assert len(results) == 1
        finally:
            ss._author_index = old_index


@pytest.fixture
def index(monkeypatch):
    """Install a list of names (or author dicts) as the in-memory index."""
    import author_search.search_service as ss

    def install(*authors):
        entries = [a if isinstance(a, dict) else _make_author(f"id{i}", a, citedby=1000 - i)
                   for i, a in enumerate(authors)]
        monkeypatch.setattr(ss, "_author_index", entries)
        monkeypatch.setattr(ss, "_compiled", None)
        return ss._search_in_memory
    return install


def _names(results):
    return [r["name"] for r in results]


class TestFuzzyMatching:
    """Approximate matches, returned only when no name matches exactly.

    The queries are searches that returned nothing on the live site in
    October-November 2026.
    """

    @pytest.mark.parametrize("query,expected", [
        ("viral acharaya", "Viral Acharya"),             # extra letter
        ("arun subdararajan", "Arun Sundararajan"),      # wrong letter
        ("marios papachrisou", "Marios Papachristou"),   # missing letter
        ("ioannis daras", "Giannis Daras"),              # variant of the first name
        ("john hortno", "John Horton"),                  # swapped letters
        ("robert f. engle", "Robert Engle"),             # middle initial the profile lacks
        ("prasanna parasyr", "Prasanna Parasurama"),     # typo in a half-typed word
        ("tuhun", "Tuhin Chakrabarty"),                  # single word
    ])
    def test_finds_the_intended_author(self, index, query, expected):
        search = index("Viral Acharya", "Arun Sundararajan", "Marios Papachristou", "Giannis Daras",
                       "John Horton", "Robert Engle", "Prasanna Parasurama", "Tuhin Chakrabarty",
                       "Maria Garcia", "Wei Zhang")
        results = search(query)
        assert _names(results) == [expected]
        assert results[0]["fuzzy"] is True

    def test_exact_matches_are_not_mixed_with_similar_names(self, index):
        search = index("Viral Acharya", "Viral Acharaya")
        results = search("viral acharaya")
        assert _names(results) == ["Viral Acharaya"]
        assert "fuzzy" not in results[0]

    def test_closest_name_first_then_by_citations(self, index):
        search = index(_make_author("far", "Tomas Hartens", citedby=9000),
                       _make_author("near_low", "Tom Hortan", citedby=10),
                       _make_author("near_high", "Tim Hortin", citedby=500))
        assert [r["scholar_id"] for r in search("horton")] == ["near_high", "near_low", "far"]

    def test_short_words_must_match_exactly(self, index):
        search = index("Wei Li", "Wen Lu")
        assert search("wei lo") == []

    def test_every_word_must_match_something(self, index):
        search = index("Viral Acharya", "Viral Shah")
        assert _names(search("viral acharaya")) == ["Viral Acharya"]
        assert search("journal of fluid mechanics") == []

    def test_unrelated_query_finds_nothing(self, index):
        search = index("Viral Acharya", "John Horton")
        assert search("zzzzqq") == []
        assert search("j. k.") == []

    def test_at_most_five_suggestions(self, index):
        search = index(*[f"Hortan{c} Smith" for c in "abcdefgh"])
        assert len(search("hortun", limit=50)) == 5

    def test_hyphenated_surname_parts(self, index):
        search = index("Carlos Carranza-Torres")
        assert _names(search("carlos torrez")) == ["Carlos Carranza-Torres"]

    def test_without_rapidfuzz_search_still_works(self, index, monkeypatch):
        import sys
        monkeypatch.setitem(sys.modules, "rapidfuzz", None)
        search = index("Viral Acharya")
        assert search("viral acharaya") == []
        assert _names(search("viral")) == ["Viral Acharya"]


class TestAccentFolding:
    def test_unaccented_query_finds_accented_name(self, index):
        search = index("Gülsün Tünay", "José Peña", "Søren Łukasz Straße")
        assert _names(search("gulsun tunay")) == ["Gülsün Tünay"]
        assert _names(search("jose pena")) == ["José Peña"]
        assert _names(search("soren lukasz strasse")) == ["Søren Łukasz Straße"]
        assert "fuzzy" not in search("jose pena")[0]

    def test_accented_query_finds_unaccented_name(self, index):
        search = index("Jose Pena")
        assert _names(search("José Peña")) == ["Jose Pena"]

    def test_comma_in_query_is_ignored(self, index):
        search = index("John J. Horton")
        assert _names(search("john j, horton")) == ["John J. Horton"]


class TestCompiledIndex:
    def test_same_results_as_checking_every_name(self, index):
        """The candidate lookup must find exactly the names the per-name rule accepts."""
        import random
        import author_search.search_service as ss
        rng = random.Random(3)
        first = ["John", "J.", "Tyler", "T.", "Ann", "A", "Li", "Wei", "Maria", "M.", "Joan", "Jo"]
        last = ["Smith", "Cowen", "Li", "Lee", "Williams", "Johnson", "Jones", "Sloan", "Wei", "Smithson Jr."]
        names = [" ".join(rng.choice(first) for _ in range(rng.randint(1, 3))) + " " + rng.choice(last)
                 for _ in range(400)]
        search = index(*names)
        queries = set()
        for name in rng.sample(names, 120):
            words = name.lower().split()
            queries |= {name.lower(), words[-1], words[0] + " " + words[-1], words[0][0] + " " + words[-1],
                        words[0][0] + ". " + words[-1], name.lower()[:rng.randint(2, len(name))].strip(),
                        " ".join(w[0] for w in words)}
        for query in sorted(q for q in queries if q):
            tokens = query.split()
            expected = [a for a in ss._author_index if ss._entry_matches(tokens, a["name"].lower())]
            expected.sort(key=ss._rank_key, reverse=True)
            got = search(query, limit=1000)
            if expected:
                assert [r["scholar_id"] for r in got] == [a["scholar_id"] for a in expected], query
            else:
                assert all(r.get("fuzzy") for r in got), query

    def test_results_follow_a_new_index(self, index):
        assert _names(index("Ann Lee")("ann")) == ["Ann Lee"]
        assert _names(index("Ann Wu")("ann")) == ["Ann Wu"]

    def test_repeated_query_is_served_from_memory(self, index):
        import author_search.search_service as ss
        search = index("Ann Lee", "Ann Wu")
        first = search("ann")
        with mock.patch.object(ss._CompiledIndex, "exact", side_effect=AssertionError("recomputed")):
            again = search("ANN ")
        assert again == first and again is not first
        again[0]["name"] = "changed"
        assert search("ann")[0]["name"] == "Ann Lee"

    def test_result_cache_is_bounded(self, index, monkeypatch):
        import author_search.search_service as ss
        monkeypatch.setattr(ss, "_RESULT_CACHE_SIZE", 3)
        search = index("Ann Lee")
        for q in ("an", "ann", "le", "lee", "nn"):
            search(q)
        assert len(ss._compiled._results) == 3

    def test_name_with_line_break_does_not_shift_other_names(self, index):
        search = index("Ann\nLee", "Bob Stone")
        assert _names(search("bob stone")) == ["Bob Stone"]
        assert _names(search("ann lee")) == ["Ann\nLee"]
