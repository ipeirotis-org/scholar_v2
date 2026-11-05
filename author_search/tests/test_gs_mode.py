"""Tests for DATA_SOURCE=gs author search (profile URL/ID lookup, no S2 fallback)."""

from unittest import mock

import pytest

import author_search.search_service as ss
from author_search.bigquery_client import BigQuerySearchClient
from author_search.config import Config
from author_search.search_service import AuthorSearchService, parse_gs_profile_id


@pytest.fixture
def gs_mode(monkeypatch):
    monkeypatch.setattr(Config, "IS_GS", True)
    monkeypatch.setattr(Config, "BQ_STATS_DATASET", "gs_statistics")
    with mock.patch("author_search.search_service._ensure_index_loaded"):
        yield


class TestParseGsProfileId:
    @pytest.mark.parametrize("text,expected", [
        ("https://scholar.google.com/citations?user=PA9La6oAAAAJ&hl=en", "PA9La6oAAAAJ"),
        ("scholar.google.com/citations?hl=en&user=oi2tUDZypQwC", "oi2tUDZypQwC"),
        ("PA9La6oAAAAJ", "PA9La6oAAAAJ"),
        ("evnr-MwAAAAJ", "evnr-MwAAAAJ"),
        ("  Av4MFGAAAAAJ ", "Av4MFGAAAAAJ"),
        ("Christianson", None),   # 12-letter name
        ("panos ipeirotis", None),
        ("PA9La6oAAAA", None),    # 11 chars
    ])
    def test_parse(self, text, expected):
        assert parse_gs_profile_id(text) == expected


class TestGsSearch:
    def _svc(self):
        return AuthorSearchService(bq_client=mock.MagicMock(), cache=mock.MagicMock(**{"get_search_results.return_value": None}))

    def test_pasted_url_returns_indexed_author(self, gs_mode, monkeypatch):
        monkeypatch.setattr(ss, "_author_index", [{"scholar_id": "PA9La6oAAAAJ", "name": "Panos Ipeirotis", "name_lower": "panos ipeirotis"}])
        r = self._svc().search("https://scholar.google.com/citations?user=PA9La6oAAAAJ")
        assert r == [{"scholar_id": "PA9La6oAAAAJ", "name": "Panos Ipeirotis", "name_lower": "panos ipeirotis", "source": "gs_id"}]

    def test_pasted_unknown_id_returns_stub(self, gs_mode, monkeypatch):
        monkeypatch.setattr(ss, "_author_index", [{"scholar_id": "x", "name": "X", "name_lower": "x"}])
        r = self._svc().search("oi2tUDZypQwC")
        assert r[0]["scholar_id"] == "oi2tUDZypQwC" and r[0]["source"] == "gs_id"

    def test_no_s2_fallback(self, gs_mode, monkeypatch):
        monkeypatch.setattr(ss, "_author_index", [{"scholar_id": "x", "name": "Someone Else", "name_lower": "someone else"}])
        with mock.patch("author_search.s2_client.search_authors") as s2:
            assert self._svc().search("Nobody Here", scholar=True) == []
        s2.assert_not_called()


class TestGsIndexQuery:
    def test_index_reads_gs_view_without_thresholds(self, gs_mode):
        client = BigQuerySearchClient.__new__(BigQuerySearchClient)
        client._query = mock.MagicMock(return_value=[])
        client.get_all_author_names()
        sql = client._query.call_args[0][0]
        assert "gs_statistics.ranked_author_current`" in sql
        assert "hindex >=" not in sql


class TestWikidataIndex:
    def _client(self, profiled, wikidata, coauthors=()):
        client = BigQuerySearchClient.__new__(BigQuerySearchClient)
        client._query = mock.MagicMock(side_effect=[profiled, wikidata, list(coauthors)])
        return client

    def test_index_adds_unprofiled_wikidata_authors(self, gs_mode):
        client = self._client([{"scholar_id": "A", "name": "Profiled"}],
                              [{"scholar_id": "iGzDl8IAAAAJ", "name": "Peyman Milanfar", "source": "wikidata"}])
        names = [a["name"] for a in client.get_all_author_names()]
        assert names == ["Profiled", "Peyman Milanfar"]
        wiki_sql = client._query.call_args_list[1][0][0]
        assert "gs_data.wikidata_authors`" in wiki_sql
        assert "NOT IN" in wiki_sql and "ranked_author_current`" in wiki_sql
        assert "gs_data.retired_profiles`" in wiki_sql  # deleted profiles are not offered

    def test_coauthors_added_and_merged_with_wikidata(self, gs_mode):
        client = self._client(
            [{"scholar_id": "A", "name": "Profiled"}],
            [{"scholar_id": "W", "name": "Both Places", "affiliation": "American computer scientist",
              "sitelinks": 3, "source": "wikidata"}],
            [{"scholar_id": "W", "name": "Both", "affiliation": "Professor, NYU", "coauthor_of": 4,
              "source": "coauthor"},
             {"scholar_id": "C", "name": "Only Coauthor", "affiliation": None, "coauthor_of": 2,
              "source": "coauthor"}])
        index = {a["scholar_id"]: a for a in client.get_all_author_names()}
        assert set(index) == {"A", "W", "C"}
        assert index["W"]["affiliation"] == "Professor, NYU" and index["W"]["source"] == "wikidata"
        assert index["W"]["coauthor_of"] == 4 and index["W"]["name"] == "Both Places"
        assert index["C"]["source"] == "coauthor"
        co_sql = client._query.call_args_list[2][0][0]
        assert "UNNEST(a.coauthors)" in co_sql and "ranked_author_current`" in co_sql
        assert "gs_data.retired_profiles`" in co_sql

    def test_failed_coauthor_query_keeps_the_rest(self, gs_mode):
        client = self._client([{"scholar_id": "A", "name": "Profiled"}],
                              [{"scholar_id": "W", "name": "Wiki", "source": "wikidata"}], [])
        assert [a["scholar_id"] for a in client.get_all_author_names()] == ["A", "W"]

    def test_coauthors_rank_after_wikidata_then_by_coauthor_count(self, gs_mode, monkeypatch):
        monkeypatch.setattr(ss, "_author_index", [
            {"scholar_id": "C1", "name": "Ann Lee", "name_lower": "ann lee", "source": "coauthor", "coauthor_of": 1},
            {"scholar_id": "C5", "name": "Ann Lee", "name_lower": "ann lee", "source": "coauthor", "coauthor_of": 5},
            {"scholar_id": "W1", "name": "Ann Lee", "name_lower": "ann lee", "source": "wikidata", "sitelinks": 1},
            {"scholar_id": "P0", "name": "Ann Lee", "name_lower": "ann lee", "citedby": 0},
        ])
        results = ss._search_in_memory("ann lee")
        assert [r["scholar_id"] for r in results] == ["P0", "W1", "C5", "C1"]
        assert results[2]["source"] == "coauthor"

    def test_wikidata_without_sitelinks_still_ranks_before_coauthors(self, gs_mode, monkeypatch):
        monkeypatch.setattr(ss, "_author_index", [
            {"scholar_id": "C9", "name": "Ann Lee", "name_lower": "ann lee", "source": "coauthor", "coauthor_of": 9},
            {"scholar_id": "W0", "name": "Ann Lee", "name_lower": "ann lee", "source": "wikidata", "sitelinks": None},
        ])
        assert [r["scholar_id"] for r in ss._search_in_memory("ann lee")] == ["W0", "C9"]

    def test_no_index_from_wikidata_alone(self, gs_mode):
        client = self._client([], [{"scholar_id": "x", "name": "Only Wikidata"}])
        assert client.get_all_author_names() == []

    def test_unprofiled_authors_rank_after_profiled_ones(self, gs_mode, monkeypatch):
        monkeypatch.setattr(ss, "_author_index", [
            {"scholar_id": "W1", "name": "Ann Lee", "name_lower": "ann lee", "source": "wikidata", "sitelinks": 2},
            {"scholar_id": "W2", "name": "Ann Lee", "name_lower": "ann lee", "source": "wikidata", "sitelinks": 9},
            {"scholar_id": "P1", "name": "Ann Lee", "name_lower": "ann lee", "citedby": 10},
        ])
        results = ss._search_in_memory("ann lee")
        assert [r["scholar_id"] for r in results] == ["P1", "W2", "W1"]
        assert [r["source"] for r in results] == ["index", "wikidata", "wikidata"]

    def test_profiled_author_without_citations_still_ranks_first(self, gs_mode, monkeypatch):
        monkeypatch.setattr(ss, "_author_index", [
            {"scholar_id": "W1", "name": "Ann Lee", "name_lower": "ann lee", "source": "wikidata", "sitelinks": 30},
            {"scholar_id": "P0", "name": "Ann Lee", "name_lower": "ann lee", "citedby": 0},
        ])
        assert [r["scholar_id"] for r in ss._search_in_memory("ann lee")] == ["P0", "W1"]

    def test_pasted_url_of_wikidata_author_shows_name(self, gs_mode, monkeypatch):
        monkeypatch.setattr(ss, "_author_index", [
            {"scholar_id": "iGzDl8IAAAAJ", "name": "Peyman Milanfar", "name_lower": "peyman milanfar",
             "source": "wikidata"}])
        svc = AuthorSearchService(bq_client=mock.MagicMock(), cache=mock.MagicMock())
        r = svc.search("https://scholar.google.com/citations?hl=en&user=iGzDl8IAAAAJ")
        assert r[0]["name"] == "Peyman Milanfar" and r[0]["source"] == "gs_id"


class TestIndexChunks:
    def test_stale_chunks_from_a_larger_index_are_deleted(self, monkeypatch):
        monkeypatch.setattr(ss, "_CHUNK_SIZE", 2)
        stored = {f"chunk_{i}": ["old"] for i in range(5)}
        cache = mock.MagicMock()
        cache.set_index_chunk.side_effect = lambda c, d, data: stored.__setitem__(d, data)
        cache.get_index_chunk.side_effect = lambda c, d: stored.get(d)
        cache.delete_index_chunk.side_effect = lambda c, d: stored.pop(d) is not None
        ss._save_index_to_firestore(cache, [{"n": 1}, {"n": 2}, {"n": 3}])
        assert sorted(stored) == ["chunk_0", "chunk_1"]

    def test_failed_delete_does_not_loop(self, monkeypatch):
        cache = mock.MagicMock()
        cache.get_index_chunk.return_value = ["old"]
        cache.delete_index_chunk.return_value = False
        ss._save_index_to_firestore(cache, [{"n": 1}])
        assert cache.delete_index_chunk.call_count == 1
