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
        monkeypatch.setattr(Config, "GS_LIVE_SEARCH", False)
        monkeypatch.setattr(ss, "_author_index", [{"scholar_id": "x", "name": "Someone Else", "name_lower": "someone else"}])
        with mock.patch("author_search.s2_client.search_authors") as s2:
            assert self._svc().search("Nobody Here", scholar=True) == []
        s2.assert_not_called()

    def test_index_results_skip_the_firestore_cache(self, gs_mode, monkeypatch):
        """An index search takes milliseconds; a Firestore read would be slower."""
        monkeypatch.setattr(ss, "_author_index", [{"scholar_id": "x", "name": "Ann Lee"}])
        svc = self._svc()
        assert [r["scholar_id"] for r in svc.search("ann lee")] == ["x"]
        svc.cache.get_search_results.assert_not_called()
        svc.cache.set_search_results.assert_not_called()


LIVE = {"scholar_id": "OgG4DH0AAAAJ", "name": "Ilias Triantafyllopoulos",
        "affiliation": "PhD Student, New York University", "email_domain": "stern.nyu.edu",
        "citedby": 21, "hindex": None, "source": "gs_live"}


class TestGsLiveSearch:
    """scholar=True looks names up on Google Scholar (paid, so cached and capped)."""

    @pytest.fixture(autouse=True)
    def _index(self, gs_mode, monkeypatch):
        monkeypatch.setattr(Config, "GS_LIVE_SEARCH", True)
        monkeypatch.setattr(ss, "_author_index", [
            {"scholar_id": "PA9La6oAAAAJ", "name": "Panos Ipeirotis", "citedby": 30000, "hindex": 60},
            {"scholar_id": "WIKI00000001", "name": "Ilias Wiki", "source": "wikidata"},
        ])

    def _svc(self, cached=None, allowed=True):
        cache = mock.MagicMock()
        cache.get.return_value = cached
        cache.reserve_live_search.return_value = allowed
        return AuthorSearchService(bq_client=mock.MagicMock(), cache=cache)

    def test_looks_up_and_caches(self):
        svc = self._svc()
        with mock.patch("author_search.gs_search_client.search_profiles", return_value=[dict(LIVE)]) as live:
            results = svc.search("Ilias Triantafyllopoulos", scholar=True)
        live.assert_called_once_with("Ilias Triantafyllopoulos")
        assert results == [LIVE]
        svc.cache.set.assert_called_once_with("gs:Ilias Triantafyllopoulos", [LIVE])
        assert svc.cache.get.call_args == mock.call("gs:Ilias Triantafyllopoulos",
                                                    ttl_hours=Config.GS_LIVE_CACHE_HOURS)

    def test_not_without_scholar_flag(self):
        svc = self._svc()
        with mock.patch("author_search.gs_search_client.search_profiles") as live:
            assert svc.search("Ilias Triantafyllopoulos") == []
            assert svc.search("Ilias Triantafyllopoulos", typeahead=True) == []
        live.assert_not_called()
        svc.cache.reserve_live_search.assert_not_called()

    def test_not_for_automated_traffic(self):
        svc = self._svc()
        with mock.patch("author_search.gs_search_client.search_profiles") as live:
            assert svc.search("Ilias Triantafyllopoulos", scholar=True, allow_live=False) == []
        live.assert_not_called()

    def test_cached_lookup_costs_nothing(self):
        svc = self._svc(cached=[dict(LIVE)])
        with mock.patch("author_search.gs_search_client.search_profiles") as live:
            assert svc.search("Ilias Triantafyllopoulos", scholar=True) == [LIVE]
        live.assert_not_called()
        svc.cache.reserve_live_search.assert_not_called()

    def test_cached_empty_lookup_is_not_repeated(self):
        svc = self._svc(cached=[])
        with mock.patch("author_search.gs_search_client.search_profiles") as live:
            assert svc.search("Nobody Atall", scholar=True) == []
        live.assert_not_called()

    def test_empty_lookup_is_cached(self):
        svc = self._svc()
        with mock.patch("author_search.gs_search_client.search_profiles", return_value=[]):
            assert svc.search("Nobody Atall", scholar=True) == []
        svc.cache.set.assert_called_once_with("gs:Nobody Atall", [])

    def test_failed_lookup_is_not_cached(self):
        svc = self._svc()
        with mock.patch("author_search.gs_search_client.search_profiles", return_value=None):
            assert svc.search("Nobody Atall", scholar=True) == []
        svc.cache.set.assert_not_called()

    def test_daily_limit_stops_lookups(self):
        svc = self._svc(allowed=False)
        with mock.patch("author_search.gs_search_client.search_profiles") as live:
            assert svc.search("Ilias Triantafyllopoulos", scholar=True) == []
        live.assert_not_called()
        svc.cache.reserve_live_search.assert_called_once_with(Config.GS_LIVE_SEARCH_DAILY_LIMIT)

    @pytest.mark.parametrize("query", [
        "https://www.researchgate.net/profile/Alessandro-Calvi-2/stats",
        "12345", "a b c d e f g", "x" * 90,
    ])
    def test_only_names_are_looked_up(self, query):
        svc = self._svc()
        with mock.patch("author_search.gs_search_client.search_profiles") as live:
            svc.search(query, scholar=True)
        live.assert_not_called()

    @pytest.mark.parametrize("query", ["Syropoulos", "Ιλίας Τριανταφυλλόπουλος", "Jean-Pierre O'Neil", "J. K. Rowling"])
    def test_names_in_any_script_are_looked_up(self, query):
        assert ss._looks_like_name(query)

    def test_live_profiles_come_before_similar_index_names(self):
        svc = self._svc()
        found = [dict(LIVE, scholar_id="PA9La6oAAAAJ", name="P Ipeirotis", citedby=1, affiliation="NYU"),
                 dict(LIVE)]
        with mock.patch("author_search.gs_search_client.search_profiles", return_value=found):
            results = svc.search("Panos Ipeirotiss", scholar=True)  # misspelled: the index match is fuzzy
        assert [r["scholar_id"] for r in results] == ["PA9La6oAAAAJ", "OgG4DH0AAAAJ"]
        # A profiled author keeps our name and numbers; the page fetches the other on demand.
        assert results[0]["name"] == "Panos Ipeirotis" and results[0]["citedby"] == 30000
        assert results[0]["hindex"] == 60 and results[1]["hindex"] is None
        assert {r["source"] for r in results} == {"gs_live"}

    def test_unprofiled_index_entry_keeps_google_scholar_numbers(self):
        svc = self._svc()
        found = [dict(LIVE, scholar_id="WIKI00000001", name="Ilias Wiki", citedby=77)]
        with mock.patch("author_search.gs_search_client.search_profiles", return_value=found):
            results = svc.search("Ilias Wikk", scholar=True)
        assert len(results) == 1 and results[0]["citedby"] == 77 and results[0]["hindex"] is None


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


class FakeIndexStore:
    """SearchCache's index-document methods over a dict."""

    def __init__(self, docs=None):
        self.docs = dict(docs or {})
        self.reads = []
        self.fail_writes = set()

    def get_index_chunk(self, collection, doc_id):
        self.reads.append(doc_id)
        return self.docs.get(doc_id)

    def set_index_chunk(self, collection, doc_id, data):
        if doc_id not in self.fail_writes:
            self.docs[doc_id] = data

    def delete_index_chunk(self, collection, doc_id):
        return self.docs.pop(doc_id, None) is not None


@pytest.fixture
def clean_index():
    """Restore the module's index state after a test that loads or swaps it."""
    saved = (ss._author_index, ss._compiled, ss._index_loaded_at, ss._index_version, ss._bootstrap_triggered)
    ss._author_index, ss._compiled, ss._index_loaded_at, ss._index_version = [], None, 0, None
    yield
    (ss._author_index, ss._compiled, ss._index_loaded_at, ss._index_version, ss._bootstrap_triggered) = saved


AUTHORS = [{"scholar_id": f"id{i:010d}", "name": f"Author Number{i}", "citedby": i, "hindex": i % 50,
            "affiliation": "Some University " * 3} for i in range(3000)]


class TestIndexStorage:
    """The index is saved as gzip-JSON parts under a version, with a meta document naming it."""

    def test_round_trip(self):
        store = FakeIndexStore()
        version = ss._save_index_to_firestore(store, [dict(a, name_lower="x", sitelinks=None) for a in AUTHORS])
        meta = store.docs["meta"]
        assert meta == {"format": "gzip-json", "version": version, "parts": meta["parts"], "count": 3000}
        assert sorted(store.docs) == sorted(["meta"] + [f"pack_{version}_{n}" for n in range(meta["parts"])])
        # Derived and empty fields are not stored.
        assert ss._load_index(store) == (AUTHORS, version)

    def test_large_index_is_cut_into_parts_below_the_document_limit(self, monkeypatch):
        monkeypatch.setattr(ss, "_PACK_PART_BYTES", 2000)
        store = FakeIndexStore()
        version = ss._save_index_to_firestore(store, AUTHORS)
        assert store.docs["meta"]["parts"] > 3
        assert all(len(v) <= 2000 for k, v in store.docs.items() if k != "meta")
        assert ss._load_index(store) == (AUTHORS, version)

    def test_previous_version_and_legacy_chunks_are_deleted(self):
        store = FakeIndexStore({f"chunk_{i}": [{"name": "old"}] for i in range(5)})
        first = ss._save_index_to_firestore(store, AUTHORS[:10])
        with mock.patch("time.time", return_value=4102444800.0):
            second = ss._save_index_to_firestore(store, AUTHORS[:20])
        assert first != second
        assert sorted(store.docs) == ["meta", f"pack_{second}_0"]
        assert ss._load_index(store) == (AUTHORS[:20], second)

    def test_failed_write_keeps_the_previous_index(self):
        store = FakeIndexStore()
        first = ss._save_index_to_firestore(store, AUTHORS[:10])
        with mock.patch("time.time", return_value=4102444800.0):
            store.fail_writes.add("pack_4102444800000_0")
            assert ss._save_index_to_firestore(store, AUTHORS[:20]) is None
        assert store.docs["meta"]["version"] == first
        assert ss._load_index(store) == (AUTHORS[:10], first)

    def test_failed_meta_write_keeps_the_previous_index(self):
        """The parts are written but the switch is not: the old parts must survive."""
        store = FakeIndexStore()
        first = ss._save_index_to_firestore(store, AUTHORS[:10])
        store.fail_writes.add("meta")
        with mock.patch("time.time", return_value=4102444800.0):
            assert ss._save_index_to_firestore(store, AUTHORS[:20]) is None
        assert ss._load_index(store) == (AUTHORS[:10], first)

    def test_failed_first_migration_keeps_the_legacy_chunks(self):
        chunks = {f"chunk_{i}": [{"scholar_id": f"c{i}", "name": f"Name {i}"}] for i in range(3)}
        store = FakeIndexStore(chunks)
        store.fail_writes.add("meta")
        assert ss._save_index_to_firestore(store, AUTHORS[:20]) is None
        assert [a["scholar_id"] for a in ss._load_index(store)[0]] == ["c0", "c1", "c2"]

    def test_unpublished_refresh_reports_no_authors(self, clean_index):
        store = FakeIndexStore()
        store.fail_writes.add("meta")
        bq = mock.MagicMock()
        bq.get_all_author_names.return_value = [dict(a) for a in AUTHORS[:5]]
        assert ss.refresh_author_index(bq=bq, cache=store) == 0

    def test_failed_delete_does_not_loop(self):
        cache = mock.MagicMock()
        cache.get_index_chunk.return_value = ["old"]
        cache.delete_index_chunk.return_value = False
        ss._delete_docs(cache, lambda n: f"chunk_{n}")
        assert cache.delete_index_chunk.call_count == 1

    def test_legacy_chunks_still_load(self):
        """An index saved by the previous code: chunk_0, chunk_1, ... and no meta document."""
        chunks = {f"chunk_{i}": [{"scholar_id": f"c{i}", "name": f"Name {i}", "name_lower": f"name {i}"}]
                  for i in range(40)}
        authors, version = ss._load_index(FakeIndexStore(chunks))
        assert version is None
        assert [a["scholar_id"] for a in authors] == [f"c{i}" for i in range(40)]
        assert "name_lower" not in authors[0]

    def test_damaged_part_falls_back_to_nothing(self):
        store = FakeIndexStore()
        version = ss._save_index_to_firestore(store, AUTHORS[:10])
        store.docs[f"pack_{version}_0"] = b"not gzip"
        assert ss._load_index(store) == ([], None)

    def test_mock_cache_does_not_loop(self):
        """A MagicMock cache answers every read with a truthy mock; loading must still end."""
        assert ss._load_index(mock.MagicMock()) == ([], None)


class TestIndexLoading:
    def test_first_load_blocks_and_compiles(self, clean_index):
        store = FakeIndexStore()
        version = ss._save_index_to_firestore(store, AUTHORS[:10])
        ss._ensure_index_loaded(store)
        assert ss._author_index == AUTHORS[:10] and ss._index_version == version
        assert ss._compiled is not None and ss._compiled.entries is ss._author_index
        assert [r["name"] for r in ss._search_in_memory("number7")] == ["Author Number7"]

    def test_fresh_index_is_not_reloaded(self, clean_index):
        store = FakeIndexStore()
        ss._save_index_to_firestore(store, AUTHORS[:10])
        ss._ensure_index_loaded(store)
        store.reads.clear()
        ss._ensure_index_loaded(store)
        assert store.reads == []

    def _reload(self, store):
        """Run the stale-index check and wait for its background thread."""
        ss._index_loaded_at = 0
        with mock.patch("threading.Thread") as thread:
            ss._ensure_index_loaded(store)
        target = thread.call_args.kwargs["target"]
        target()

    def test_stale_index_with_same_version_is_not_downloaded_again(self, clean_index):
        store = FakeIndexStore()
        ss._save_index_to_firestore(store, AUTHORS[:10])
        ss._ensure_index_loaded(store)
        loaded = ss._author_index
        store.reads.clear()
        self._reload(store)
        assert store.reads == ["meta"]
        assert ss._author_index is loaded and ss._index_loaded_at > 0

    def test_stale_index_is_replaced_by_a_newer_version(self, clean_index):
        store = FakeIndexStore()
        ss._save_index_to_firestore(store, AUTHORS[:10])
        ss._ensure_index_loaded(store)
        with mock.patch("time.time", return_value=4102444800.0):
            newer = ss._save_index_to_firestore(store, AUTHORS[:20])
        self._reload(store)
        assert len(ss._author_index) == 20 and ss._index_version == newer
        assert ss._compiled.entries is ss._author_index

    def test_stale_index_keeps_serving_during_the_reload(self, clean_index):
        store = FakeIndexStore()
        ss._save_index_to_firestore(store, AUTHORS[:10])
        ss._ensure_index_loaded(store)
        ss._index_loaded_at = 0
        with mock.patch("threading.Thread") as thread:
            ss._ensure_index_loaded(store)  # returns at once; the reload thread is mocked out
            assert thread.return_value.start.called
            assert [r["name"] for r in ss._search_in_memory("number7")] == ["Author Number7"]
        ss._reload_running = False

    def test_failed_reload_keeps_the_index_and_retries_later(self, clean_index):
        store = FakeIndexStore()
        ss._save_index_to_firestore(store, AUTHORS[:10])
        ss._ensure_index_loaded(store)
        store.docs.clear()
        self._reload(store)
        assert len(ss._author_index) == 10
        assert ss._reload_running is False
        # Due again after the retry delay, not a full TTL.
        import time
        assert time.time() - ss._index_loaded_at >= ss._INDEX_TTL_SECONDS - ss._RELOAD_RETRY_SECONDS - 5

    def test_refresh_publishes_and_serves_the_new_index(self, clean_index):
        store = FakeIndexStore()
        bq = mock.MagicMock()
        bq.get_all_author_names.return_value = [dict(a) for a in AUTHORS[:5]]
        assert ss.refresh_author_index(bq=bq, cache=store) == 5
        assert ss._index_version == store.docs["meta"]["version"]
        assert [r["name"] for r in ss._search_in_memory("number3")] == ["Author Number3"]

    def test_warm_loads_in_a_background_thread(self, clean_index):
        store = FakeIndexStore()
        ss._save_index_to_firestore(store, AUTHORS[:10])
        with mock.patch("threading.Thread") as thread:
            AuthorSearchService(bq_client=mock.MagicMock(), cache=store).warm()
        assert thread.return_value.start.called and not ss._author_index
        thread.call_args.kwargs["target"]()
        assert len(ss._author_index) == 10
