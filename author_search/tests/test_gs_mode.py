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
