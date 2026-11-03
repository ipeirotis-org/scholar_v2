"""Template wording for DATA_SOURCE=gs."""

import pytest

from frontend.config import Config


@pytest.fixture
def gs_client(monkeypatch, client):
    monkeypatch.setattr(Config, "DATA_SOURCE", "gs")
    return client


def test_help_mentions_google_scholar(gs_client):
    html = gs_client.get("/help").get_data(as_text=True)
    assert "scholar.google.com" in html
    assert "semanticscholar.org" not in html


def test_index_invites_profile_urls(gs_client):
    html = gs_client.get("/").get_data(as_text=True)
    assert "paste a Google Scholar profile URL" in html
    assert "Semantic Scholar" not in html


def test_s2_wording_by_default(client):
    assert Config.DATA_SOURCE == "s2"
    html = client.get("/help").get_data(as_text=True)
    assert "semanticscholar.org" in html


def test_search_beyond_option_hidden_without_s2_fallback(client):
    html = client.get("/").get_data(as_text=True)
    assert "return;  // no external name search" in html


def test_empty_search_offers_paste_hint_in_gs_mode(gs_client):
    html = gs_client.get("/").get_data(as_text=True)
    assert "appendPasteHint();" in html
    assert "not analyzed yet" in html
    assert 'author.source === "coauthor"' in html


def test_no_paste_hint_in_s2_mode(client):
    html = client.get("/").get_data(as_text=True)
    assert "appendPasteHint();" not in html
