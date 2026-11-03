"""Frontend behaviour under DATA_SOURCE=gs: template wording and GS-only routes."""

from unittest import mock

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


# Per-paper citation histories don't exist for GS profiles yet, so the
# publication detail route must not enqueue cache tasks or show a loading page.
GS_PUB = "iGzDl8IAAAAJ:kAoMiUvgv_QC"
GS_RECORD = (
    "https://scholar.google.com/citations?view_op=view_citation&hl=en"
    "&citation_for_view=iGzDl8IAAAAJ:kAoMiUvgv_QC"
)


def test_publication_detail_redirects_to_google_scholar(gs_client):
    with mock.patch("frontend.routes.enqueue_cache_populate") as enqueue:
        response = gs_client.get(f"/publication/iGzDl8IAAAAJ/{GS_PUB}")
    assert response.status_code == 302
    assert response.headers["Location"] == GS_RECORD
    enqueue.assert_not_called()


def test_publication_detail_without_gs_record_is_404(gs_client):
    with mock.patch("frontend.routes.enqueue_cache_populate") as enqueue:
        response = gs_client.get("/publication/iGzDl8IAAAAJ/12345")
    assert response.status_code == 404
    assert "not available for Google Scholar profiles" in response.get_data(as_text=True)
    enqueue.assert_not_called()


def test_publication_api_reports_unavailable(gs_client):
    with mock.patch("frontend.routes.enqueue_cache_populate") as enqueue:
        response = gs_client.get(f"/api/publication/{GS_PUB}/data")
    assert response.status_code == 404
    assert response.json["status"] == "unavailable"
    assert response.json["google_scholar_url"] == GS_RECORD
    enqueue.assert_not_called()


def test_publication_detail_still_enqueues_in_s2_mode(client):
    with mock.patch("frontend.routes.enqueue_cache_populate", return_value=True) as enqueue, \
         mock.patch("frontend.routes.FirestoreCache.get", return_value=None):
        response = client.get("/publication/2942126/12345")
    assert response.status_code == 200
    enqueue.assert_called_once_with("populate_publication_detail", {"author_pub_id": "12345"})


def _publications_page(monkeypatch, data_source):
    from frontend.app import create_app

    monkeypatch.setattr(Config, "DATA_SOURCE", data_source)
    with mock.patch("frontend.routes.FirestoreCache") as cache_cls:
        def cache_get(collection, doc_id):
            if collection == Config.CACHE_AUTHOR_FRESHNESS:
                return {"exists": True}
            if collection == Config.CACHE_AUTHOR_STATS:
                return {"name": "Peyman Milanfar"}
            if collection == Config.CACHE_AUTHOR_PUB_STATS:
                return [{"author_pub_id": GS_PUB, "title": "Fast and robust multiframe super resolution",
                         "citation": "IEEE TIP, 2004", "pub_year": 2004, "num_citations": 3098,
                         "num_citations_percentile": 0.994}]
            return None

        cache_cls.return_value.get.side_effect = cache_get
        app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
        return app.test_client().get("/publications/iGzDl8IAAAAJ").get_data(as_text=True)


def test_publications_table_links_to_google_scholar_in_gs_mode(monkeypatch):
    html = _publications_page(monkeypatch, "gs")
    assert GS_RECORD.replace("&", "&amp;") in html
    assert "View Details" not in html
    assert "Cited publications" in html


def test_publications_table_keeps_detail_links_in_s2_mode(monkeypatch):
    html = _publications_page(monkeypatch, "s2")
    assert f"/publication/iGzDl8IAAAAJ/{GS_PUB}" in html
    assert "scholar.google.com/citations?view_op" not in html
