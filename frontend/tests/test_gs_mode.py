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


# Old S2 links (numeric author IDs) still hit the site after the switch, and
# the Firestore cache still holds their S2-era entries. GS mode must not serve
# those entries, list them as recent, or enqueue cache tasks for them.
S2_ID = "1688882"


def _app_with_s2_cache(monkeypatch, data_source):
    """App whose cache holds an S2-era entry for S2_ID; returns (app, cache_instance)."""
    from frontend.app import create_app

    monkeypatch.setattr(Config, "DATA_SOURCE", data_source)
    patcher = mock.patch("frontend.routes.FirestoreCache")
    cache_cls = patcher.start()

    def cache_get(collection, doc_id):
        if collection == Config.CACHE_AUTHOR_FRESHNESS:
            return {"exists": True}
        if collection == Config.CACHE_AUTHOR_STATS:
            return {"scholar_id": doc_id, "name": "Yann LeCun", "pip_auc_score": 0.987}
        if collection == Config.CACHE_AUTHOR_PUB_STATS:
            return [{"author_pub_id": "14542261", "title": "Gradient-based learning",
                     "pub_year": 1998, "num_citations": 59237, "num_citations_percentile": 1.0}]
        if collection == Config.CACHE_AUTHOR_TEMPORAL:
            return [{"state_year": 2020, "h_index": 150, "h_index_percentile": 0.99}]
        return None

    cache_cls.return_value.get.side_effect = cache_get
    cache_cls.return_value.get_timestamp.return_value = None
    app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
    return app, cache_cls.return_value, patcher


@pytest.fixture
def s2_cache_gs(monkeypatch):
    app, cache, patcher = _app_with_s2_cache(monkeypatch, "gs")
    yield app.test_client(), cache
    patcher.stop()


@pytest.mark.parametrize("path", [
    f"/results?author_id={S2_ID}",
    f"/publications/{S2_ID}",
    f"/download/{S2_ID}",
    f"/publication/{S2_ID}/14542261",
])
def test_s2_author_pages_are_gone_in_gs_mode(s2_cache_gs, path):
    gs_client, cache = s2_cache_gs
    with mock.patch("frontend.routes.enqueue_cache_populate") as enqueue:
        response = gs_client.get(path)
    html = response.get_data(as_text=True)
    assert response.status_code == 410
    assert "old version of PiP Score" in html
    assert "index?author_name=Yann+LeCun" in html
    assert "Metric Evolution Over Time" not in html
    enqueue.assert_not_called()
    cache.record_recent_author.assert_not_called()


def test_s2_author_page_without_cached_name_links_to_search(monkeypatch):
    from frontend.app import create_app

    monkeypatch.setattr(Config, "DATA_SOURCE", "gs")
    with mock.patch("frontend.routes.FirestoreCache") as cache_cls:
        cache_cls.return_value.get.return_value = None
        app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
        response = app.test_client().get(f"/results?author_id={S2_ID}")
    assert response.status_code == 410
    assert "Search for an author" in response.get_data(as_text=True)


def test_s2_author_api_is_gone_in_gs_mode(s2_cache_gs):
    gs_client, _ = s2_cache_gs
    with mock.patch("frontend.routes.enqueue_cache_populate") as enqueue:
        response = gs_client.get(f"/api/author/{S2_ID}/data")
    assert response.status_code == 410
    assert response.json["status"] == "legacy_id"
    enqueue.assert_not_called()


def test_rebuild_skips_s2_ids_in_gs_mode(s2_cache_gs):
    gs_client, _ = s2_cache_gs
    with mock.patch("frontend.routes.enqueue_cache_populate", return_value=True) as enqueue:
        response = gs_client.post("/api/rebuild_statistics",
                                  data={"scholar_ids": f"{S2_ID},WLN3QrAAAAAJ"})
    assert response.json["authors"] == [{"scholar_id": "WLN3QrAAAAAJ"}]
    enqueue.assert_called_once_with("populate_author_profile", {"scholar_id": "WLN3QrAAAAAJ"})


def test_gs_author_page_still_renders_in_gs_mode(s2_cache_gs):
    gs_client, cache = s2_cache_gs
    response = gs_client.get("/results?author_id=WLN3QrAAAAAJ")
    assert response.status_code == 200
    assert 'id="paperChart"' in response.get_data(as_text=True)
    cache.record_recent_author.assert_called_once()


def test_s2_author_page_still_renders_in_s2_mode(monkeypatch):
    app, cache, patcher = _app_with_s2_cache(monkeypatch, "s2")
    try:
        response = app.test_client().get(f"/results?author_id={S2_ID}")
    finally:
        patcher.stop()
    assert response.status_code == 200
    assert "Metric Evolution Over Time" in response.get_data(as_text=True)
    cache.record_recent_author.assert_called_once()


def _index_with_recent(monkeypatch, data_source, query=""):
    from frontend.app import create_app

    monkeypatch.setattr(Config, "DATA_SOURCE", data_source)
    recent = [
        {"scholar_id": S2_ID, "name": "Yann LeCun (S2)", "hindex": 137, "citedby": 264266},
        {"scholar_id": "WLN3QrAAAAAJ", "name": "Yann LeCun (GS)", "hindex": 145, "citedby": 341620},
    ]
    with mock.patch("frontend.routes.FirestoreCache") as cache_cls:
        cache_cls.return_value.get.side_effect = (
            lambda collection, doc_id: recent if collection == "v3_recent_authors" else None
        )
        app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
        return app.test_client().get("/" + query).get_data(as_text=True)


def test_index_hides_s2_recent_authors_in_gs_mode(monkeypatch):
    html = _index_with_recent(monkeypatch, "gs")
    assert "Yann LeCun (GS)" in html
    assert "Yann LeCun (S2)" not in html
    assert f"author_id={S2_ID}" not in html


def test_index_keeps_s2_recent_authors_in_s2_mode(monkeypatch):
    html = _index_with_recent(monkeypatch, "s2")
    assert "Yann LeCun (S2)" in html


def test_index_prefills_and_runs_search(monkeypatch):
    html = _index_with_recent(monkeypatch, "gs", "?author_name=Yann+LeCun")
    assert 'value="Yann LeCun"' in html
    assert "\n    findAuthor();" in html


def test_index_prefill_is_escaped(monkeypatch):
    html = _index_with_recent(monkeypatch, "gs", '?author_name="><script>x</script>')
    assert "<script>x</script>" not in html
    assert 'value="&#34;&gt;&lt;script&gt;x&lt;/script&gt;"' in html


def test_index_without_prefill_does_not_search(monkeypatch):
    html = _index_with_recent(monkeypatch, "gs")
    assert 'value=""' in html
    assert "\n    findAuthor();" not in html
