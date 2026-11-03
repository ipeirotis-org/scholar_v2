"""Featured authors: the pinned homepage list and /featured (GS mode only)."""

from unittest import mock

import pytest

from frontend.cache import FEATURED_AUTHORS_COLLECTION, FEATURED_AUTHORS_DOC_ID, RECENT_AUTHORS_COLLECTION
from frontend.config import Config
from frontend.routes import HOMEPAGE_FEATURED


def _author(i, **extra):
    return {"scholar_id": f"FEAT{i:04d}AAAJ", "name": f"Featured Author {i}", "affiliation": f"Univ {i}",
            "hindex": 50 + i, "citedby": 10000 + i, "pip_auc_score": 0.5, "pip_auc_percentile": 0.8, **extra}


RECENT = [{"scholar_id": "RECENTAAAAAJ", "name": "Recent Author", "hindex": 20, "citedby": 999}]


def _client(monkeypatch, featured, data_source="gs"):
    """(test client, FirestoreCache.get mock) with `featured` stored as the featured list."""
    from frontend.app import create_app

    monkeypatch.setattr(Config, "DATA_SOURCE", data_source)
    patcher = mock.patch("frontend.routes.FirestoreCache")
    cache_cls = patcher.start()

    def cache_get(collection, doc_id):
        if (collection, doc_id) == (FEATURED_AUTHORS_COLLECTION, FEATURED_AUTHORS_DOC_ID):
            return featured
        if collection == RECENT_AUTHORS_COLLECTION:
            return RECENT
        return None

    cache_cls.return_value.get.side_effect = cache_get
    app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
    return app.test_client(), cache_cls.return_value.get, patcher


@pytest.fixture
def make_client(monkeypatch):
    patchers = []

    def make(featured, data_source="gs"):
        client, get, patcher = _client(monkeypatch, featured, data_source)
        patchers.append(patcher)
        return client, get

    yield make
    for p in patchers:
        p.stop()


def test_homepage_shows_top_featured_and_see_more(make_client):
    client, _ = make_client([_author(i) for i in range(1, 31)])
    html = client.get("/").get_data(as_text=True)
    assert "Featured Authors" in html
    assert f"Featured Author {HOMEPAGE_FEATURED}<" in html
    assert f"Featured Author {HOMEPAGE_FEATURED + 1}<" not in html
    assert 'href="/featured"' in html
    # Featured comes first; the recent list is still there.
    assert html.index("Featured Authors") < html.index("Recently Analyzed Authors")
    assert "Recent Author" in html


def test_homepage_without_more_featured_has_no_see_more(make_client):
    client, _ = make_client([_author(i) for i in range(1, HOMEPAGE_FEATURED + 1)])
    html = client.get("/").get_data(as_text=True)
    assert f"Featured Author {HOMEPAGE_FEATURED}<" in html
    assert "See more featured authors" not in html


def test_homepage_without_featured_list(make_client):
    client, _ = make_client(None)
    html = client.get("/").get_data(as_text=True)
    assert "Featured Authors" not in html
    assert "Recent Author" in html


def test_homepage_skips_malformed_featured_rows(make_client):
    client, _ = make_client(["junk", {"name": "No ID"}, {"scholar_id": "1688882", "name": "S2 Era"}, _author(1)])
    html = client.get("/").get_data(as_text=True)
    assert "Featured Author 1<" in html
    assert "No ID" not in html and "S2 Era" not in html


def test_rows_link_to_results(make_client):
    client, _ = make_client([_author(1)])
    html = client.get("/").get_data(as_text=True)
    assert 'data-href="/results?author_id=FEAT0001AAAJ"' in html
    assert '<a href="/results?author_id=FEAT0001AAAJ">Featured Author 1</a>' in html
    assert "js/author_rows.js" in html


def test_featured_names_are_escaped(make_client):
    client, _ = make_client([_author(1, name="<script>x</script>")])
    html = client.get("/featured").get_data(as_text=True)
    assert "<script>x</script>" not in html


def test_featured_page_lists_all_with_rank(make_client):
    client, _ = make_client([_author(i) for i in range(1, 31)])
    response = client.get("/featured")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "Featured Author 1<" in html and "Featured Author 30<" in html
    assert '<td class="text-muted">30</td>' in html
    assert "deceased" in html


def test_featured_page_when_empty(make_client):
    client, _ = make_client(None)
    response = client.get("/featured")
    assert response.status_code == 200
    assert "No featured authors yet" in response.get_data(as_text=True)


def test_featured_is_gs_only(make_client):
    client, get = make_client([_author(1)], data_source="s2")
    home = client.get("/").get_data(as_text=True)
    page = client.get("/featured").get_data(as_text=True)
    assert "Featured Author 1" not in home and "Featured Author 1" not in page
    assert 'href="/featured"' not in home
    assert all(c.args[0] != FEATURED_AUTHORS_COLLECTION for c in get.call_args_list)


def test_nav_links_to_featured_in_gs_mode(make_client):
    client, _ = make_client([_author(1)])
    assert 'href="/featured">Featured</a>' in client.get("/help").get_data(as_text=True)
