"""Shared fixtures for gs_crawler tests: SerpApi-shaped author pages."""

import pytest


def make_article(i, year="2015", cites=10, author_id="TESTAAAAAAAJ"):
    return {
        "title": f"Paper {i}",
        "citation_id": f"{author_id}:pub{i:04d}",
        "authors": "A Author, B Author",
        "publication": f"Journal of Tests {i}, 1-10, {year}",
        "cited_by": {"value": cites} if cites else {"value": None},
        "year": year,
    }


def make_page(n_articles, offset=0, first=True, author_id="TESTAAAAAAAJ"):
    page = {
        "search_metadata": {"status": "Success"},
        "search_parameters": {"engine": "google_scholar_author", "author_id": author_id, "start": offset},
        "articles": [make_article(offset + i, author_id=author_id) for i in range(n_articles)],
    }
    if first:
        page["author"] = {
            "name": "Test Author",
            "affiliations": "Test University",
            "email": "Verified email at test.edu",
            "interests": [{"title": "Testing"}, {"title": "Data"}],
        }
        page["cited_by"] = {
            "table": [
                {"citations": {"all": 1200, "since_2021": 500}},
                {"h_index": {"all": 15, "since_2021": 10}},
                {"i10_index": {"all": 20, "since_2021": 12}},
            ],
            "graph": [{"year": 2023, "citations": 100}, {"year": 2024, "citations": 120}],
        }
        page["co_authors"] = [
            {"name": "Co Author", "author_id": "COAUTHAAAAJ", "affiliations": "Elsewhere"},
            {"name": "No Id"},
        ]
    else:
        page["author"] = {"name": "Test Author"}
    return page


@pytest.fixture
def serpapi_key(monkeypatch):
    import gs_crawler.serpapi_client as client
    monkeypatch.setattr(client, "_api_key", "test-key")
    monkeypatch.setattr(client.time, "sleep", lambda s: None)
