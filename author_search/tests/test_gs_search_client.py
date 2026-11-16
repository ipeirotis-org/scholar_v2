"""Tests for the live Google Scholar profile lookup (SerpApi)."""

from unittest import mock

import pytest
import requests

from author_search import gs_search_client as client

# Shape of a SerpApi engine=google_scholar response for a name query (2026-11-16).
RESPONSE = {
    "search_metadata": {"status": "Success"},
    "profiles": {
        "title": "User profiles for Ilias Triantafyllopoulos",
        "authors": [{
            "name": "Ilias Triantafyllopoulos",
            "link": "https://scholar.google.com/citations?user=OgG4DH0AAAAJ&hl=en&oi=ao",
            "author_id": "OgG4DH0AAAAJ",
            "affiliations": "PhD Student, New York University",
            "email": "Verified email at stern.nyu.edu",
            "cited_by": 21,
        }],
    },
    "organic_results": [
        {"title": "Conceptors for Semantic Steering",
         "publication_info": {"summary": "I Triantafyllopoulos, YM Cho - arXiv, 2026", "authors": [
             {"name": "I Triantafyllopoulos", "author_id": "OgG4DH0AAAAJ"},
             {"name": "YM Cho", "author_id": "ChoChoChoAAJ"}]}},
        {"title": "A paper by a namesake with a very long title that goes on and on well past seventy characters",
         "publication_info": {"authors": [
             {"name": "IK Triantafyllopoulos", "author_id": "NameSake0AAJ"},
             {"name": "G Triantafyllopoulos", "author_id": "OtherFirstAJ"},
             {"name": "I Triantafyllopoulos", "author_id": "bad id"}]}},
        {"title": "No linked authors", "publication_info": {"summary": "X Y - 2020"}},
    ],
}


class TestParseProfiles:
    def test_profile_block_first_with_details(self):
        results = client.parse_profiles(RESPONSE, "Ilias Triantafyllopoulos")
        assert results[0] == {
            "scholar_id": "OgG4DH0AAAAJ", "name": "Ilias Triantafyllopoulos",
            "affiliation": "PhD Student, New York University", "email_domain": "stern.nyu.edu",
            "citedby": 21, "hindex": None, "source": "gs_live",
        }

    def test_bylines_add_namesakes_only(self):
        """Paper authors with the searched surname and first initial; never a co-author."""
        results = client.parse_profiles(RESPONSE, "Ilias Triantafyllopoulos")
        assert [r["scholar_id"] for r in results] == ["OgG4DH0AAAAJ", "NameSake0AAJ"]
        namesake = results[1]
        assert namesake["name"] == "IK Triantafyllopoulos" and namesake["citedby"] is None
        assert namesake["affiliation"].startswith("Author of “A paper by a namesake")
        assert namesake["affiliation"].endswith("...”") and len(namesake["affiliation"]) < 90

    def test_surname_only_query_accepts_any_first_initial(self):
        ids = [r["scholar_id"] for r in client.parse_profiles(RESPONSE, "triantafyllopoulos")]
        assert ids == ["OgG4DH0AAAAJ", "NameSake0AAJ", "OtherFirstAJ"]

    def test_accents_do_not_matter(self):
        data = {"organic_results": [{"title": "T", "publication_info": {"authors": [
            {"name": "J Peña", "author_id": "PenaPenaAAAJ"}]}}]}
        assert [r["scholar_id"] for r in client.parse_profiles(data, "José Pena")] == ["PenaPenaAAAJ"]

    def test_nothing_usable(self):
        assert client.parse_profiles({}, "Any Name") == []
        assert client.parse_profiles({"profiles": {"authors": [{"name": "No Id"}]}}, "No Id") == []

    def test_max_results(self):
        data = {"profiles": {"authors": [{"name": f"A {i}", "author_id": f"{i:012d}"} for i in range(30)]}}
        assert len(client.parse_profiles(data, "A")) == 10


class TestSearchProfiles:
    @pytest.fixture(autouse=True)
    def _key(self, monkeypatch):
        monkeypatch.setattr(client, "_api_key", "secret-key")

    def _get(self, payload):
        return mock.patch.object(client.requests, "get", return_value=mock.MagicMock(json=lambda: payload))

    def test_one_scholar_search_per_lookup(self):
        with self._get(RESPONSE) as get:
            results = client.search_profiles("Ilias Triantafyllopoulos")
        assert results[0]["scholar_id"] == "OgG4DH0AAAAJ"
        assert get.call_count == 1
        params = get.call_args.kwargs["params"]
        assert params["engine"] == "google_scholar" and params["q"] == "Ilias Triantafyllopoulos"
        assert get.call_args.kwargs["timeout"] > 0

    def test_no_results_is_an_answer(self):
        with self._get({"error": "Google hasn't returned any results for this query."}):
            assert client.search_profiles("Nobody Atall") == []

    @pytest.mark.parametrize("payload", [
        {"error": "Your account has run out of searches."},
        {"error": "Invalid API key."},
        {"search_metadata": {"status": "Processing"}},
        {},
        ["not", "a", "dict"],
    ])
    def test_failures_are_not_answers(self, payload):
        with self._get(payload):
            assert client.search_profiles("Some Name") is None

    def test_network_error_does_not_log_the_key(self, caplog):
        error = requests.ConnectionError("GET https://serpapi.com/search.json?api_key=secret-key failed")
        with mock.patch.object(client.requests, "get", side_effect=error):
            assert client.search_profiles("Some Name") is None
        assert "secret-key" not in caplog.text and "ConnectionError" in caplog.text

    def test_without_a_key_nothing_is_requested(self, monkeypatch):
        monkeypatch.setattr(client, "_get_api_key", lambda: "")
        with mock.patch.object(client.requests, "get") as get:
            assert client.search_profiles("Some Name") is None
        get.assert_not_called()

    def test_key_from_environment(self, monkeypatch):
        monkeypatch.setattr(client, "_api_key", None)
        monkeypatch.setenv("SERPAPI_KEY", " env-key\n")
        assert client._get_api_key() == "env-key"
