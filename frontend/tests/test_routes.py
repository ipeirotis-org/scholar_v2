"""Tests for v3 frontend route handlers.

The frontend is cache-read-only — it reads from Firestore and enqueues
tasks to the Cache Layer on cache miss. No BigQuery dependency.
Visualization is client-side via Plotly.js — no matplotlib.
"""

import datetime
from unittest import mock

import pytest


class TestIndexRoute:
    def test_index_returns_200(self, client):
        response = client.get("/")
        assert response.status_code == 200
        assert b"PiP Score" in response.data

    def test_index_alias(self, client):
        response = client.get("/index")
        assert response.status_code == 200


class TestResultsRoute:
    def test_results_missing_author_id_redirects(self, client):
        response = client.get("/results")
        assert response.status_code == 302

    def test_results_invalid_author_id_redirects(self, client):
        response = client.get("/results?author_id=<script>alert(1)</script>")
        assert response.status_code == 302

    def test_results_cache_miss_shows_loading(self):
        """When freshness cache is empty, enqueue cache populate and show redirect."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate") as mock_enqueue:
            cache_instance = cache_cls.return_value
            cache_instance.get.return_value = None  # Everything is a cache miss

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/results?author_id=abc123def456")
            assert response.status_code == 200
            assert b"processing" in response.data.lower() or b"prepared" in response.data.lower()
            mock_enqueue.assert_called()

    def test_results_with_cached_data_renders_chart_divs(self):
        """When cache has PiP data, render page with standing curve, tau controls, and 3-tab explorer."""
        from frontend.app import create_app

        curve = [round(1.0 - j / 80, 6) for j in range(40)]
        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate"):
            cache_instance = cache_cls.return_value

            def cache_get(collection, doc_id):
                if collection == "v3_author_freshness":
                    return {"exists": True, "last_updated": datetime.datetime(2025, 1, 1)}
                if collection == "v3_author_stats":
                    return {
                        "scholar_id": "123",
                        "name": "Test Author",
                        "affiliation": "Test U",
                        "hindex": 10,
                        "citedby": 500,
                        "citedby_percentile": 0.8,
                        "hindex_percentile": 0.7,
                        "total_publications_with_citations": 30,
                        "total_publications_with_citations_percentile": 0.6,
                        "i10index": 5,
                        "i10index_percentile": 0.5,
                        "pip_score": 0.8125,
                        "pip_score_percentile": 0.92,
                        "pip_standing": curve,
                        "pip_auc_score": 0.75,
                        "pip_auc_score_percentile": 0.9,
                        "year_of_first_pub": 2010,
                    }
                if collection == "v3_author_pub_stats":
                    return [
                        {
                            "publication_rank": 1,
                            "num_citations_percentile": 0.9,
                            "num_papers_percentile": 0.8,
                            "pub_year": 2020,
                            "title": "Test Paper",
                            "num_citations": 50,
                        },
                    ]
                if collection == "v3_author_temporal":
                    return [
                        {
                            "state_year": 2020,
                            "h_index": 10,
                            "h_index_percentile": 0.7,
                            "total_citations": 500,
                            "total_citations_percentile": 0.8,
                            "i10_index": 5,
                            "i10_index_percentile": 0.5,
                        },
                    ]
                return None

            cache_instance.get.side_effect = cache_get
            cache_instance.get_timestamp.return_value = datetime.datetime(2025, 1, 1)
            cache_instance.set.return_value = True

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/results?author_id=123")
            assert response.status_code == 200
            html = response.data.decode()
            # Headline leads with the comparison group, then the raw PiP score
            assert "ranks <span class=\"standing-share\">at the 92.0th percentile</span>" in html
            assert "percentile in this cohort" not in html  # one scale: no second percentile line
            assert "whose career started in 2010" in html
            assert "Career start 2010" in html
            assert "PiP 0.8125" in html
            assert "Cited publications" in html
            # One chart + pinned-work panel + list + tau cutoff controls
            assert 'id="paperChart"' in html
            assert 'id="paperPanel"' in html
            assert 'id="paperList"' in html
            assert 'id="tauControls"' in html
            assert 'id="tauSlider"' in html
            assert "initPaperExplorer" in html
            # The explorer opens on the Standing curve, with By publication year and PiP-AUC curve tabs
            normalized = " ".join(html.split())
            assert ('data-explorer-view="standing" aria-pressed="true">Standing curve</button> '
                    '<button type="button" class="btn btn-outline-violet" data-explorer-view="year" '
                    'aria-pressed="false">By publication year</button> '
                    '<button type="button" class="btn btn-outline-violet" data-explorer-view="pip" '
                    'aria-pressed="false">PiP-AUC curve') in normalized
            # The by-year view starts at the career-start year
            assert "startYear: 2010," in html
            assert "pipAucScore: 0.75," in html
            assert "hIndexPlot" in html
            # Should NOT contain base64 PNG images
            assert "data:image/png;base64" not in html

    def test_results_legacy_stats_without_pip_score_falls_back_and_enqueues_gs_refresh(self):
        """Pre-PiP cached profiles show PiP-AUC with 2-tab explorer and trigger a background GS refresh."""
        from frontend.app import create_app
        from frontend.config import Config

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate") as mock_enqueue, \
             mock.patch.object(Config, "DATA_SOURCE", "gs"):
            cache_instance = cache_cls.return_value

            def cache_get(collection, doc_id):
                if collection == "v3_author_freshness":
                    return {"exists": True, "last_updated": datetime.datetime(2025, 1, 1)}
                if collection == "v3_author_stats":
                    return {
                        "scholar_id": "abc123def456",
                        "name": "Legacy Author",
                        "pip_auc_score": 0.75,
                        "pip_auc_score_percentile": 0.9,
                        "year_of_first_pub": 2010,
                    }
                if collection == "v3_author_pub_stats":
                    return [{"publication_rank": 1, "num_citations_percentile": 0.9,
                             "num_papers_percentile": 0.8, "pub_year": 2020,
                             "title": "Paper", "num_citations": 50}]
                return None

            cache_instance.get.side_effect = cache_get
            cache_instance.get_timestamp.return_value = None
            cache_instance.set.return_value = True

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            html = app.test_client().get("/results?author_id=abc123def456").get_data(as_text=True)
            normalized = " ".join(html.split())
            assert "PiP-AUC 0.7500" in html
            assert 'id="tauControls"' not in html
            assert ('data-explorer-view="pip" aria-pressed="true">PiP-AUC curve</button> '
                    '<button type="button" class="btn btn-outline-violet" data-explorer-view="year" '
                    'aria-pressed="false">By publication year') in normalized
            mock_enqueue.assert_called_once_with(
                "populate_author_profile", {"scholar_id": "abc123def456"}
            )

    def test_results_zero_pip_score_renders_zero_not_pending(self):
        html = self._render_with_stats({
            "name": "Early Author",
            "pip_score": 0.0,
            "pip_score_percentile": 0.08,
            "year_of_first_pub": 2021,
        })
        assert "at the 8.0th percentile" in html
        assert "PiP 0.0000" in html
        assert "is pending" not in html

    def test_results_cited_count_comes_from_author_stats(self):
        """A failed publication-list read must not turn the note into '0 of 282'."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate"):
            cache_instance = cache_cls.return_value

            def cache_get(collection, doc_id):
                if collection == "v3_author_freshness":
                    return {"exists": True}
                if collection == "v3_author_stats":
                    return {"name": "A", "total_publications": 282,
                            "total_publications_with_citations": 237,
                            "total_publications_with_citations_percentile": 0.682,
                            "pip_score": 0.5123, "pip_score_percentile": 0.812,
                            "pip_auc_score": 0.4272, "pip_auc_score_percentile": 0.771,
                            "year_of_first_pub": 1990}
                return None  # publication list unavailable

            cache_instance.get.side_effect = cache_get
            cache_instance.get_timestamp.return_value = None

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            html = app.test_client().get("/results?author_id=123").get_data(as_text=True)
            normalized = " ".join(html.split())
            assert "counts the 237 of 282 profile records" in normalized
            assert "counts the 0 of" not in normalized
            assert 'id="paperChart"' not in html

    @staticmethod
    def _get(stats, url, data_source="s2"):
        """GET url with stats as the cached author; returns (status, location, normalized html)."""
        from frontend.app import create_app
        from frontend.config import Config

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate"), \
             mock.patch.object(Config, "DATA_SOURCE", data_source):
            cache_instance = cache_cls.return_value

            def cache_get(collection, doc_id):
                if collection == "v3_author_freshness":
                    return {"exists": True}
                if collection == "v3_author_stats":
                    return stats
                return None

            cache_instance.get.side_effect = cache_get
            cache_instance.get_timestamp.return_value = None
            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            response = app.test_client().get(url)
        return response.status_code, response.headers.get("Location"), " ".join(response.get_data(as_text=True).split())

    @classmethod
    def _render_with_stats(cls, stats, query=""):
        return cls._get(stats, "/results?author_id=123" + query)[2]

    PARKER = {
        "name": "Jonathan A. Parker",
        "pip_score": 0.085, "pip_score_percentile": 0.211,
        "pip_auc_score": 0.0692, "pip_auc_score_percentile": 0.211,
        "year_of_first_pub": 1994, "field_group": "Economics & Business", "field_source": "interests",
        "hindex": 40, "hindex_percentile": 0.31, "citedby": 12000, "citedby_percentile": 0.33,
        "field_percentiles": [
            {"field_group": "Computer Science", "cohort_start": 1992, "cohort_end": 1996,
             "peer_count": 380, "percentile": 0.12, "hindex_percentile": 0.2, "citedby_percentile": 0.25},
            {"field_group": "Economics & Business", "cohort_start": 1992, "cohort_end": 1996,
             "peer_count": 151, "percentile": 0.34, "hindex_percentile": 0.48, "citedby_percentile": 0.6},
            {"field_group": "Physics & Astronomy", "cohort_start": 1986, "cohort_end": 2004,
             "peer_count": 20, "percentile": None},
        ],
    }

    def test_results_link_to_the_field_page(self):
        html = self._render_with_stats(self.PARKER)
        assert "at the 21.1st percentile" in html  # overall headline
        assert ('<a href="/field?author_id=123">How Jonathan A. Parker compares within '
                "Economics &amp; Business &rarr;</a>") in html
        assert "<select" not in html  # the picker lives on the field page
        no_own = self._render_with_stats(dict(self.PARKER, field_group=float("nan")))
        assert "compares within a field &rarr;</a>" in no_own

    def test_results_without_field_percentiles_have_no_field_link(self):
        # Cache entries from before field percentiles, and S2 mode
        html = self._render_with_stats({"name": "A", "pip_auc_score": 0.4272,
                                        "pip_auc_score_percentile": 0.771, "year_of_first_pub": 1990})
        assert "at the 77.1st percentile" in html
        assert "/field?" not in html

    def test_results_field_parameter_redirects_to_the_field_page(self):
        status, location, _ = self._get(self.PARKER, "/results?author_id=123&field=Computer+Science")
        assert status == 302
        assert location.endswith("/field?author_id=123&field=Computer+Science")

    def test_field_page_on_own_field(self):
        status, _, html = self._get(self.PARKER, "/field?author_id=uXulDDsAAAAJ", "gs")
        assert status == 200
        assert ("Jonathan A. Parker ranks <span class=\"standing-share\">at the 34.0th percentile</span> "
                "of the 151 tracked active authors in Economics &amp; Business whose career started 1992–1996.") in html
        assert '<option value="Economics &amp; Business" selected>Economics &amp; Business (from profile)</option>' in html
        assert '<option value="Physics &amp; Astronomy" disabled>Physics &amp; Astronomy (too few authors)</option>' in html
        # Metric rows among the same peers, with the PiP line at the field percentile
        assert '<span class="metric-pct" role="cell">48.0th</span>' in html
        assert '<span class="metric-pct" role="cell">60.0th</span>' in html
        assert '<span class="metric-ref-label" style="left: 34.0%" aria-hidden="true">PiP</span>' in html
        assert "PiP 0.0850" in html
        assert "the PiP percentile (34.0th)." in html
        assert ('<a href="/results?author_id=uXulDDsAAAAJ">Across all fields: 21.1st percentile '
                '(career start 1994)</a>') in html
        assert "their Google Scholar interests" in html

    def test_field_page_honours_the_field_parameter(self):
        _, _, html = self._get(self.PARKER, "/field?author_id=uXulDDsAAAAJ&field=Computer+Science", "gs")
        assert '<option value="Computer Science" selected>' in html
        assert "at the 12.0th percentile</span> of the 380 tracked active authors in Computer Science" in html

    def test_field_page_without_own_field_asks_for_a_choice(self):
        stats = dict(self.PARKER, field_group=float("nan"), field_source=float("nan"))
        _, _, html = self._get(stats, "/field?author_id=uXulDDsAAAAJ", "gs")
        assert '<option value="" selected disabled>choose a field</option>' in html
        assert "names no field we recognize" in html
        assert "standing-share" not in html

    def test_field_page_falls_back_to_results(self):
        for stats, mode in [(None, "gs"),                                   # not cached yet
                            ({"name": "A", "pip_score": 0.4, "pip_score_percentile": 0.7}, "gs"),
                            (self.PARKER, "s2")]:                           # S2 mode
            status, location, _ = self._get(stats, "/field?author_id=uXulDDsAAAAJ", mode)
            assert status == 302 and location.endswith("/results?author_id=uXulDDsAAAAJ"), (stats, mode)

    def test_results_pending_pip_score_renders_without_headline_share(self):
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate"):
            cache_instance = cache_cls.return_value

            def cache_get(collection, doc_id):
                if collection == "v3_author_freshness":
                    return {"exists": True}
                if collection == "v3_author_stats":
                    return {"name": "New Author", "hindex": 3, "hindex_percentile": 0.2,
                            "pip_score": float("nan"), "pip_score_percentile": None,
                            "pip_auc_score": float("nan"), "pip_auc_score_percentile": None,
                            "year_of_first_pub": 2020}
                return None

            cache_instance.get.side_effect = cache_get
            cache_instance.get_timestamp.return_value = None

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            response = app.test_client().get("/results?author_id=123")
            assert response.status_code == 200
            html = response.data.decode()
            assert "PiP is pending" in html
            assert "standing-share" not in html
            assert 'id="paperChart"' not in html


class TestInputValidation:
    def test_scholar_id_validation_accepts_valid(self, client):
        from frontend.routes import _validate_scholar_id
        assert _validate_scholar_id("abc123def456") == "abc123def456"
        assert _validate_scholar_id("A-B_c123") == "A-B_c123"

    def test_scholar_id_validation_rejects_invalid(self, client):
        from frontend.routes import _validate_scholar_id
        assert _validate_scholar_id("") is None
        assert _validate_scholar_id(None) is None
        assert _validate_scholar_id("<script>") is None

    def test_scholar_id_validation_accepts_s2_numeric_ids(self, client):
        from frontend.routes import _validate_scholar_id
        assert _validate_scholar_id("2942126") == "2942126"
        assert _validate_scholar_id("2242100447") == "2242100447"

    def test_pub_id_validation_accepts_valid(self, client):
        from frontend.routes import _validate_author_pub_id
        assert _validate_author_pub_id("abc123:def456") == "abc123:def456"

    def test_pub_id_validation_rejects_invalid(self, client):
        from frontend.routes import _validate_author_pub_id
        assert _validate_author_pub_id("") is None
        assert _validate_author_pub_id(None) is None
        assert _validate_author_pub_id("<script>") is None


class TestDataRoute:
    def test_data_page_returns_200(self, client):
        response = client.get("/data")
        assert response.status_code == 200
        assert b"Data Export" in response.data


class TestHelpRoute:
    def test_help_page_returns_200(self, client):
        response = client.get("/help")
        assert response.status_code == 200
        html = response.get_data(as_text=True)
        assert "Percentile-in-Percentile" in html
        assert "PiP-AUC" in html
        assert "0.50" not in html
        assert "semanticscholar.org" in html

    def test_help_page_gs_mode(self, monkeypatch):
        from frontend.app import create_app
        from frontend.config import Config
        monkeypatch.setattr(Config, "DATA_SOURCE", "gs")
        app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
        html = app.test_client().get("/help").get_data(as_text=True)
        assert "40 steps of 2.5 percentile points" in html
        assert "0.50" in html
        assert "strictly fewer" in html
        assert "PiP-AUC" in html


class TestSecurityHeaders:
    def test_security_headers_present(self, client):
        response = client.get("/")
        assert response.headers.get("X-Content-Type-Options") == "nosniff"
        assert response.headers.get("X-Frame-Options") == "DENY"
        assert response.headers.get("X-XSS-Protection") == "1; mode=block"
        assert response.headers.get("Referrer-Policy") == "strict-origin-when-cross-origin"
        assert "geolocation=()" in response.headers.get("Permissions-Policy", "")


class TestApiRoutes:
    def test_get_similar_authors_empty_name(self, client):
        response = client.get("/get_similar_authors?author_name=")
        assert response.status_code == 200
        assert response.json == []

    def test_api_rebuild_statistics_no_ids(self, client):
        response = client.post("/api/rebuild_statistics", data={"scholar_ids": ""})
        assert response.status_code == 400

    def test_api_rebuild_statistics_rejects_get(self, client):
        response = client.get("/api/rebuild_statistics?scholar_ids=123")
        assert response.status_code == 405


class TestApiAuthorData:
    def test_api_author_data_invalid_id(self, client):
        response = client.get("/api/author/<bad>/data")
        assert response.status_code == 400
        assert response.json["error"] == "Invalid author ID"

    def test_api_author_data_cache_miss_returns_202(self):
        """When author data is not cached, return 202 and enqueue."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate") as mock_enqueue:
            cache_instance = cache_cls.return_value
            cache_instance.get.return_value = None

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/api/author/2942126/data")
            assert response.status_code == 202
            assert response.json["status"] == "loading"
            mock_enqueue.assert_called()

    def test_api_author_data_not_found_returns_404(self):
        """When freshness says author doesn't exist, return 404."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate"):
            cache_instance = cache_cls.return_value

            def cache_get(collection, doc_id):
                if collection == "v3_author_freshness":
                    return {"exists": False}
                return None

            cache_instance.get.side_effect = cache_get
            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/api/author/9999999/data")
            assert response.status_code == 404
            assert response.json["status"] == "not_found"

    def test_api_author_data_returns_json(self):
        """When cache has data, return structured JSON."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate"):
            cache_instance = cache_cls.return_value

            def cache_get(collection, doc_id):
                if collection == "v3_author_freshness":
                    return {"exists": True, "last_updated": datetime.datetime(2025, 1, 1)}
                if collection == "v3_author_stats":
                    return {"scholar_id": "2942126", "name": "Test", "hindex": 10,
                            "year_of_first_pub": 2010}
                if collection == "v3_author_pub_stats":
                    return [{"publication_rank": 1, "num_citations_percentile": 0.9,
                             "num_papers_percentile": 0.8, "pub_year": 2020,
                             "title": "Paper", "num_citations": 50}]
                if collection == "v3_author_temporal":
                    return [{"state_year": 2020, "h_index": 10,
                             "h_index_percentile": 0.7, "total_citations": 500,
                             "total_citations_percentile": 0.8,
                             "i10_index": 5, "i10_index_percentile": 0.5}]
                return None

            cache_instance.get.side_effect = cache_get
            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/api/author/2942126/data")
            assert response.status_code == 200
            data = response.json
            assert "author" in data
            assert "publications" in data
            assert "temporal" in data
            assert data["author"]["name"] == "Test"
            assert len(data["publications"]) == 1
            assert data["publications"][0]["num_citations_percentile"] == 90.0


class TestApiPublicationData:
    def test_api_publication_data_invalid_id(self, client):
        response = client.get("/api/publication/ab/data")
        assert response.status_code == 400

    def test_api_publication_data_cache_miss_returns_202(self):
        """When pub data is not cached, return 202."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls, \
             mock.patch("frontend.routes.enqueue_cache_populate") as mock_enqueue:
            cache_instance = cache_cls.return_value
            cache_instance.get.return_value = None

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/api/publication/abc123:def456/data")
            assert response.status_code == 202
            mock_enqueue.assert_called()


class TestSpeedCheck:
    def test_speed_check_invalid_author_id(self, client):
        response = client.get("/api/speed_check?author_id=<bad>")
        assert response.status_code == 400
        assert response.json["status"] == "error"

    def test_speed_check_returns_timings(self):
        """Speed check reads from cache only — no BigQuery."""
        from frontend.app import create_app

        with mock.patch("frontend.routes.FirestoreCache") as cache_cls:
            cache_instance = cache_cls.return_value

            def cache_get(collection, doc_id):
                if collection == "v3_author_freshness":
                    return {"exists": True, "last_updated": datetime.datetime(2025, 1, 1)}
                if collection == "v3_author_stats":
                    return {"name": "Test Author"}
                if collection == "v3_author_pub_stats":
                    return [{"pub_year": 2020, "num_citations_percentile": 0.5}]
                if collection == "v3_author_temporal":
                    return [{"state_year": 2020}]
                return None

            cache_instance.get.side_effect = cache_get
            cache_instance.set.return_value = True

            app = create_app(config={"TESTING": True, "SECRET_KEY": "test"})
            test_client = app.test_client()
            response = test_client.get("/api/speed_check?author_id=evnr-MwAAAAJ")
            assert response.status_code == 200
            data = response.json
            assert data["status"] == "ok"
            assert "timings" in data
            assert "total_ms" in data["timings"]
            assert data["has_author_stats"] is True


class TestPrepareChartData:
    def test_prepare_pub_chart_data_empty(self):
        from frontend.routes import _prepare_pub_chart_data
        assert _prepare_pub_chart_data(None, {}) == []
        assert _prepare_pub_chart_data([], {}) == []

    def test_prepare_pub_chart_data_transforms(self):
        from frontend.routes import _prepare_pub_chart_data
        pub_stats = [
            {
                "publication_rank": 1,
                "num_citations_percentile": 0.9,
                "num_papers_percentile": 0.8,
                "pub_year": 2020,
                "title": "Test Paper",
                "num_citations": 50,
            },
        ]
        result = _prepare_pub_chart_data(pub_stats, {})
        assert len(result) == 1
        assert result[0]["num_citations_percentile"] == 90.0
        assert result[0]["pip_class"] == 36
        assert result[0]["num_papers_percentile"] == 80.0
        assert result[0]["age"] > 0

    def test_prepare_pub_chart_data_pip_class_uses_unrounded_percentile(self):
        from frontend.routes import _prepare_pub_chart_data
        # 0.4999 rounds to 50.00% for display, but belongs in class 19 (not >=20) in BigQuery.
        rows = [
            {"publication_rank": i + 1, "num_citations_percentile": y,
             "num_papers_percentile": 0.5, "pub_year": 2020, "title": f"P{i}", "num_citations": 10}
            for i, y in enumerate([0.0, 0.4999, 0.5, 0.975, 1.0])
        ]
        result = _prepare_pub_chart_data(rows, {})
        assert [r["pip_class"] for r in result] == [0, 19, 20, 39, 39]

    def test_prepare_pub_chart_data_keeps_id_and_venue_without_year(self):
        from frontend.routes import _prepare_pub_chart_data
        base = {"publication_rank": 1, "num_citations_percentile": 0.9,
                "num_papers_percentile": 0.8, "pub_year": 2004, "title": "T",
                "num_citations": 5, "author_pub_id": "abc:def"}
        result = _prepare_pub_chart_data([
            {**base, "citation": "IEEE TIP 13 (10), 1327-1344, 2004"},
            {**base, "citation": "2004"},
            {**base, "citation": None},
            {**base, "citation": float("nan")},
        ], {})
        assert result[0]["author_pub_id"] == "abc:def"
        assert [r["venue"] for r in result] == ["IEEE TIP 13 (10), 1327-1344", "", "", ""]

    def test_prepare_pub_chart_data_skips_invalid_year(self):
        from frontend.routes import _prepare_pub_chart_data
        pub_stats = [
            {"publication_rank": 1, "num_citations_percentile": 0.9,
             "num_papers_percentile": 0.8, "pub_year": "bad", "title": "X",
             "num_citations": 5},
        ]
        result = _prepare_pub_chart_data(pub_stats, {})
        assert len(result) == 0

    def test_prepare_temporal_chart_data_empty(self):
        from frontend.routes import _prepare_temporal_chart_data
        assert _prepare_temporal_chart_data(None, {}) == []
        assert _prepare_temporal_chart_data([], {}) == []

    def test_prepare_temporal_chart_data_filters_old_years(self):
        from frontend.routes import _prepare_temporal_chart_data
        temporal = [
            {"state_year": 1940, "h_index": 1, "h_index_percentile": 0.1,
             "total_citations": 5, "total_citations_percentile": 0.1,
             "i10_index": 0, "i10_index_percentile": 0},
            {"state_year": 2020, "h_index": 10, "h_index_percentile": 0.7,
             "total_citations": 500, "total_citations_percentile": 0.8,
             "i10_index": 5, "i10_index_percentile": 0.5},
        ]
        result = _prepare_temporal_chart_data(temporal, {})
        assert len(result) == 1
        assert result[0]["state_year"] == 2020

    def test_prepare_temporal_chart_data_respects_first_pub_year(self):
        from frontend.routes import _prepare_temporal_chart_data
        temporal = [
            {"state_year": 2000, "h_index": 1, "h_index_percentile": 0.1,
             "total_citations": 5, "total_citations_percentile": 0.1,
             "i10_index": 0, "i10_index_percentile": 0},
            {"state_year": 2015, "h_index": 10, "h_index_percentile": 0.7,
             "total_citations": 500, "total_citations_percentile": 0.8,
             "i10_index": 5, "i10_index_percentile": 0.5},
        ]
        result = _prepare_temporal_chart_data(temporal, {"year_of_first_pub": 2010})
        assert len(result) == 1
        assert result[0]["state_year"] == 2015


class TestErrorHandlers:
    def test_404_handler(self, client):
        response = client.get("/nonexistent-page")
        assert response.status_code == 404
        assert b"Page not found" in response.data
