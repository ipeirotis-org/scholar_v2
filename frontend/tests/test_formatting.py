"""Tests for the plain-language percentile formatting on the results page."""

import pytest

from frontend.formatting import (
    build_standing,
    ordinal_percentile,
    standing_phrase,
    top_share,
)


@pytest.mark.parametrize("fraction, expected", [
    (0.771, "77.1st"),
    (0.86, "86th"),
    (0.682, "68.2nd"),
    (0.453, "45.3rd"),
    (0.799, "79.9th"),
    (0.11, "11th"),
    (0.12, "12th"),
    (0.21, "21st"),
    (0.0, "0th"),
])
def test_ordinal_percentile(fraction, expected):
    assert ordinal_percentile(fraction) == expected


@pytest.mark.parametrize("fraction, expected", [
    (0.771, "23"),
    (0.994, "0.6"),
    (0.95, "5"),
    (0.955, "4.5"),
    (1.0, "0.1"),
])
def test_top_share(fraction, expected):
    assert top_share(fraction) == expected


def test_standing_phrase_switches_below_median():
    assert standing_phrase(0.771) == "in the top 23%"
    assert standing_phrase(0.5) == "in the top 50%"
    assert standing_phrase(0.312) == "ahead of 31.2%"
    assert standing_phrase(None) is None


MILANFAR = {
    "name": "Peyman Milanfar",
    "citedby": 38080, "citedby_percentile": 0.799,
    "hindex": 90, "hindex_percentile": 0.86,
    "i10index": 172, "i10index_percentile": 0.707,
    "total_publications": 282,
    "total_publications_with_citations": 237,
    "total_publications_with_citations_percentile": 0.682,
    "pip_auc_score": 0.4272, "pip_auc_score_percentile": 0.771,
    "year_of_first_pub": 1990,
}


def test_build_standing_gs_wording_and_metric_order():
    s = build_standing(MILANFAR, 237, "gs")
    assert s["phrase"] == "in the top 23%"
    assert s["group"] == "tracked active authors"
    assert s["cohort"] == "whose first cited publication was in 1990"
    assert s["pip_ordinal"] == "77.1st"
    assert s["pip_score"] == 0.4272
    assert [m["label"] for m in s["metrics"]] == [
        "h-index", "Total citations", "i10-index", "Cited publications"]
    assert s["metrics"][0]["ordinal"] == "86th"
    assert s["metrics"][3]["value"] == 237
    assert (s["num_scored"], s["num_records"]) == (237, 282)


def test_build_standing_s2_wording():
    s = build_standing(MILANFAR, 237, "s2")
    assert s["group"] == "active Semantic Scholar authors"
    assert s["cohort"] == "whose first publication was in 1990"


def test_build_standing_treats_nan_as_missing():
    stats = dict(MILANFAR, pip_auc_score=float("nan"), hindex_percentile=float("nan"),
                 year_of_first_pub=None)
    s = build_standing(stats, 0, "gs")
    assert s["has_pip"] is False
    assert s["phrase"] is None and s["pip_score"] is None
    assert s["cohort"] is None
    assert "h-index" not in [m["label"] for m in s["metrics"]]
