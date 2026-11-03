"""Tests for the plain-language percentile formatting on the results page."""

import pytest

from frontend.formatting import (
    build_standing,
    field_comparison,
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
    s = build_standing(MILANFAR, "gs")
    assert s["phrase"] == "in the top 23%"
    assert s["group"] == "tracked active authors"
    assert s["cohort"] == "whose career started in 1990"
    assert s["pip_ordinal"] == "77.1st"
    assert s["pip_score"] == 0.4272
    assert [m["label"] for m in s["metrics"]] == [
        "h-index", "Total citations", "i10-index", "Cited publications"]
    assert s["metrics"][0]["ordinal"] == "86th"
    assert s["metrics"][3]["value"] == 237
    assert (s["num_cited"], s["num_records"]) == (237, 282)


def test_build_standing_s2_wording():
    s = build_standing(MILANFAR, "s2")
    assert s["group"] == "active Semantic Scholar authors"
    assert s["cohort"] == "whose career started in 1990"


def _row(group, pct, peers=200, start=1988, end=1992):
    return {"field_group": group, "cohort_start": start, "cohort_end": end,
            "peer_count": peers, "percentile": pct}


FIELDS = {
    "field_group": "Computer Science", "field_source": "interests",
    "field_percentiles": [
        _row("Computer Science", 0.6547, peers=412),
        _row("Economics & Business", 0.95, peers=243, start=1990, end=1990),
        _row("Physics & Astronomy", None, peers=21),  # window too thin to rank
    ],
}


def test_field_comparison_starts_on_the_authors_field():
    fc = build_standing(dict(MILANFAR, **FIELDS), "gs")["field"]
    assert fc["own"] == "Computer Science" and fc["selected"] == "Computer Science"
    assert fc["own_from_coauthors"] is False
    assert fc["current"] == {
        "name": "Computer Science", "rankable": True, "phrase": "in the top 35%",
        "peers": 412, "cohort": "whose career started 1988\u20131992",
    }
    assert [c["name"] for c in fc["choices"]] == [
        "Computer Science", "Economics & Business", "Physics & Astronomy"]
    econ, physics = fc["choices"][1], fc["choices"][2]
    assert econ["phrase"] == "in the top 5%" and econ["cohort"] == "whose career started in 1990"
    assert physics["rankable"] is False and physics["phrase"] is None


def test_field_comparison_honours_a_requested_field():
    stats = dict(MILANFAR, **FIELDS)
    assert build_standing(stats, "gs", field="Economics & Business")["field"]["selected"] == \
        "Economics & Business"
    # Unknown or unrankable requests fall back to the author's own field
    for bad in ("Astrology", "Physics & Astronomy", "", None):
        assert field_comparison(stats, bad)["selected"] == "Computer Science"


def test_field_comparison_without_an_own_field_waits_for_a_choice():
    # Firestore keeps a NULL field_group as NaN
    stats = dict(MILANFAR, **dict(FIELDS, field_group=float("nan"), field_source=float("nan")))
    fc = field_comparison(stats)
    assert fc["own"] is None and fc["selected"] is None and fc["current"] is None
    assert field_comparison(stats, "Economics & Business")["selected"] == "Economics & Business"
    # Own field present but too thin to rank: nothing selected either
    thin = dict(FIELDS, field_group="Physics & Astronomy", field_source="coauthors")
    fc = field_comparison(thin)
    assert fc["own"] == "Physics & Astronomy" and fc["own_from_coauthors"] is True
    assert fc["selected"] is None


def test_field_comparison_missing():
    assert field_comparison(MILANFAR) is None  # cache entry from before field percentiles
    assert field_comparison(dict(FIELDS, field_percentiles=[])) is None
    # No overall PiP score: no field comparison either
    assert build_standing(dict(MILANFAR, **FIELDS, pip_auc_score=None), "gs")["field"] is None


def test_build_standing_treats_nan_as_missing():
    stats = dict(MILANFAR, pip_auc_score=float("nan"), hindex_percentile=float("nan"),
                 year_of_first_pub=None)
    s = build_standing(stats, "gs")
    assert s["has_pip"] is False
    assert s["phrase"] is None and s["pip_score"] is None
    assert s["cohort"] is None
    assert "h-index" not in [m["label"] for m in s["metrics"]]
