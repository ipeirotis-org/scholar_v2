"""Plain-language formatting of percentiles for the author results page.

Percentiles arrive from the cache as fractions (0.771) and are shown in one
form everywhere: percentiles with one decimal, always ("77.1st percentile",
"86.0th percentile", "86.0" in table columns). The paper explorer
(static/js/paper_explorer.js) follows the same rule.
"""

import math


def _number(value):
    """Return value as a float, or None when missing or NaN (pandas fills gaps with NaN)."""
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(value) else value


def _text(value):
    """Return value as a non-empty string, or None (pandas stores NULL strings as NaN)."""
    if value is None or isinstance(value, float):
        return None
    text = str(value).strip()
    return text or None


def percentile_value(fraction):
    """Percentile for display, one decimal: 0.7713 -> 77.1, clamped to 0.1..99.9; None when missing.

    Clamped so the top author of a cohort reads "99.9th" rather than "100th" and
    the bottom one "0.1st" rather than "0th". Missing and non-finite values (cached
    NaN for authors without a PiP score) give None instead of raising.
    """
    value = _number(fraction)
    if value is None or math.isinf(value):
        return None
    return min(99.9, max(0.1, round(100 * value, 1)))


def percentile_text(fraction):
    """Template filter for table columns: '77.1' for 0.771, '86.0' for 0.86, '' when missing."""
    value = percentile_value(fraction)
    return "" if value is None else f"{value:.1f}"


def ordinal_percentile(fraction):
    """0.771 -> '77.1st', 0.86 -> '86.0th', 0.682 -> '68.2nd'; '' when missing."""
    value = percentile_value(fraction)
    if value is None:
        return ""
    text = f"{value:.1f}"
    return text + {"1": "st", "2": "nd", "3": "rd"}.get(text[-1], "th")


def standing_phrase(fraction):
    """'at the 77.1st percentile', or None when missing."""
    if fraction is None:
        return None
    return f"at the {ordinal_percentile(fraction)} percentile"


def _career_window(start, end):
    """'whose career started 1998\u20132002' (or 'in 1998'), or None when unknown."""
    start, end = _number(start), _number(end)
    if start is None or end is None:
        return None
    if int(start) == int(end):
        return f"whose career started in {int(start)}"
    return f"whose career started {int(start)}\u2013{int(end)}"


_METRICS = [
    ("hindex", "h-index", "Largest h such that h publications have at least h citations."),
    ("citedby", "Total citations", "Citations across all publications on the profile."),
    ("i10index", "i10-index", "Publications with at least 10 citations."),
    ("total_publications_with_citations", "Cited publications",
     "Publications with at least one citation. Only these enter the PiP score."),
]


def _metric_rows(stats, percentiles):
    """Rows for the metric scale: each metric's value from stats, its percentile from
    percentiles[f"{key}_percentile"] (the stats doc itself, or one field's entry)."""
    rows = []
    for key, label, help_text in _METRICS:
        value = _number(stats.get(key))
        pct = _number(percentiles.get(f"{key}_percentile"))
        if value is None or pct is None:
            continue
        rows.append({
            "label": label,
            "help": help_text,
            "value": int(value),
            "position": round(100 * pct, 1),
            "ordinal": ordinal_percentile(pct),
        })
    return rows


def field_comparison(stats, requested=None):
    """The standing within each field group, for the author's field page.

    stats["field_percentiles"] (GS mode; see
    bigquery/gs_statistics/ranked_author_pip_scores_current.sql and
    ranked_author_current.sql) lists every field group with the career-start window
    of its peers, their count, and the author's PiP-AUC and metric percentiles among
    them. The page starts on `requested` (the ?field= URL parameter) when that group
    can be ranked, else on the author's own group (field_group), else on nothing,
    so the viewer picks.

    Returns None when the cache entry has no field percentiles (S2 mode, no
    PiP score, or an entry cached before field percentiles existed).
    """
    choices = []
    for row in stats.get("field_percentiles") or []:
        if not isinstance(row, dict):
            continue
        name = _text(row.get("field_group"))
        if not name:
            continue
        pct = _number(row.get("percentile"))
        peers = _number(row.get("peer_count"))
        rankable = pct is not None and bool(peers)
        choices.append({
            "name": name,
            "rankable": rankable,
            "phrase": standing_phrase(pct) if rankable else None,
            "position": round(100 * pct, 1) if rankable else None,
            "ordinal": ordinal_percentile(pct) if rankable else None,
            "peers": int(peers) if rankable else 0,
            "cohort": _career_window(row.get("cohort_start"), row.get("cohort_end")),
            "metrics": _metric_rows(stats, row) if rankable else [],
        })
    if not choices:
        return None
    by_name = {c["name"]: c for c in choices}
    own = _text(stats.get("field_group"))
    own = own if own in by_name else None
    requested = _text(requested)
    if requested in by_name and by_name[requested]["rankable"]:
        selected = requested
    elif own and by_name[own]["rankable"]:
        selected = own
    else:
        selected = None
    return {
        "choices": choices,
        "own": own,
        "own_from_coauthors": _text(stats.get("field_source")) == "coauthors",
        "selected": selected,
        "current": by_name.get(selected),
    }


def build_standing(stats, data_source):
    """Collect what the results page headline and metric scale need.

    Everything comes from the author stats document, so the page stays
    consistent even when the publication list failed to load.

    Args:
        stats: cached author stats (percentiles as 0-1 fractions).
        data_source: "gs" or "s2"; picks the wording of the comparison group.
    """
    first_year = _number(stats.get("year_of_first_pub"))
    first_year = int(first_year) if first_year is not None else None
    if data_source == "gs":
        group = "tracked active authors"
    else:
        group = "active Semantic Scholar authors"
    # year_of_first_pub is the career-start year (in GS it skips stray early
    # items), matching the "Career start" label in the page header.
    cohort = f"whose career started in {first_year}" if first_year else None

    pip_score = _number(stats.get("pip_auc_score"))
    pip_pct = _number(stats.get("pip_auc_score_percentile"))
    has_pip = pip_score is not None and pip_pct is not None

    metrics = _metric_rows(stats, stats)
    # The field comparison lives on its own page; the results page links to it,
    # naming the author's own field when there is one.
    fields = field_comparison(stats) if has_pip else None

    return {
        "name": stats.get("name") or "This author",
        "group": group,
        "cohort": cohort,
        "has_pip": has_pip,
        "pip_score": pip_score if has_pip else None,
        "phrase": standing_phrase(pip_pct) if has_pip else None,
        "pip_position": round(100 * float(pip_pct), 1) if has_pip else None,
        "pip_ordinal": ordinal_percentile(pip_pct) if has_pip else None,
        "field_link": {"own": fields["own"]} if fields else None,
        "metrics": metrics,
        "num_cited": int(_number(stats.get("total_publications_with_citations")) or 0),
        "num_records": int(_number(stats.get("total_publications")) or 0),
    }
