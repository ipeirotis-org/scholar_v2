"""Plain-language formatting of percentiles for the author results page.

Percentiles arrive from the cache as fractions (0.771) and are shown as
ordinals ("77.1st") or as a top share ("top 23%").
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


def _trim(value):
    """Format a number with at most one decimal, dropping a trailing '.0'."""
    text = f"{value:.1f}"
    return text[:-2] if text.endswith(".0") else text


def _suffix(text):
    if "." not in text:
        n = int(text)
        if 10 <= n % 100 <= 20:
            return "th"
    return {"1": "st", "2": "nd", "3": "rd"}.get(text[-1], "th")


def ordinal_percentile(fraction):
    """0.771 -> '77.1st', 0.86 -> '86th', 0.682 -> '68.2nd'."""
    if fraction is None:
        return ""
    text = _trim(100 * float(fraction))
    return text + _suffix(text)


def top_share(fraction):
    """Share of the cohort at or above this percentile: 0.771 -> '23', 0.994 -> '0.6'."""
    share = max(0.1, 100 - 100 * float(fraction))
    if share >= 10:
        return str(int(round(share)))
    return _trim(share)


def standing_phrase(fraction):
    """'in the top 23%' for upper-half percentiles, 'ahead of 31%' below the median."""
    if fraction is None:
        return None
    if float(fraction) >= 0.5:
        return f"in the top {top_share(fraction)}%"
    return f"ahead of {_trim(100 * float(fraction))}%"


def _career_window(start, end):
    """'whose career started 1998\u20132002' (or 'in 1998'), or None when unknown."""
    start, end = _number(start), _number(end)
    if start is None or end is None:
        return None
    if int(start) == int(end):
        return f"whose career started in {int(start)}"
    return f"whose career started {int(start)}\u2013{int(end)}"


def field_comparison(stats, requested=None):
    """The PiP-AUC standing within each field group, for the results page's field picker.

    stats["field_percentiles"] (GS mode; see
    bigquery/gs_statistics/ranked_author_pip_scores_current.sql) lists every field
    group with the career-start window of its peers, their count and the author's
    percentile among them. The picker starts on `requested` (the ?field= URL
    parameter) when that group can be ranked, else on the author's own group
    (field_group), else on nothing, so the viewer picks.

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
            "peers": int(peers) if rankable else 0,
            "cohort": _career_window(row.get("cohort_start"), row.get("cohort_end")),
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


def build_standing(stats, data_source, field=None):
    """Collect what the results page headline and metric scale need.

    Everything comes from the author stats document, so the page stays
    consistent even when the publication list failed to load.

    Args:
        stats: cached author stats (percentiles as 0-1 fractions).
        data_source: "gs" or "s2"; picks the wording of the comparison group.
        field: field group to compare within first (the ?field= URL parameter).
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

    metrics = []
    for key, label, help_text in [
        ("hindex", "h-index",
         "Largest h such that h publications have at least h citations."),
        ("citedby", "Total citations", "Citations across all publications on the profile."),
        ("i10index", "i10-index", "Publications with at least 10 citations."),
        ("total_publications_with_citations", "Cited publications",
         "Publications with at least one citation. Only these enter the PiP score."),
    ]:
        value = _number(stats.get(key))
        pct = _number(stats.get(f"{key}_percentile"))
        if value is None or pct is None:
            continue
        metrics.append({
            "label": label,
            "help": help_text,
            "value": int(value),
            "position": round(100 * float(pct), 1),
            "ordinal": ordinal_percentile(pct),
        })

    return {
        "name": stats.get("name") or "This author",
        "group": group,
        "cohort": cohort,
        "has_pip": has_pip,
        "pip_score": pip_score if has_pip else None,
        "phrase": standing_phrase(pip_pct) if has_pip else None,
        "pip_position": round(100 * float(pip_pct), 1) if has_pip else None,
        "pip_ordinal": ordinal_percentile(pip_pct) if has_pip else None,
        "field": field_comparison(stats, field) if has_pip else None,
        "metrics": metrics,
        "num_cited": int(_number(stats.get("total_publications_with_citations")) or 0),
        "num_records": int(_number(stats.get("total_publications")) or 0),
    }
