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


def field_standing(stats):
    """The author's PiP-AUC standing among field peers, or None.

    The cache layer adds field_* columns in GS mode (see
    bigquery/gs_statistics/ranked_author_pip_scores_current.sql): the field
    group, the career-start window of the peers and their count, and the
    percentile among them. Missing when the author has no field, the window is
    too thin, or the cache entry predates field percentiles.
    """
    name = _text(stats.get("field_group"))
    pct = _number(stats.get("field_pip_auc_score_percentile"))
    peers = _number(stats.get("field_peer_count"))
    if not name or pct is None or not peers:
        return None
    start = _number(stats.get("field_cohort_start"))
    end = _number(stats.get("field_cohort_end"))
    if start is None or end is None:
        cohort = None
    elif int(start) == int(end):
        cohort = f"whose career started in {int(start)}"
    else:
        cohort = f"whose career started {int(start)}–{int(end)}"
    return {
        "name": name,
        "phrase": standing_phrase(pct),
        "peers": int(peers),
        "cohort": cohort,
        "from_coauthors": _text(stats.get("field_source")) == "coauthors",
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
        "field": field_standing(stats) if has_pip else None,
        "metrics": metrics,
        "num_cited": int(_number(stats.get("total_publications_with_citations")) or 0),
        "num_records": int(_number(stats.get("total_publications")) or 0),
    }
