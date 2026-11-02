"""Normalize SerpApi google_scholar_author pages into gs_data snapshot rows.

Rows match the gs_data.author_snapshots / publication_snapshots schemas
(bigquery/gs_data/tables.sql). Column names follow the legacy
scholar_raw_data tables (scholar_id, author_pub_id, citedby, hindex,
i10index, email_domain, pub_year, num_citations) so legacy snapshots and
fresh crawls can be unioned.
"""

import json
import re

_SINCE_KEY = re.compile(r"^since_(\d{4})$")
_YEAR = re.compile(r"(1[89]\d\d|20\d\d)")


def _metric(table, name):
    """Return (all, since, since_year) for a cited_by.table metric."""
    for row in table or []:
        if name in row:
            values = row[name] or {}
            since, since_year = None, None
            for key, value in values.items():
                m = _SINCE_KEY.match(key)
                if m:
                    since, since_year = value, int(m.group(1))
            return values.get("all"), since, since_year
    return None, None, None


def _email_domain(email):
    """'Verified email at stern.nyu.edu' -> '@stern.nyu.edu' (legacy format)."""
    if not email:
        return None
    m = re.search(r"at\s+(\S+)", email)
    return f"@{m.group(1)}" if m else None


def _int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _pub_year(article):
    year = _int(article.get("year"))
    if year is None:
        # Some articles carry the year only in the publication string.
        m = _YEAR.findall(article.get("publication") or "")
        year = int(m[-1]) if m else None
    return year


def parse_author(pages, scholar_id, fetched_at):
    """Build (author_row, publication_rows) from a profile's pages, first page first.

    `fetched_at` is an ISO-8601 UTC timestamp string shared by all rows.
    """
    first = pages[0]
    author = first.get("author") or {}
    cited_by = first.get("cited_by") or {}
    table = cited_by.get("table")
    citedby, citedby_since, since_year = _metric(table, "citations")
    hindex, hindex_since, _ = _metric(table, "h_index")
    i10index, i10index_since, _ = _metric(table, "i10_index")
    cites_per_year = {str(g["year"]): g["citations"] for g in cited_by.get("graph") or [] if "year" in g}

    publications, seen = [], set()
    for page in pages:
        for article in page.get("articles") or []:
            pub_id = article.get("citation_id")
            if not pub_id or pub_id in seen:
                continue
            seen.add(pub_id)
            publications.append({
                "scholar_id": scholar_id,
                "author_pub_id": pub_id,
                "fetched_at": fetched_at,
                "source": "serpapi",
                "title": article.get("title"),
                "authors": article.get("authors"),
                "venue": article.get("publication"),
                "pub_year": _pub_year(article),
                "num_citations": _int((article.get("cited_by") or {}).get("value")) or 0,
            })

    author_row = {
        "scholar_id": scholar_id,
        "fetched_at": fetched_at,
        "source": "serpapi",
        "name": author.get("name"),
        "affiliation": author.get("affiliations"),
        "email_domain": _email_domain(author.get("email")),
        "interests": [i.get("title") for i in author.get("interests") or [] if i.get("title")],
        "citedby": _int(citedby),
        "hindex": _int(hindex),
        "i10index": _int(i10index),
        "citedby_since": _int(citedby_since),
        "hindex_since": _int(hindex_since),
        "i10index_since": _int(i10index_since),
        "since_year": since_year,
        "cites_per_year": json.dumps(cites_per_year, sort_keys=True),
        "coauthors": [
            {"scholar_id": c.get("author_id"), "name": c.get("name"), "affiliation": c.get("affiliations")}
            for c in first.get("co_authors") or [] if c.get("author_id")
        ],
        "num_publications": len(publications),
    }
    return author_row, publications
