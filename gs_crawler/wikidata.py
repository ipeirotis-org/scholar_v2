"""Google Scholar author IDs from Wikidata (property P1960), for name search.

Google Scholar's author search needs a signed-in browser, and SerpApi's Scholar
results no longer link author names to profiles, so the app cannot look up an
unprofiled author's GS ID by name. Wikidata maps ~130K people to their GS IDs;
loading that mapping into `gs_data.wikidata_authors` lets the search index offer
those names too (the results page then crawls the profile on demand).

The full query runs in ~20 s on QLever (qlever.dev, University of Freiburg),
which serves a recent Wikidata snapshot without the 60 s limit. If QLever fails,
the job falls back to the Wikidata Query Service, where the query only fits when
run once per leading character of the GS ID (split further on timeouts, with a
back-off, since WDQS also times out when it throttles a busy client).
"""

import csv
import io
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import requests
from google.cloud import bigquery

from gs_crawler.config import Config

logger = logging.getLogger(__name__)

SPARQL_URL = "https://query.wikidata.org/sparql"
QLEVER_URL = "https://qlever.dev/api/wikidata"
# Sanity floor for a full QLever result (~125K rows with English labels, 2026-11).
MIN_FULL_ROWS = 50000
THROTTLE_PAUSE = 60  # seconds to wait before retrying a small partition on WDQS
USER_AGENT = "pip-score.org/1.0 (https://www.pip-score.org; Google Scholar ID lookup)"
GS_ID_RE = re.compile(r"^[A-Za-z0-9_-]{12}$")
ID_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
MAX_PREFIX_LEN = 3
MAX_ATTEMPTS = 3

SCHEMA = [
    bigquery.SchemaField("scholar_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("wikidata_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("sitelinks", "INTEGER"),
    bigquery.SchemaField("enwiki_title", "STRING"),
    bigquery.SchemaField("enwiki_views_60d", "INTEGER"),
    bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("died", "BOOLEAN"),
    bigquery.SchemaField("is_scholarly", "BOOLEAN"),
    bigquery.SchemaField("is_politician", "BOOLEAN"),
    bigquery.SchemaField("views_fetched_at", "TIMESTAMP"),
]


class WikidataError(Exception):
    pass


# English Wikipedia pageviews (last 60 days) rank which unprofiled authors the
# refresh job ingests first. The Action API's prop=pageviews returns only ~5
# titles per request before asking for a continuation (the first production run
# got views for 2.4K of 22K articles), so this uses the per-article REST endpoint,
# one request per article from a few parallel workers. Views are best-effort:
# titles go in order of sitelinks, so a time limit leaves out the least known,
# and a missing value is NULL (ingestion then falls back to the sitelinks count).
PAGEVIEW_URL = ("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
                "en.wikipedia.org/all-access/user/{title}/daily/{start}/{end}")
PAGEVIEW_DAYS = 60
PAGEVIEW_WORKERS = 8
# Wikimedia throttles this endpoint to ~3.5 articles/s for us (2026-11), so one run
# covers ~8K of the ~22K articles. Views are carried forward between runs and the
# titles without any go first, so coverage is complete after about three runs.
PAGEVIEW_MAX_SECONDS = 2400  # leaves ~20 min of the job's 1 h task timeout


_PREFIXES = """PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX wikibase: <http://wikiba.se/ontology#>
"""


# Ingestion is limited to living people with a scholarly occupation (or a subclass):
# researcher, engineer, academic, university teacher, physician. Without this the
# most-viewed candidates were Einstein, Newton, Marx, Xi Jinping and Joe Biden, who
# would waste the budget and distort the percentile reference population.
# Politicians (Q82955) are left out even when they also hold an academic occupation.
SCHOLARLY_OCCUPATIONS = "wd:Q1650915 wd:Q81096 wd:Q3400985 wd:Q1622272 wd:Q39631"


def _query(prefix=None):
    """The P1960 query; with `prefix`, only GS IDs starting with it (case-sensitive)."""
    gs_filter = f'FILTER(STRSTARTS(?g, "{prefix}"))' if prefix else ""
    return _PREFIXES + f"""
SELECT ?p ?g ?label ?desc ?links ?enwiki ?dead ?scholarly ?politician WHERE {{
  ?p wdt:P1960 ?g .
  {gs_filter}
  ?p rdfs:label ?label . FILTER(LANG(?label) = "en")
  OPTIONAL {{ ?p wikibase:sitelinks ?links }}
  OPTIONAL {{ ?p schema:description ?desc . FILTER(LANG(?desc) = "en") }}
  OPTIONAL {{ ?art schema:about ?p ; schema:isPartOf <https://en.wikipedia.org/> ; schema:name ?enwiki }}
  OPTIONAL {{ ?p wdt:P570 ?death }}
  BIND(BOUND(?death) AS ?dead)
  BIND(EXISTS {{ VALUES ?occ {{ {SCHOLARLY_OCCUPATIONS} }} ?p wdt:P106/wdt:P279* ?occ }} AS ?scholarly)
  BIND(EXISTS {{ ?p wdt:P106/wdt:P279* wd:Q82955 }} AS ?politician)
}}"""


def _fetch_qlever(session):
    """All rows in one QLever query, or None if it fails or looks incomplete."""
    try:
        r = session.get(QLEVER_URL, params={"query": _query()},
                        headers={"Accept": "text/csv", "User-Agent": USER_AGENT}, timeout=300)
    except requests.RequestException as e:
        logger.warning("QLever: %s; falling back to WDQS", type(e).__name__)
        return None
    if r.status_code != 200 or not r.text.startswith("p,g,"):
        logger.warning("QLever: HTTP %s; falling back to WDQS", r.status_code)
        return None
    rows = list(csv.DictReader(io.StringIO(r.text)))
    if len(rows) < MIN_FULL_ROWS:
        logger.warning("QLever: only %d rows; falling back to WDQS", len(rows))
        return None
    logger.info("QLever: %d rows", len(rows))
    return rows


def _fetch(prefix, session):
    """Rows for GS IDs starting with `prefix`; None if the query timed out."""
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = session.get(SPARQL_URL, params={"query": _query(prefix)},
                            headers={"Accept": "text/csv", "User-Agent": USER_AGENT},
                            timeout=90)
        except requests.RequestException as e:
            logger.warning("Wikidata prefix %r: %s (attempt %d)", prefix, type(e).__name__, attempt + 1)
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", "30")))
            continue
        # A timed-out query comes back as 500, or as a truncated 200 ending in a Java trace.
        if r.status_code >= 500 or "TimeoutException" in r.text[-4000:]:
            return None
        if r.status_code != 200:
            raise WikidataError(f"prefix {prefix!r}: HTTP {r.status_code}")
        return list(csv.DictReader(io.StringIO(r.text)))
    raise WikidataError(f"prefix {prefix!r}: gave up after {MAX_ATTEMPTS} attempts")


def _fetch_partition(prefix, session, pause=THROTTLE_PAUSE):
    rows = _fetch(prefix, session)
    # A partition of a few hundred IDs times out only when WDQS throttles us:
    # back off and retry before splitting it further.
    for attempt in range(MAX_ATTEMPTS if len(prefix) >= 2 else 0):
        if rows is not None:
            break
        logger.info("Wikidata prefix %r timed out; retrying in %ds", prefix, pause)
        time.sleep(pause * (attempt + 1))
        rows = _fetch(prefix, session)
    if rows is not None:
        return rows
    if len(prefix) >= MAX_PREFIX_LEN:
        raise WikidataError(f"prefix {prefix!r} still times out")
    logger.info("Wikidata prefix %r timed out; splitting", prefix)
    out = []
    for c in ID_CHARS:
        out.extend(_fetch_partition(prefix + c, session, pause))
    return out


def _bool(value):
    value = (value or "").strip().lower()
    return True if value in ("true", "1") else False if value in ("false", "0") else None


def _int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize(raw_rows, fetched_at):
    """CSV rows -> table rows: valid GS IDs only, one row per GS ID."""
    out, seen = [], set()
    for r in raw_rows:
        gs_id = (r.get("g") or "").strip()
        name = (r.get("label") or "").strip()
        if not GS_ID_RE.match(gs_id) or not name or gs_id in seen:
            continue
        seen.add(gs_id)
        out.append({
            "scholar_id": gs_id,
            "wikidata_id": (r.get("p") or "").rsplit("/", 1)[-1],
            "name": name,
            "description": (r.get("desc") or "").strip() or None,
            "sitelinks": _int(r.get("links")),
            "enwiki_title": (r.get("enwiki") or "").strip() or None,
            "enwiki_views_60d": None,
            "views_fetched_at": None,
            "fetched_at": fetched_at,
            "died": _bool(r.get("dead")),
            "is_scholarly": _bool(r.get("scholarly")),
            "is_politician": _bool(r.get("politician")),
        })
    return out


def _pageview_window(today=None):
    """(start, end) as YYYYMMDD: the PAGEVIEW_DAYS full days before today (UTC)."""
    today = today or datetime.now(timezone.utc).date()
    end = today - timedelta(days=1)
    start = end - timedelta(days=PAGEVIEW_DAYS - 1)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def _article_views(title, session, window, deadline=None):
    """Views of one English Wikipedia article over `window`; 0 if the API has no
    data for it, None on failure or once `deadline` (time.monotonic()) has passed."""
    url = PAGEVIEW_URL.format(title=quote(title.replace(" ", "_"), safe=""),
                              start=window[0], end=window[1])

    def pause(seconds):
        if deadline is not None:
            seconds = min(seconds, deadline - time.monotonic())
        if seconds > 0:
            time.sleep(seconds)

    for attempt in range(MAX_ATTEMPTS):
        if deadline is not None and time.monotonic() > deadline:
            return None
        try:
            r = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        except requests.RequestException:
            pause(2 * (attempt + 1))
            continue
        if r.status_code == 429 or r.status_code >= 500:
            try:
                wait = int(r.headers.get("Retry-After") or 0)
            except ValueError:
                wait = 0
            pause(min(max(wait, 2 * (attempt + 1)), 60))
            continue
        if r.status_code == 404:
            return 0  # no recorded views (new or rarely read article)
        if r.status_code != 200:
            return None
        try:
            items = r.json().get("items") or []
        except ValueError:
            return None
        return sum(int(i.get("views") or 0) for i in items)
    return None


def fetch_pageviews(titles, session=None, workers=PAGEVIEW_WORKERS,
                    max_seconds=PAGEVIEW_MAX_SECONDS, window=None):
    """{title: views} for as many titles as time allows, in the given order; never raises."""
    window = window or _pageview_window()
    titles = list(dict.fromkeys(titles))
    local = threading.local()

    def views_of(title):
        if session is not None:
            s = session
        else:
            s = getattr(local, "session", None) or requests.Session()
            local.session = s
        # Queued titles past the deadline return at once instead of draining the chunk.
        return title, _article_views(title, s, window, deadline)

    views, failed = {}, 0
    deadline = time.monotonic() + max_seconds
    with ThreadPoolExecutor(max_workers=workers) as pool:
        # Submit in chunks so the deadline can stop the run without queuing everything.
        for i in range(0, len(titles), workers * 25):
            if time.monotonic() > deadline:
                logger.warning("Pageviews: time limit reached after %d titles", i)
                break
            for title, n in pool.map(views_of, titles[i:i + workers * 25]):
                if n is None:
                    failed += 1
                else:
                    views[title] = n
            if failed > 500 and not views:
                logger.warning("Pageviews unavailable; leaving them NULL")
                break
    logger.info("Pageviews for %d of %d articles (%d failed)", len(views), len(titles), failed)
    return views


def previous_views(client=None):
    """{enwiki_title: (views, views_fetched_at)} from the current table; {} if it is
    missing or unreadable. views_fetched_at is an ISO string, or None if unknown."""
    client = client or bigquery.Client(project=Config.PROJECT_ID)
    sql = f"""
        SELECT enwiki_title, ANY_VALUE(enwiki_views_60d) AS views,
               MAX(views_fetched_at) AS views_fetched_at
        FROM {Config.bq_table_ref(Config.WIKIDATA_AUTHORS_TABLE)}
        WHERE enwiki_title IS NOT NULL AND enwiki_views_60d IS NOT NULL
        GROUP BY enwiki_title
    """
    try:
        out = {}
        for row in client.query(sql).result():
            at = row["views_fetched_at"]
            out[row["enwiki_title"]] = (row["views"], at.isoformat(timespec="seconds") if at else None)
        return out
    except Exception:
        logger.warning("Could not read previous pageviews; fetching from scratch")
        return {}


def fetch_all(session=None, previous=None):
    """Rows for gs_data.wikidata_authors. `previous` ({title: (views, fetched_at)})
    is carried forward for articles this run does not reach."""
    session = session or requests.Session()
    previous = previous or {}
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    raw = _fetch_qlever(session)
    if raw is None:
        raw = []
        for c in ID_CHARS:
            raw.extend(_fetch_partition(c, session))
    rows = normalize(raw, fetched_at)
    # Articles without views first, then the stalest views (unknown age counts as
    # oldest), so every article is refreshed in turn; best-known first within ties,
    # so a time limit leaves out the least known.
    def order(r):
        prev = previous.get(r["enwiki_title"])
        if prev is None:
            return (0, "", -(r["sitelinks"] or 0))
        return (1, prev[1] or "", -(r["sitelinks"] or 0))

    with_article = sorted((r for r in rows if r["enwiki_title"]), key=order)
    views = fetch_pageviews([r["enwiki_title"] for r in with_article])
    for r in rows:
        title = r["enwiki_title"]
        if title in views:
            r["enwiki_views_60d"], r["views_fetched_at"] = views[title], fetched_at
        elif title in previous:
            r["enwiki_views_60d"], r["views_fetched_at"] = previous[title]
    logger.info("Pageviews known for %d of %d articles after this run",
                sum(1 for r in with_article if r["enwiki_views_60d"] is not None), len(with_article))
    return rows


def load(rows, client=None):
    """Replace gs_data.wikidata_authors. Refuses a load that shrinks the table by
    more than half, which would mean a broken fetch rather than real deletions."""
    client = client or bigquery.Client(project=Config.PROJECT_ID)
    table = Config.bq_table(Config.WIKIDATA_AUTHORS_TABLE)
    try:
        previous = client.get_table(table).num_rows or 0
    except Exception:
        previous = 0
    if len(rows) < previous / 2:
        raise WikidataError(f"fetched {len(rows)} rows, table has {previous}; not replacing")
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    client.load_table_from_json(rows, table, job_config=job_config).result()
    logger.info("Loaded %d Wikidata GS IDs into %s (was %d)", len(rows), table, previous)
    return len(rows)


def run():
    from gs_crawler.scheduler import wikidata_migration_sql
    client = bigquery.Client(project=Config.PROJECT_ID)
    try:
        client.query(wikidata_migration_sql()).result()  # previous_views reads views_fetched_at
    except Exception:
        logger.exception("wikidata_authors migration failed")
    return load(fetch_all(previous=previous_views(client)), client=client)
