"""Google Scholar author IDs from Wikidata (property P1960), for name search.

Google Scholar's author search needs a signed-in browser, and SerpApi's Scholar
results no longer link author names to profiles, so the app cannot look up an
unprofiled author's GS ID by name. Wikidata maps ~130K people to their GS IDs;
loading that mapping into `gs_data.wikidata_authors` lets the search index offer
those names too (the results page then crawls the profile on demand).

The full query times out on the public endpoint (60 s), so it runs once per
leading character of the GS ID, splitting a partition further when it times out.
"""

import csv
import io
import logging
import re
import time
from datetime import datetime, timezone

import requests
from google.cloud import bigquery

from gs_crawler.config import Config

logger = logging.getLogger(__name__)

SPARQL_URL = "https://query.wikidata.org/sparql"
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
]


class WikidataError(Exception):
    pass


# English Wikipedia pageviews (last 60 days) rank which unprofiled authors the
# refresh job ingests first. The Action API takes 50 titles per request, so
# ~22K articles need ~450 requests; they are paced, and views are best-effort:
# a failed batch leaves NULL and ingestion falls back to the sitelinks count.
WIKI_API_URL = "https://en.wikipedia.org/w/api.php"
PAGEVIEW_BATCH = 50
PAGEVIEW_DAYS = 60
PAGEVIEW_PAUSE = 1.0  # seconds between requests
PAGEVIEW_MAX_SECONDS = 1500  # keep the job well inside its 1 h task timeout


def _query(prefix):
    # STRSTARTS is case-sensitive, so each prefix is its own partition.
    return f"""
SELECT ?p ?g ?label ?desc ?links ?enwiki WHERE {{
  ?p wdt:P1960 ?g .
  FILTER(STRSTARTS(?g, "{prefix}"))
  ?p rdfs:label ?label . FILTER(LANG(?label) = "en")
  OPTIONAL {{ ?p wikibase:sitelinks ?links }}
  OPTIONAL {{ ?p schema:description ?desc . FILTER(LANG(?desc) = "en") }}
  OPTIONAL {{ ?art schema:about ?p ; schema:isPartOf <https://en.wikipedia.org/> ; schema:name ?enwiki }}
}}"""


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


def _fetch_partition(prefix, session):
    rows = _fetch(prefix, session)
    if rows is not None:
        return rows
    if len(prefix) >= MAX_PREFIX_LEN:
        raise WikidataError(f"prefix {prefix!r} still times out")
    logger.info("Wikidata prefix %r timed out; splitting", prefix)
    out = []
    for c in ID_CHARS:
        out.extend(_fetch_partition(prefix + c, session))
    return out


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
            "fetched_at": fetched_at,
        })
    return out


def _pageview_batch(titles, session):
    """{requested title: views over PAGEVIEW_DAYS} for one batch; None on failure."""
    params = {"action": "query", "prop": "pageviews", "pvipdays": PAGEVIEW_DAYS,
              "titles": "|".join(titles), "redirects": 1, "format": "json", "formatversion": 2}
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = session.get(WIKI_API_URL, params=params, headers={"User-Agent": USER_AGENT}, timeout=60)
        except requests.RequestException as e:
            logger.warning("Pageviews batch: %s (attempt %d)", type(e).__name__, attempt + 1)
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 429:
            time.sleep(min(int(r.headers.get("Retry-After") or 30), 120))
            continue
        if r.status_code != 200:
            return None
        try:
            query = r.json().get("query") or {}
        except ValueError:
            return None
        # Map the titles the API resolved back to the ones we asked for.
        back = {t: t for t in titles}
        for step in ("normalized", "redirects"):
            for m in query.get(step) or []:
                if m.get("from") in back:
                    back[m["to"]] = back.pop(m["from"])
        views = {}
        for page in query.get("pages") or []:
            asked = back.get(page.get("title"))
            daily = page.get("pageviews")
            if asked is not None and isinstance(daily, dict):
                views[asked] = sum(v or 0 for v in daily.values())
        return views
    return None


def fetch_pageviews(titles, session=None, pause=PAGEVIEW_PAUSE, max_seconds=PAGEVIEW_MAX_SECONDS):
    """{title: views} for as many titles as the API returns; never raises."""
    session = session or requests.Session()
    titles = sorted(set(titles))
    views, failed = {}, 0
    deadline = time.monotonic() + max_seconds
    for i in range(0, len(titles), PAGEVIEW_BATCH):
        if time.monotonic() > deadline:
            logger.warning("Pageviews: time limit reached after %d titles", i)
            break
        batch = _pageview_batch(titles[i:i + PAGEVIEW_BATCH], session)
        if batch is None:
            failed += 1
            if failed >= 10 and not views:
                logger.warning("Pageviews unavailable; leaving them NULL")
                break
        else:
            views.update(batch)
        time.sleep(pause)
    logger.info("Pageviews for %d of %d articles (%d failed batches)", len(views), len(titles), failed)
    return views


def fetch_all(session=None):
    session = session or requests.Session()
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    raw = []
    for c in ID_CHARS:
        raw.extend(_fetch_partition(c, session))
    rows = normalize(raw, fetched_at)
    views = fetch_pageviews([r["enwiki_title"] for r in rows if r["enwiki_title"]], session)
    for r in rows:
        r["enwiki_views_60d"] = views.get(r["enwiki_title"])
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
    return load(fetch_all())
