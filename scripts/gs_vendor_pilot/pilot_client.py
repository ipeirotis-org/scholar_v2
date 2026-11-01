"""Thin clients for the GS vendor pilot (SerpApi, SearchApi, ScraperAPI).

Keys come from Secret Manager (see keys.py) and are scrubbed from everything written to disk.
Every call is logged with status and latency to $PILOT_DIR/raw/calls.jsonl.
"""
import json, os, re, sys, time, urllib.parse, urllib.request, hashlib

sys.path.insert(0, os.path.dirname(__file__))
import keys

K = keys.load()
RAW_DIR = os.path.join(os.environ.get("PILOT_DIR", "/tmp/gs_pilot"), "raw")
LOG = os.path.join(RAW_DIR, "calls.jsonl")
os.makedirs(RAW_DIR, exist_ok=True)


def _fetch(vendor, label, url, headers=None, timeout=120):
    t = time.time()
    status, body, err = None, b"", None
    try:
        req = urllib.request.Request(url, headers=headers or {})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            status, body = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, body = e.code, e.read()
    except Exception as e:  # noqa: BLE001
        err = keys.scrub(repr(e))
    dt = time.time() - t
    name = hashlib.sha1(f"{vendor}:{label}".encode()).hexdigest()[:12]
    path = os.path.join(RAW_DIR, vendor, f"{name}.raw")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(keys.scrub(body.decode("utf-8", "replace")).encode())
    with open(LOG, "a") as f:
        f.write(json.dumps({"vendor": vendor, "label": label, "status": status, "secs": round(dt, 2),
                            "bytes": len(body), "err": err, "file": path, "ts": time.time()}) + "\n")
    return status, body.decode("utf-8", "replace"), dt


def serpapi_author(author_id, start=0, num=100):
    q = urllib.parse.urlencode({"engine": "google_scholar_author", "author_id": author_id, "start": start,
                                "num": num, "hl": "en", "no_cache": "true", "api_key": K["serpapi"]})
    return _fetch("serpapi", f"author:{author_id}:{start}:{num}", f"https://serpapi.com/search.json?{q}")


def serpapi_citation(citation_id):
    q = urllib.parse.urlencode({"engine": "google_scholar_author", "view_op": "view_citation",
                                "citation_id": citation_id, "hl": "en", "no_cache": "true", "api_key": K["serpapi"]})
    return _fetch("serpapi", f"citation:{citation_id}", f"https://serpapi.com/search.json?{q}")


def searchapi_author(author_id, page=1, extra=None):
    p = {"engine": "google_scholar_author", "author_id": author_id, "page": page, "hl": "en"}
    p.update(extra or {})
    q = urllib.parse.urlencode(p)
    return _fetch("searchapi", f"author:{author_id}:{page}:{json.dumps(extra or {}, sort_keys=True)}",
                  f"https://www.searchapi.io/api/v1/search?{q}", {"Authorization": f"Bearer {K['searchapi']}"})


def searchapi_citation(citation_id):
    q = urllib.parse.urlencode({"engine": "google_scholar_author", "view_op": "view_citation",
                                "citation_id": citation_id, "hl": "en"})
    return _fetch("searchapi", f"citation:{citation_id}", f"https://www.searchapi.io/api/v1/search?{q}",
                  {"Authorization": f"Bearer {K['searchapi']}"})


def scraperapi_get(gs_url, label):
    q = urllib.parse.urlencode({"api_key": K["scraperapi"], "url": gs_url})
    return _fetch("scraperapi", label, f"https://api.scraperapi.com/?{q}", timeout=90)


def gs_profile_url(author_id, cstart=0, pagesize=100):
    return f"https://scholar.google.com/citations?user={author_id}&hl=en&cstart={cstart}&pagesize={pagesize}"


def gs_citation_url(citation_id):
    return f"https://scholar.google.com/citations?view_op=view_citation&hl=en&citation_for_view={citation_id}"


def accounts():
    out = {}
    s, b, _ = _fetch("meta", f"acct:serpapi:{time.time()}", f"https://serpapi.com/account.json?api_key={K['serpapi']}")
    d = json.loads(b); out["serpapi_left"] = d.get("total_searches_left")
    s, b, _ = _fetch("meta", f"acct:searchapi:{time.time()}", "https://www.searchapi.io/api/v1/me",
                     {"Authorization": f"Bearer {K['searchapi']}"})
    d = json.loads(b); out["searchapi_left"] = d.get("account", {}).get("remaining_credits")
    s, b, _ = _fetch("meta", f"acct:scraperapi:{time.time()}", f"https://api.scraperapi.com/account?api_key={K['scraperapi']}")
    d = json.loads(b); out["scraperapi_left"] = d.get("creditsLeft")
    return out
