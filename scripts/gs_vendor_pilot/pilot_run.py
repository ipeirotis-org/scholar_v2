"""Fetch phase of the GS vendor pilot.

Usage:
    PILOT_DIR=/tmp/gs_pilot python3 scripts/gs_vendor_pilot/pilot_run.py

Results are cached in $PILOT_DIR/results.json, so reruns don't spend quota.
Uses ~95 SerpApi searches and ~60 SearchApi credits (both fit the free tiers).
"""
import json, os, random, sys
sys.path.insert(0, os.path.dirname(__file__))
import pilot_client as pc

OUT = os.path.join(os.path.dirname(pc.RAW_DIR), "results.json")
R = json.load(open(OUT)) if os.path.exists(OUT) else {"serpapi": {}, "searchapi": {}}


def save():
    json.dump(R, open(OUT, "w"))


def once(vendor, key, fn):
    if key in R[vendor] and R[vendor][key]["status"] == 200:
        return R[vendor][key]
    s, b, dt = fn()
    try:
        d = json.loads(b)
    except ValueError:
        d = {"_unparsed": b[:500]}
    R[vendor][key] = {"status": s, "secs": dt, "data": d}
    save()
    return R[vendor][key]


PANEL = {  # author_id: (name, fetch full profile via searchapi?, citations sampled)
    "Av4MFGAAAAAJ": ("Omkant Pandey", True, 15),
    "7Xko5sYAAAAJ": ("Ece Kamar", True, 15),
    "PA9La6oAAAAJ": ("Panos Ipeirotis", False, 15),
    "WF5c0_8AAAAJ": ("Lisa Feldman Barrett", False, 10),
    "F4URLXYAAAAJ": ("Anders M. Dale", False, 10),
}
SEARCHAPI_CITATIONS = {"Av4MFGAAAAAJ", "7Xko5sYAAAAJ", "PA9La6oAAAAJ"}

# 1. SerpApi full profiles (100/page)
profiles = {}
for aid in PANEL:
    arts, start = [], 0
    while True:
        r = once("serpapi", f"author:{aid}:{start}", lambda: pc.serpapi_author(aid, start, 100))
        page = r["data"].get("articles", []) if r["status"] == 200 else []
        arts += page
        if r["status"] != 200 or len(page) < 100:
            break
        start += 100
    profiles[aid] = arts
    print(f"serpapi {PANEL[aid][0]}: {len(arts)} articles in {start // 100 + 1} pages")

# 2. SearchApi full profiles for small authors (20/page)
for aid, (name, full, _) in PANEL.items():
    if not full:
        continue
    n, page = 0, 1
    while True:
        r = once("searchapi", f"author:{aid}:{page}", lambda: pc.searchapi_author(aid, page))
        arts = r["data"].get("articles", []) if r["status"] == 200 else []
        n += len(arts)
        if r["status"] != 200 or not r["data"].get("pagination", {}).get("next") or not arts:
            break
        page += 1
    print(f"searchapi {name}: {n} articles in {page} pages")

# 3. Citation sample: top-5 cited, 8 random cited, 2 uncited (deterministic)
sample = {}
for aid, (name, _, k) in PANEL.items():
    arts = profiles[aid]
    def nc(a):
        return (a.get("cited_by") or {}).get("value") or 0
    cited = sorted([a for a in arts if nc(a) > 0], key=nc, reverse=True)
    uncited = [a for a in arts if nc(a) == 0]
    rnd = random.Random(aid)
    top = cited[:5]
    rest = rnd.sample(cited[5:], min(k - 7 if k >= 15 else k - 5, len(cited[5:])))
    unc = rnd.sample(uncited, min(2 if k >= 15 else 0, len(uncited)))
    sample[aid] = [a["citation_id"] for a in top + rest + unc]
R["sample"] = sample
save()

for aid, cids in sample.items():
    for cid in cids:
        once("serpapi", f"citation:{cid}", lambda: pc.serpapi_citation(cid))
        if aid in SEARCHAPI_CITATIONS:
            once("searchapi", f"citation:{cid}", lambda: pc.searchapi_citation(cid))
    print(f"citations fetched for {PANEL[aid][0]}: {len(cids)}")

print("quota left:", pc.accounts())
