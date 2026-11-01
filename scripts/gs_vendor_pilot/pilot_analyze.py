"""Analysis phase of the GS vendor pilot.

Usage (after pilot_run.py, and exporting legacy histograms for the sampled ids to $PILOT_DIR/legacy.json):
    python3 scripts/gs_vendor_pilot/pilot_analyze.py $PILOT_DIR
"""
import collections, json, os, statistics, sys

D = sys.argv[1]
R = json.load(open(f"{D}/results.json"))
L = {r["author_pub_id"]: r for r in json.load(open(f"{D}/legacy.json"))}
calls = [json.loads(l) for l in open(f"{D}/raw/calls.jsonl")]
NAMES = {"Av4MFGAAAAAJ": "Pandey", "7Xko5sYAAAAJ": "Kamar", "PA9La6oAAAAJ": "Ipeirotis",
         "WF5c0_8AAAAJ": "Barrett", "F4URLXYAAAAJ": "Dale"}
LEGACY_N = {"Av4MFGAAAAAJ": 55, "7Xko5sYAAAAJ": 131, "PA9La6oAAAAJ": 209, "WF5c0_8AAAAJ": 606, "F4URLXYAAAAJ": 1314}


def q(xs, p):
    xs = sorted(xs)
    return xs[min(int(p * len(xs)), len(xs) - 1)] if xs else None


print("== Calls ==")
for v in ("serpapi", "searchapi", "scraperapi"):
    for kind in ("author", "citation", "profile"):
        cs = [c for c in calls if c["vendor"] == v and c["label"].startswith(kind)]
        if not cs:
            continue
        ok = [c for c in cs if c["status"] == 200]
        print(f"  {v:10s} {kind:8s} n={len(cs):3d} ok={len(ok):3d} p50={q([c['secs'] for c in cs], .5):5.1f}s "
              f"p95={q([c['secs'] for c in cs], .95):5.1f}s max={max(c['secs'] for c in cs):5.1f}s")

print("\n== Profiles ==")
for aid, nm in NAMES.items():
    pages = sorted((int(k.split(":")[2]), v) for k, v in R["serpapi"].items() if k.startswith(f"author:{aid}:"))
    arts = [a for _, v in pages for a in v["data"].get("articles", [])]
    first = pages[0][1]["data"]
    tbl = first.get("cited_by", {}).get("table", [])
    au = first.get("author", {})
    ids = [a["citation_id"] for a in arts]
    empty = sum(1 for _, v in pages if v["status"] == 200 and not v["data"].get("articles"))
    line = (f"  {nm:9s} serpapi: {len(arts):4d} articles / {len(pages)} pages (legacy {LEGACY_N[aid]}), dup ids {len(ids) - len(set(ids))}, "
            f"empty pages {empty}, table {[list(t)[0] + ':' + ','.join(list(t[list(t)[0]])) for t in tbl]}, "
            f"graph yrs {len(first.get('cited_by', {}).get('graph', []))}, coauthors {len(first.get('co_authors', []))}, "
            f"email {'yes' if au.get('email') else 'no'}, interests {len(au.get('interests', []))}")
    sa = sorted((int(k.split(":")[2]), v) for k, v in R["searchapi"].items() if k.startswith(f"author:{aid}:"))
    if sa:
        sarts = [a for _, v in sa for a in v["data"].get("articles", [])]
        sids = {a["citation_id"] for a in sarts}
        sc = {a["citation_id"]: (a.get("cited_by") or {}).get("total") for a in sarts}
        pc = {a["citation_id"]: (a.get("cited_by") or {}).get("value") for a in arts}
        same = sum(1 for c in sids & set(ids) if (sc[c] or 0) == (pc[c] or 0))
        line += (f"\n            searchapi: {len(sarts)} articles / {len(sa)} pages; ids in both {len(sids & set(ids))}, "
                 f"only serpapi {len(set(ids) - sids)}, only searchapi {len(sids - set(ids))}, same num_citations {same}/{len(sids & set(ids))}")
        gs = {x["year"]: x["citations"] for x in first.get("cited_by", {}).get("graph", [])}
        ga = {x["year"]: x["cites"] for x in sa[0][1]["data"].get("cited_by", {}).get("histogram", [])}
        line += f"; author chart identical: {gs == ga} ({len(gs)} yrs, gaps in serpapi chart: {sum(1 for y in range(min(gs), max(gs) + 1) if y not in gs) if gs else 0})"
    print(line)


def hist_serp(c):
    t = c["data"].get("citation", {}).get("total_citations", {})
    return {int(x["year"]): int(x["citations"]) for x in t.get("table", [])}, (t.get("cited_by") or {}).get("total")


def hist_sa(c):
    cz = c["data"].get("citation", {})
    return {int(x["year"]): int(x["cites"]) for x in cz.get("cites_histogram", [])}, (cz.get("cited_by") or {}).get("total")


print("\n== Per-paper histograms ==")
stats = collections.Counter()
ratios, legacy_drop, gaps = [], [], collections.Counter()
for aid, cids in R["sample"].items():
    for cid in cids:
        s = R["serpapi"].get(f"citation:{cid}")
        if not s:
            continue
        stats["serp_calls"] += 1
        if s["status"] != 200 or "citation" not in s["data"]:
            stats["serp_fail"] += 1
            continue
        hs, ts = hist_serp(s)
        if ts and not hs:
            stats["serp_total_but_no_hist"] += 1
        if ts:
            ratios.append(sum(hs.values()) / ts)
        if hs:
            ys = sorted(hs)
            gaps["serp_missing_years_inside_range"] += sum(1 for y in range(ys[0], ys[-1] + 1) if y not in hs)
            gaps["serp_zero_years_listed"] += sum(1 for v in hs.values() if v == 0)
        a = R["searchapi"].get(f"citation:{cid}")
        if a:
            stats["sa_calls"] += 1
            if a["status"] != 200 or "citation" not in a["data"]:
                stats["sa_fail"] += 1
            else:
                ha, ta = hist_sa(a)
                stats["sa_vs_serp_same_hist"] += ha == hs
                stats["sa_vs_serp_same_total"] += ta == ts
                if ha != hs:
                    diff = {y: (hs.get(y), ha.get(y)) for y in set(hs) | set(ha) if hs.get(y) != ha.get(y)}
                    print(f"  DIFF {cid}: {dict(sorted(diff.items()))}")
        lg = L.get(cid)
        if lg and lg.get("cites_per_year"):
            hl = {int(k): v for k, v in json.loads(lg["cites_per_year"]).items()}
            stats["legacy_compared"] += 1
            yrs_drop = [y for y in hl if hl[y] > hs.get(y, 0)]
            legacy_drop.append(sum(max(hl[y] - hs.get(y, 0), 0) for y in hl) / max(sum(hl.values()), 1))
            stats["legacy_years_decreased_any"] += bool(yrs_drop)
            stats["legacy_total_now_ge_then"] += (ts or 0) >= int(lg["num_citations"] or 0)
print(" ", dict(stats))
print("  histogram sum / cited_by total: median", round(statistics.median(ratios), 3), "min", round(min(ratios), 3), "n", len(ratios))
print("  ", dict(gaps))
print("  vs legacy (Apr-May 2025): share of legacy citations no longer in the same year, median",
      round(statistics.median(legacy_drop), 4), "p90", round(q(legacy_drop, .9), 4))
