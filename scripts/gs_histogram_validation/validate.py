"""Offline validation of extending legacy GS per-paper histograms.

For authors with a profile snapshot at t0 and t1 (>400 days apart) and per-paper
histogram snapshots near both, predict the t1 histograms from:
  - t0 per-paper histograms (H0)
  - t1 profile: per-paper totals (N1) and the author's yearly histogram (A1)
  - t1 histograms of papers that are new since t0 (assumed fetched in production)
and compare against the true t1 histograms (H1).

Estimators:
  E0 stale        H0 unchanged
  E1 time-split   growth g = N1 - N0 (change in the paper's GS total) split across years by elapsed time
  E2 author-IPF   same seed, raked to the author's yearly residuals (rows exact)
  E3 trend-IPF    seed uses each paper's own recent trend, raked like E2

Usage (results are in docs/google-scholar-data-sources.md):
    mkdir -p /tmp/gsval
    bq query --use_legacy_sql=false --format=json --max_rows=5000000 \
        < scripts/gs_histogram_validation/export_authors.sql > /tmp/gsval/authors.json
    bq query --use_legacy_sql=false --format=json --max_rows=5000000 \
        < scripts/gs_histogram_validation/export_pubs.sql > /tmp/gsval/pubs.json
    python3 scripts/gs_histogram_validation/validate.py /tmp/gsval
"""
import bisect, collections, datetime as dt, json, sys

D = sys.argv[1]
WIN = dt.timedelta(days=5)
T = dt.datetime.fromisoformat


def hist(s):
    if not s:
        return {}
    try:
        return {int(k): int(v) for k, v in json.loads(s).items()}
    except (ValueError, TypeError):
        return {}


A = json.load(open(f"{D}/authors.json"))
P = json.load(open(f"{D}/pubs.json"))
asn = collections.defaultdict(list)
for r in A:
    asn[r["sid"]].append(r)
psn = collections.defaultdict(list)
for r in P:
    psn[r["sid"]].append(r)


def nearest(snaps, t):
    """pid -> snapshot nearest to t within WIN."""
    best = {}
    for r in snaps:
        d = abs(T(r["ts"]) - t)
        if d <= WIN and (r["pid"] not in best or d < best[r["pid"]][0]):
            best[r["pid"]] = (d, r)
    return {k: v[1] for k, v in best.items()}


def year_fracs(t0, t1):
    out = {}
    for y in range(t0.year, t1.year + 1):
        a = max(t0, dt.datetime(y, 1, 1))
        b = min(t1, dt.datetime(y + 1, 1, 1))
        out[y] = max((b - a).total_seconds(), 0)
    tot = sum(out.values())
    return {y: v / tot for y, v in out.items()}


def ipf(seed, row_t, col_t, iters=200):
    """seed: {p: {y: w}}; rows exact, columns as shares scaled to the row total."""
    rows = list(seed)
    years = sorted({y for p in rows for y in seed[p]})
    tot_r = sum(row_t[p] for p in rows)
    tot_c = sum(max(col_t.get(y, 0), 0) for y in years)
    if tot_r <= 0:
        return {p: {y: 0.0 for y in seed[p]} for p in rows}
    if tot_c <= 0:
        col = None
    else:
        col = {y: max(col_t.get(y, 0), 0) * tot_r / tot_c for y in years}
    m = {p: dict(seed[p]) for p in rows}
    for _ in range(iters):
        for p in rows:  # rows
            s = sum(m[p].values())
            f = row_t[p] / s if s > 0 else 0
            for y in m[p]:
                m[p][y] *= f
        if col is None:
            break
        for y in years:  # columns
            s = sum(m[p][y] for p in rows)
            f = col[y] / s if s > 0 else 0
            for p in rows:
                m[p][y] *= f
    for p in rows:  # finish on rows so each paper's total is exact
        s = sum(m[p].values())
        f = row_t[p] / s if s > 0 else 0
        for y in m[p]:
            m[p][y] *= f
    return m


def hindex(cites):
    c = sorted(cites, reverse=True)
    return sum(1 for i, x in enumerate(c) if x >= i + 1)


records = []      # per-paper results
author_rows = []  # per-author results
author_year_rows = []  # per-author, per-window-year totals
diag = collections.Counter()

for sid, snaps in asn.items():
    snaps.sort(key=lambda r: r["ts"])
    a0, a1 = snaps[0], snaps[-1]
    t0, t1 = T(a0["ts"]), T(a1["ts"])
    if (t1 - t0).days <= 400:
        continue
    pubs0 = {p["pid"]: p for p in json.loads(a0["pubs"])}
    pubs1 = {p["pid"]: p for p in json.loads(a1["pubs"])}
    near0, near1 = nearest(psn[sid], t0), nearest(psn[sid], t1)
    if len(near0) < 0.9 * max(len(pubs0), 1) or len(near1) < 0.9 * max(len(pubs1), 1):
        continue
    A1 = hist(a1["cpy"])
    old = [p for p in pubs1 if p in pubs0 and p in near0 and p in near1]
    new = [p for p in pubs1 if p not in pubs0 and p in near1]
    diag["authors"] += 1
    diag["old_pubs"] += len(old)
    diag["new_pubs"] += len(new)
    diag["new_pubs_cited"] += sum(1 for p in new if (pubs1[p]["nc"] or 0) > 0)
    diag["old_pubs_cited_t1"] += sum(1 for p in old if (pubs1[p]["nc"] or 0) > 0)
    diag["t1_pubs"] += len(pubs1)
    diag["missing_truth"] += sum(1 for p in pubs1 if p not in near1)

    H0 = {p: hist(near0[p]["cpy"]) for p in old}
    H1 = {p: hist(near1[p]["cpy"]) for p in old + new}
    N1 = {p: pubs1[p]["nc"] or 0 for p in old}
    yrs = list(range(t0.year, t1.year + 1))
    fr = year_fracs(t0, t1)

    # How much true growth lands in years before t0's year (GS backfill)?
    for p in old:
        for y in set(H0[p]) | set(H1[p]):
            d = H1[p].get(y, 0) - H0[p].get(y, 0)
            if y < t0.year:
                diag["growth_pre_t0"] += abs(d)
            else:
                diag["growth_window"] += abs(d)

    # Growth = change in the paper's GS total. Not N1 - sum(H0): GS totals include
    # citations without a year, which never appear in the histogram.
    N0 = {p: pubs0[p]["nc"] or 0 for p in old}
    g = {p: max(N1[p] - N0[p], 0) for p in old}
    diag["neg_growth_pubs"] += sum(1 for p in old if N1[p] - N0[p] < 0)
    # author yearly residual after removing known parts
    R = {}
    for y in yrs:
        R[y] = A1.get(y, 0) - sum(H0[p].get(y, 0) for p in old) - sum(H1[p].get(y, 0) for p in new)

    seed1 = {p: {y: fr[y] + 1e-9 for y in yrs} for p in old}

    def trend_seed(p):
        h = H0[p]
        ly = t0.year - 1
        r1, r2 = h.get(ly, 0) + 0.5, h.get(ly - 1, 0) + 0.5
        tr = min(max(r1 / r2, 0.5), 2.0)
        return {y: r1 * tr ** (y - ly) * fr[y] + 1e-9 for y in yrs}

    seed3 = {p: trend_seed(p) for p in old}
    E1 = ipf(seed1, g, {}, iters=1)
    E2 = ipf(seed1, g, R)
    E3 = ipf(seed3, g, R)

    def full(p, add):
        h = dict(H0[p])
        for y, v in (add or {}).items():
            h[y] = h.get(y, 0) + v
        return {y: int(round(v)) for y, v in h.items()}

    preds = {
        "E0": {p: dict(H0[p]) for p in old},
        "E1": {p: full(p, E1[p]) for p in old},
        "E2": {p: full(p, E2[p]) for p in old},
        "E3": {p: full(p, E3[p]) for p in old},
    }

    def cum(h, Y):
        return sum(v for y, v in h.items() if y <= Y)

    evyrs = list(range(2019, t1.year + 1))
    for p in old:
        rec = {"sid": sid, "pid": p, "yr": pubs1[p].get("yr"), "growth_true": {}, "true": {}, "pred": {}}
        for Y in evyrs:
            rec["true"][Y] = cum(H1[p], Y)
            rec["pred"][Y] = {k: cum(preds[k][p], Y) for k in preds}
        rec["abs_err_window"] = {k: sum(abs(preds[k][p].get(y, 0) - H1[p].get(y, 0)) for y in yrs) for k in preds}
        rec["true_growth_window"] = sum(abs(H1[p].get(y, 0) - H0[p].get(y, 0)) for y in yrs)
        records.append(rec)

    for Y in evyrs:
        true_c = [cum(H1[p], Y) for p in old + new]
        row = {"sid": sid, "Y": Y, "h_true": hindex(true_c)}
        for k in preds:
            pc = [cum(preds[k][p], Y) for p in old] + [cum(H1[p], Y) for p in new]
            row[f"h_{k}"] = hindex(pc)
        author_rows.append(row)

    # Author yearly totals in the growth window: how close is each estimator to GS's own chart?
    for y in yrs:
        row = {"sid": sid, "y": y, "a1": A1.get(y, 0)}
        for k in preds:
            row[k] = sum(preds[k][p].get(y, 0) for p in old) + sum(H1[p].get(y, 0) for p in new)
        author_year_rows.append(row)

# ---------- report ----------
K = ["E0", "E1", "E2", "E3"]
print("== Validation set ==")
for k, v in diag.items():
    print(f"  {k}: {v}")
print(f"  share of true |growth| in years before t0's year: {diag['growth_pre_t0'] / max(diag['growth_pre_t0'] + diag['growth_window'], 1):.3f}")

print("\n== Allocation error in the growth window (sum |pred - true| / sum |true growth|) ==")
tg = sum(r["true_growth_window"] for r in records)
for k in K:
    print(f"  {k}: {sum(r['abs_err_window'][k] for r in records) / tg:.3f}")

years = sorted({Y for r in records for Y in r["true"]})
print("\n== Per-paper cumulative citations through year Y ==")
for Y in years:
    # Only papers published by Y: production temporal stats start at pub_year.
    rs = [r for r in records if Y in r["true"] and r["yr"] and r["yr"] <= Y]
    print(f"  Y={Y}  papers={len(rs)}")
    for k in K:
        ae = [abs(r["pred"][Y][k] - r["true"][Y]) for r in rs]
        big = [r for r in rs if r["true"][Y] >= 10]
        rel = sorted(abs(r["pred"][Y][k] - r["true"][Y]) / r["true"][Y] for r in big)
        within = sum(1 for x in rel if x <= 0.05) / max(len(rel), 1)
        print(f"    {k}: mean abs err {sum(ae) / len(ae):6.2f}  | papers with >=10 cites: median rel err {rel[len(rel) // 2]:.3f}, "
              f"p90 {rel[int(.9 * len(rel))]:.3f}, within 5%: {within:.3f}")

print("\n== Percentile within pub-year cohort (cumulative through Y), validation papers only ==")
for Y in years:
    rs = [r for r in records if Y in r["true"] and r["yr"] and r["yr"] <= Y]
    by = collections.defaultdict(list)
    for r in rs:
        by[r["yr"]].append(r)

    def pct(vals):
        s = sorted(vals)
        n = len(s)
        return [bisect.bisect_left(s, v) / max(n - 1, 1) for v in vals]

    out = {}
    for k in K:
        errs = []
        for yr, grp in by.items():
            if len(grp) < 20:
                continue
            pt = pct([r["true"][Y] for r in grp])
            pp = pct([r["pred"][Y][k] for r in grp])
            errs += [abs(a - b) for a, b in zip(pt, pp)]
        errs.sort()
        out[k] = (sum(errs) / len(errs), errs[int(.95 * len(errs))], sum(1 for e in errs if e > .02) / len(errs))
    print(f"  Y={Y}: " + "  ".join(f"{k}: mean {m:.4f} p95 {p:.4f} >2pts {f:.3f}" for k, (m, p, f) in out.items()))

print("\n== Author h-index through year Y ==")
for Y in years:
    rs = [r for r in author_rows if r["Y"] == Y]
    line = f"  Y={Y} authors={len(rs)} mean true h={sum(r['h_true'] for r in rs) / len(rs):.1f}: "
    for k in K:
        ex = sum(1 for r in rs if r[f"h_{k}"] == r["h_true"]) / len(rs)
        w1 = sum(1 for r in rs if abs(r[f"h_{k}"] - r["h_true"]) <= 1) / len(rs)
        mae = sum(abs(r[f"h_{k}"] - r["h_true"]) for r in rs) / len(rs)
        line += f"{k}: exact {ex:.2f} ±1 {w1:.2f} MAE {mae:.2f} | "
    print(line)

print("\n== Author yearly totals in the growth window vs GS author histogram ==")
for y in sorted({r["y"] for r in author_year_rows}):
    rs = [r for r in author_year_rows if r["y"] == y and r["a1"] > 0]
    line = f"  y={y} author-years={len(rs)}: "
    for k in K:
        tot = sum(abs(r[k] - r["a1"]) for r in rs) / sum(r["a1"] for r in rs)
        rel = sorted(abs(r[k] - r["a1"]) / r["a1"] for r in rs)
        line += f"{k}: weighted err {tot:.3f} median {rel[len(rel) // 2]:.3f} p90 {rel[int(.9 * len(rel))]:.3f} | "
    print(line)
