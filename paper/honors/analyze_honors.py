"""External validation against later scientific honors (Wikidata), from paper/sql/22_honors.sql.

Input: data/honors_sample.csv, exported from `paper_rp.honors_base`: every author with a Wikidata item linked to
their Semantic Scholar ID, plus a 10% random sample of all other active authors (sampling negatives leaves the AUC
unbiased). Two comparisons, at career ages 5 and 10:
  notable   only authors with a Wikidata item: among people notable enough to be on Wikidata, who gets honored?
  all       all active authors: honorees against everyone else
Outcomes: `honored` (any senior honor not dated at or before the evaluation year) and `honored_dated` (dated honors
only; honorees without a date are dropped). For each indicator: ROC AUC with a 95% bootstrap interval (1,000
resamples, stratified by outcome), and the paired difference PiP minus the indicator.
Writes data/honors_results.csv and prints a summary.

Run: python3 paper/honors/analyze_honors.py
"""
import csv
import os

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
PREDICTORS = ["p_pip", "p_pip_original", "p_h", "p_citations", "p_articles", "p_top10", "p_i3", "p_mean_pct",
              "p_sym0", "p_sym25", "p_sym60", "p_sym75", "p_sym90"]
B = 1000


def ranks(x):
    """Average ranks (1-based), ties averaged."""
    _, inv, cnt = np.unique(x, return_inverse=True, return_counts=True)
    cum = np.cumsum(cnt)
    return (cum - (cnt - 1) / 2)[inv]


def auc(x, y):
    r = ranks(x)
    n1 = y.sum()
    n0 = len(y) - n1
    return (r[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)


def main():
    rows = list(csv.DictReader(open(os.path.join(HERE, "data", "honors_sample.csv"))))
    cols = {k: np.array([float(r[k]) for r in rows]) for k in ["age", "in_wikidata", "honored", "honored_dated"] + PREDICTORS}
    rng = np.random.default_rng(20261105)
    out = []
    for age in (5, 10):
        for pop in ("notable", "all"):
            for outcome in ("honored", "honored_dated"):
                m = cols["age"] == age
                if pop == "notable":
                    m &= cols["in_wikidata"] == 1
                if outcome == "honored_dated":
                    m &= ~((cols["honored"] == 1) & (cols["honored_dated"] == 0))
                y = cols[outcome][m].astype(int)
                X = {p: cols[p][m] for p in PREDICTORS}
                pos, neg = np.where(y == 1)[0], np.where(y == 0)[0]
                point = {p: auc(X[p], y) for p in PREDICTORS}
                boot = {p: [] for p in PREDICTORS}
                for _ in range(B):
                    idx = np.concatenate([rng.choice(pos, len(pos)), rng.choice(neg, len(neg))])
                    yy = y[idx]
                    for p in PREDICTORS:
                        boot[p].append(auc(X[p][idx], yy))
                for p in PREDICTORS:
                    b = np.array(boot[p])
                    d = np.array(boot["p_pip"]) - b
                    out.append({
                        "age": age, "population": pop, "outcome": outcome, "predictor": p[2:],
                        "n": int(m.sum()), "positives": int(y.sum()), "auc": round(point[p], 4),
                        "auc_lo": round(np.percentile(b, 2.5), 4), "auc_hi": round(np.percentile(b, 97.5), 4),
                        "pip_minus": round(point["p_pip"] - point[p], 4),
                        "diff_lo": round(np.percentile(d, 2.5), 4), "diff_hi": round(np.percentile(d, 97.5), 4),
                    })
                print(f"age {age} {pop:7s} {outcome:13s} n={int(m.sum()):7d} pos={int(y.sum()):4d}  " +
                      "  ".join(f"{p[2:]}={point[p]:.3f}" for p in PREDICTORS))
    with open(os.path.join(HERE, "data", "honors_results.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out[0]))
        w.writeheader()
        w.writerows(out)


if __name__ == "__main__":
    main()
