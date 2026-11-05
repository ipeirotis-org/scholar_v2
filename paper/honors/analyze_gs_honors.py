"""Cross-sectional check on Google Scholar profiles (paper/sql/24_gs_honors.sql): AUC of each indicator's cohort
percentile for holding a senior honor and for having an English Wikipedia article, and Spearman correlation with
Wikipedia pageviews among authors with an article. 95% bootstrap intervals (1,000 resamples, stratified by outcome)
and paired differences PiP minus each indicator. Input data/gs_honors_sample.csv; output data/gs_honors_results.csv.

Run: python3 paper/honors/analyze_gs_honors.py
"""
import csv
import os

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
PREDICTORS = ["p_pip", "p_sym75", "p_pip_original", "p_h", "p_citations", "p_i10", "p_articles", "p_top10", "p_i3",
              "p_mean_pct"]
B = 1000


def ranks(x):
    _, inv, cnt = np.unique(x, return_inverse=True, return_counts=True)
    cum = np.cumsum(cnt)
    return (cum - (cnt - 1) / 2)[inv]


def auc(x, y):
    r = ranks(x)
    n1 = y.sum()
    return (r[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * (len(y) - n1))


def spearman(x, y):
    return np.corrcoef(ranks(x), ranks(y))[0, 1]


def main():
    rows = list(csv.DictReader(open(os.path.join(HERE, "data", "gs_honors_sample.csv"))))
    c = {k: np.array([float(r[k]) for r in rows]) for k in rows[0] if k != "cohort"}
    rng = np.random.default_rng(20261105)
    out = []
    tasks = [("honored", "auc", np.ones(len(rows), bool)), ("enwiki", "auc", np.ones(len(rows), bool)),
             ("views", "spearman", c["enwiki"] == 1)]
    for outcome, kind, m in tasks:
        y = c[outcome][m]
        X = {p: c[p][m] for p in PREDICTORS}
        f = (lambda x, yy: auc(x, yy.astype(int))) if kind == "auc" else spearman
        point = {p: f(X[p], y) for p in PREDICTORS}
        boot = {p: [] for p in PREDICTORS}
        if kind == "auc":
            pos, neg = np.where(y == 1)[0], np.where(y == 0)[0]
        for _ in range(B):
            idx = (np.concatenate([rng.choice(pos, len(pos)), rng.choice(neg, len(neg))]) if kind == "auc"
                   else rng.choice(len(y), len(y)))
            for p in PREDICTORS:
                boot[p].append(f(X[p][idx], y[idx]))
        for p in PREDICTORS:
            b, d = np.array(boot[p]), np.array(boot["p_pip"]) - np.array(boot[p])
            out.append({"outcome": outcome, "measure": kind, "predictor": p[2:], "n": int(m.sum()),
                        "positives": int(y.sum()) if kind == "auc" else "", "value": round(point[p], 4),
                        "lo": round(np.percentile(b, 2.5), 4), "hi": round(np.percentile(b, 97.5), 4),
                        "pip_minus": round(point["p_pip"] - point[p], 4),
                        "diff_lo": round(np.percentile(d, 2.5), 4), "diff_hi": round(np.percentile(d, 97.5), 4)})
        print(f"{outcome:8s} {kind:8s} n={int(m.sum())} " + "  ".join(f"{p[2:]}={point[p]:.3f}" for p in PREDICTORS))
    with open(os.path.join(HERE, "data", "gs_honors_results.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out[0]))
        w.writeheader()
        w.writerows(out)


if __name__ == "__main__":
    main()
