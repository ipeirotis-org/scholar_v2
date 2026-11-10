"""Paper figures (except the tau sweep, see make_tau_sweep.py), from the CSVs written by export_figure_data.sh.

  fig_pip_construction from the h-index citation-rank graph to paper percentiles and productivity-normalized area
  fig_standing_curves  the definition: standing curves of three real authors (1995 cohort, career age 10)
  fig_paper_stability  paper percentiles settle early: by age, and by field
  fig_cohorts          the distribution of PiP is stable across cohorts; the raw h-index drifts
  fig_prediction       predicting future work: PiP (tau = 0.5) vs the h-index (raw and percentile)
  fig_fields           share of each field in its cohort's top 10%: pooled vs field-normalized score
  fig_frontier         PiP at every cutoff against percentile-based indicators: future quantity vs future impact
  fig_early            early detection: accuracy by the career age at which each indicator is measured

Run: python3 paper/figures/make_figures.py   (matplotlib; Inter if installed)
"""
import csv
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")

BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"          # reference palette slots 1-3 (validated)
BLUE_LIGHT, ORANGE_LIGHT = "#cfe0f6", "#f9d8c8"               # lighter steps of slots 1-2 for bands/fills
INK, INK2, INK3 = "#0b0b0b", "#52514e", "#8a8984"
GRID, SHADE, SURFACE = "#e6e5e1", "#f1f0ec", "#ffffff"


def read(name):
    with open(os.path.join(DATA, name)) as f:
        return list(csv.DictReader(f))


def setup():
    names = {f.name for f in font_manager.fontManager.ttflist}
    plt.rcParams.update({
        "font.family": "Inter" if "Inter" in names else "DejaVu Sans", "font.size": 8,
        "axes.titlesize": 8.5, "axes.labelsize": 8, "xtick.labelsize": 7.5, "ytick.labelsize": 7.5,
        "axes.edgecolor": INK3, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
        "text.color": INK, "pdf.fonttype": 42, "legend.frameon": False, "legend.fontsize": 7,
    })


def clean(ax, title=None, grid_axis="y"):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    if grid_axis:
        ax.grid(axis=grid_axis, color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    if title:
        ax.set_title(title, loc="left", color=INK, fontweight="semibold", pad=6)


def dots(ax, x, y, color, **kw):
    ax.plot(x, y, linestyle="none", marker=kw.pop("marker", "o"), markersize=kw.pop("ms", 4.5),
            markerfacecolor=color, markeredgecolor=SURFACE, markeredgewidth=1.0, zorder=4, **kw)


def save(fig, name):
    for ext, kw in (("pdf", {}), ("png", {"dpi": 220})):
        fig.savefig(os.path.join(HERE, f"{name}.{ext}"), facecolor=SURFACE, **kw)
    plt.close(fig)
    print("wrote", name)


def short_field(name):
    return name.replace("Agricultural and Food Sciences", "Agriculture & Food")


def fig_standing_curves():
    rows = read("fig3_curves.csv")
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.75), facecolor=SURFACE, sharey=True)
    for ax, q, name in zip(axes, ("25", "50", "90"), ("25th-percentile", "Median", "90th-percentile")):
        pts = [r for r in rows if r["sym_tau50_quantile"] == q]
        pip, n = float(pts[0]["sym_tau50"]), int(pts[0]["n_articles"])
        clean(ax, f"{name} author\n{n} articles, PiP = {pip:.2f}")
        xs = [float(r["theta"]) for r in pts] + [1.0]
        ys = [float(r["g_below"]) for r in pts]
        ys = ys + [ys[-1]]
        ax.axvspan(0, 0.5, color=SHADE, zorder=0, linewidth=0)
        ax.fill_between(xs, ys, step="post", where=[x >= 0.5 for x in xs], color=BLUE_LIGHT, linewidth=0, zorder=1)
        ax.step(xs, ys, where="post", color=BLUE, linewidth=1.5, zorder=3)
        ax.hlines(pip, 0.5, 1.0, color=INK, linewidth=1.0, linestyle=(0, (3, 2)), zorder=4)

        # Author's articles ordered by percentile (#1 = highest); each drop in n_j occurs at theta = (j + 1) / 40.
        n_at = {int(r["j"]): int(r["n_j"]) for r in pts}
        g_at = {int(r["j"]): float(r["g_below"]) for r in pts}
        drops = []
        for j in range(40):
            d = n_at[j] - n_at.get(j + 1, 0)
            if d > 0:
                hi_rank = n_at[j]
                lo_rank = n_at.get(j + 1, 0) + 1
                lab = f"#{lo_rank}" if d == 1 else f"#{lo_rank}–{hi_rank}"
                y0 = g_at[j]
                y1 = g_at[j + 1] if j + 1 < 40 else y0
                drops.append(((j + 1) / 40, d, lab, y0, y1))

        # Label subset & y-positions so crowded high-percentile drops stay legible without overlap
        shown_labels = {
            "25": {"#6", "#5", "#4", "#3", "#1–2"},
            "50": {"#7", "#6", "#5", "#4", "#3", "#2", "#1"},
            "90": {"#13", "#8–10", "#2–4", "#1"},
        }[q]
        for x_p, _, lab, y0, y1 in drops:
            ax.plot([x_p], [0.0], linestyle="none", marker="^", markersize=5.2,
                    markerfacecolor=ORANGE, markeredgecolor="none", zorder=6, clip_on=False)
            if y1 < y0:
                ax.plot([x_p, x_p], [y1, y0], color=ORANGE, linewidth=2.1, solid_capstyle="butt", zorder=5)
            has_lab = lab in shown_labels
            stagger_hi = (q == "50" and lab in ("#6", "#1")) or (q == "90" and lab == "#1")
            y_lab = 0.115 if stagger_hi else 0.045
            y_dash_lo = (y_lab + 0.042) if has_lab else 0.02
            y_dash_hi = y1 if y1 < y0 else y0
            if y_dash_hi > y_dash_lo:
                ax.plot([x_p, x_p], [y_dash_lo, y_dash_hi], color=ORANGE, linewidth=0.75,
                        linestyle=(0, (2.2, 2.0)), zorder=2)
            if has_lab:
                dx = 0.028 if x_p <= 0.03 else (-0.024 if x_p >= 0.99 else 0.0)
                if q == "25" and lab == "#4":
                    dx = -0.032
                elif q == "25" and lab == "#1–2":
                    dx = 0.062
                elif q == "50" and lab == "#1":
                    dx = -0.025
                bg = BLUE_LIGHT if x_p >= 0.5 else SHADE
                ax.text(x_p + dx, y_lab, lab, color=ORANGE, fontsize=5.8, fontweight="semibold",
                        ha="center", va="bottom", zorder=6,
                        bbox=dict(facecolor=bg, edgecolor="none", pad=0.3) if stagger_hi else None)

        if q == "25":
            ax.annotate("drops at each\nauthor paper", xy=(0.37, 0.29), xytext=(0.05, 0.56), color=ORANGE,
                        fontsize=6.2, ha="left", va="center", zorder=6,
                        arrowprops=dict(arrowstyle="->", color=ORANGE, linewidth=0.8, shrinkA=2, shrinkB=2))
            ax.annotate("between papers:\nrises as peers drop out", xy=(0.76, 0.31), xytext=(0.38, 0.75),
                        color=INK2, fontsize=6.2, ha="left", va="center", zorder=6,
                        arrowprops=dict(arrowstyle="->", color=INK3, linewidth=0.75, shrinkA=2, shrinkB=2))
            ax.text(0.20, 0.14, "θ < τ:\nnot counted", color=INK2, fontsize=6.2, ha="center", va="bottom")
        elif q == "50":
            ax.annotate("papers #7–#1 mark\neach downward step", xy=(0.35, 0.43), xytext=(0.04, 0.78),
                        color=ORANGE, fontsize=6.2, ha="left", va="center", zorder=6,
                        arrowprops=dict(arrowstyle="->", color=ORANGE, linewidth=0.8, shrinkA=2, shrinkB=2))
            ax.annotate("no paper in (.725, .925):\nclimbs from .24 to .63", xy=(0.85, 0.50), xytext=(0.44, 0.85),
                        color=INK2, fontsize=6.1, ha="left", va="center", zorder=6,
                        arrowprops=dict(arrowstyle="->", color=BLUE, linewidth=0.85, shrinkA=2, shrinkB=2))
            ax.annotate("average height = PiP", xy=(0.67, pip), xytext=(0.43, 0.54), color=INK, fontsize=6.1,
                        ha="left", va="center", zorder=6,
                        arrowprops=dict(arrowstyle="->", color=INK3, linewidth=0.7, shrinkA=2, shrinkB=2))
        elif q == "90":
            ax.annotate("larger drops where\npapers tie (#8–10, #2–4)", xy=(0.72, 0.83), xytext=(0.035, 0.58),
                        color=ORANGE, fontsize=6.0, ha="left", va="center", zorder=6,
                        arrowprops=dict(arrowstyle="->", color=ORANGE, linewidth=0.8, shrinkA=2, shrinkB=2))

        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xticks([0, 0.25, 0.5, 0.75, 1])
        ax.set_xticklabels(["0", ".25", ".5", ".75", "1"])
        ax.set_xlabel("Paper percentile threshold θ")
    axes[0].set_ylabel("Standing among peers g(θ)")
    fig.tight_layout(w_pad=0.9)
    save(fig, "fig_standing_curves")


def fig_pip_construction():
    # Same median author of the 1995 cohort at career age 10 as in fig_standing_curves (7 articles, #1-#7 by percentile).
    # Illustrative raw citations showing h = 4 and a recent top-10% paper (#2) with only 3 < h raw citations so far.
    raw_papers = [("#1", 22), ("#3", 11), ("#4", 8), ("#5", 6), ("#2", 3), ("#6", 2), ("#7", 1)]
    # Paper percentiles (top of each 1/40 class): 0.975, 0.925, 0.725, 0.625, 0.500, 0.350, 0.300.
    ys = [0.975, 0.925, 0.725, 0.625, 0.500, 0.350, 0.300]
    # Cohort CDF F(k) = share of eligible peers (1995 cohort, age 10) with <= k articles.
    # F(5)=0.3630, F(6)=0.4547, F(7)=0.5315, F(12)=0.7648, F(13)=0.7920 are directly in fig3_curves.csv.
    F = [0.0, 0.0, 0.0, 0.1100, 0.2380, 0.3630, 0.4547, 0.5315]
    fig, (a, b, c) = plt.subplots(1, 3, figsize=(7.2, 2.65), facecolor=SURFACE)

    # Panel (a): Raw citation-rank graph and the h x h square (h = 4)
    clean(a, "(a) Raw citations vs. rank\n7 articles, h-index = 4")
    a.title.set_fontsize(7.4)
    for k, (lab, c_val) in enumerate(raw_papers, start=1):
        a.bar(k - 1, c_val, width=1.0, align="edge", color=BLUE_LIGHT, edgecolor=SURFACE, linewidth=0.9, zorder=1)
    a.fill_between([0, 4], [0, 0], [4, 4], color=ORANGE_LIGHT, linewidth=0, zorder=2)
    a.plot([0, 4, 4], [4, 4, 0], color=ORANGE, linewidth=1.4, zorder=3)
    a.text(2.0, 2.0, "h × h\n(h = 4)", color=INK, fontsize=6.5, fontweight="semibold", ha="center", va="center", zorder=4)
    a.step(range(len(raw_papers) + 1), [v for _, v in raw_papers] + [0], where="post", color=BLUE, linewidth=1.6, zorder=3)
    dots(a, [k - 0.5 for k, (lab, _) in enumerate(raw_papers, start=1) if lab != "#2"],
         [v for lab, v in raw_papers if lab != "#2"], BLUE, ms=4.2)
    dots(a, [4.5], [3], ORANGE, ms=4.8)
    for k, (lab, v) in enumerate(raw_papers, start=1):
        if k <= 4:
            a.text(k - 0.5, (4.0 + v) / 2, lab, color=INK2, fontsize=6.2, ha="center", va="center", zorder=4)
        elif lab == "#2":
            a.text(k - 0.5, 1.45, lab, color=ORANGE, fontsize=6.3, fontweight="semibold", ha="center", va="center", zorder=4)
        else:
            a.text(k - 0.5, v + 1.15, lab, color=INK2, fontsize=6.2, ha="center", va="center", zorder=4)
    a.annotate("excess citations\nabove h ignored", xy=(1.02, 15.6), xytext=(1.8, 18.8),
               color=INK2, fontsize=6.2, ha="left", va="center",
               arrowprops=dict(arrowstyle="->", color=INK3, linewidth=0.75, shrinkA=2, shrinkB=2))
    a.annotate("recent top-10% paper (#2)\nhas 3 citations (< h)", xy=(4.5, 3.3), xytext=(3.05, 10.3),
               color=ORANGE, fontsize=6.2, ha="left", va="center",
               arrowprops=dict(arrowstyle="->", color=ORANGE, linewidth=0.85, shrinkA=2, shrinkB=2))
    a.set_xlim(0, 9)
    a.set_ylim(0, 24)
    a.set_xticks([0, 2, 4, 6, 8])
    a.set_yticks([0, 4, 10, 15, 20])
    a.set_xlabel("Paper rank k (by raw citations)")
    a.set_ylabel("Raw citations")

    # Panel (b): Paper percentile vs. paper rank (width = 1 per paper; area = sum of percentiles = I3)
    clean(b, "(b) Paper percentile vs. rank\n7 articles, I3 = 4.40 (unbounded)")
    b.title.set_fontsize(7.4)
    for k, y in enumerate(ys, start=1):
        b.bar(k - 1, y, width=1.0, align="edge", color=BLUE_LIGHT, edgecolor=SURFACE, linewidth=0.9, zorder=1)
    b.step(range(len(ys) + 1), ys + [0.0], where="post", color=BLUE, linewidth=1.6, zorder=3)
    dots(b, [k - 0.5 for k in range(1, len(ys) + 1) if k != 2], [y for k, y in enumerate(ys, start=1) if k != 2], BLUE, ms=4.2)
    dots(b, [1.5], [ys[1]], ORANGE, ms=4.8)
    for k, y in enumerate(ys, start=1):
        b.text(k - 0.5, y / 2, f"#{k}", color=ORANGE if k == 2 else INK2, fontsize=6.3 if k == 2 else 6.2,
               fontweight="semibold" if k == 2 else "normal", ha="center", va="center", zorder=4)
    b.annotate("recent paper #2\nrises to .925", xy=(1.62, ys[1]), xytext=(3.2, 0.93),
               color=ORANGE, fontsize=6.2, ha="left", va="center",
               arrowprops=dict(arrowstyle="->", color=ORANGE, linewidth=0.85, shrinkA=2, shrinkB=2))
    b.annotate("width = 1 per paper\n(unbounded x-axis)", xy=(7.0, 0.305), xytext=(4.65, 0.58),
               color=INK2, fontsize=6.2, ha="left", va="center",
               arrowprops=dict(arrowstyle="->", color=INK3, linewidth=0.75, shrinkA=2, shrinkB=2))
    b.set_xlim(0, 9)
    b.set_ylim(0, 1.03)
    b.set_xticks([0, 2, 4, 6, 8])
    b.set_yticks([0, 0.25, 0.5, 0.75, 1.0])
    b.set_yticklabels(["0", ".25", ".5", ".75", "1"])
    b.set_xlabel("Paper rank k (by percentile)")
    b.set_ylabel("Paper citation percentile")

    # Panel (c): Paper percentile vs. cohort productivity percentile
    pip_auc = sum(y * (F[k] - F[k - 1]) for k, y in enumerate(ys, start=1))
    clean(c, f"(c) Percentile vs. peer productivity\n7 articles (53rd pct.), area = {pip_auc:.2f}")
    c.title.set_fontsize(7.4)
    for k, y in enumerate(ys, start=1):
        w = F[k] - F[k - 1]
        if w > 0:
            c.bar(F[k - 1], y, width=w, align="edge", color=BLUE_LIGHT, edgecolor=SURFACE, linewidth=0.9, zorder=1)
            c.text((F[k - 1] + F[k]) / 2, y / 2, f"#{k}", color=INK2, fontsize=6.2, ha="center", va="center", zorder=4)
    xs_c = [F[2], F[3], F[4], F[5], F[6], F[7], F[7]]
    ys_c = [ys[2], ys[3], ys[4], ys[5], ys[6], ys[6], 0.0]
    c.step(xs_c, ys_c, where="post", color=BLUE, linewidth=1.6, zorder=3)
    # Papers #1 and #2 sit at x = 0 with zero bar width; papers #3..#7 sit at the midpoints of their bars
    dots(c, [0.0, 0.0], [ys[0], ys[1]], ORANGE, ms=4.6, clip_on=False)
    dots(c, [(F[k - 1] + F[k]) / 2 for k in range(3, 8)], ys[2:], BLUE, ms=4.2)
    c.annotate("#1, #2 get width 0\n(no peer has < 3 papers)", xy=(0.02, 0.95), xytext=(0.13, 0.92),
               color=INK, fontsize=6.2, ha="left", va="center",
               arrowprops=dict(arrowstyle="->", color=ORANGE, linewidth=0.8, shrinkA=2, shrinkB=2))
    c.annotate("widest at mode\n(#4–#5)", xy=(0.31, 0.50), xytext=(0.48, 0.65),
               color=INK2, fontsize=6.2, ha="left", va="center",
               arrowprops=dict(arrowstyle="->", color=INK3, linewidth=0.75, shrinkA=2, shrinkB=2))
    c.annotate("stops at 0.53", xy=(F[7], 0.18), xytext=(0.60, 0.18),
               color=INK2, fontsize=6.2, ha="left", va="center",
               arrowprops=dict(arrowstyle="->", color=INK3, linewidth=0.75, shrinkA=2, shrinkB=2))
    c.set_xlim(0, 1.0)
    c.set_ylim(0, 1.03)
    c.set_xticks([0, 0.25, 0.5, 0.75, 1.0])
    c.set_xticklabels(["0", ".25", ".5", ".75", "1"])
    c.set_yticks([0, 0.25, 0.5, 0.75, 1.0])
    c.set_yticklabels([])
    c.set_xlabel("Productivity percentile (≤ k articles)")

    fig.tight_layout(rect=(0, 0, 0.988, 1), w_pad=0.9)
    save(fig, "fig_pip_construction")


def fig_paper_stability():
    pooled = read("fig1_pooled.csv")
    fields = read("fig1_fields.csv")
    fig, (a, b) = plt.subplots(1, 2, figsize=(7.0, 3.3), facecolor=SURFACE, gridspec_kw={"width_ratios": [1, 1.15]})
    clean(a, "(a) All fields, papers from 1980–1995")
    for t2, color, label, off in ((10, BLUE, "vs. age 10", (6, -2)), (30, ORANGE, "vs. age 30", (0, -12))):
        pts = sorted((int(r["t1"]), float(r["r"])) for r in pooled if int(r["t2"]) == t2)
        xs, ys = zip(*pts)
        a.plot(xs, ys, color=color, linewidth=1.6, zorder=3)
        dots(a, xs, ys, color)
        a.annotate(label, (xs[-1], ys[-1]), xytext=off, textcoords="offset points", ha="left" if t2 == 10 else "center",
                   color=INK2, fontsize=7.2)
    a.set_xlabel("Age of the paper (years after publication)")
    a.set_ylabel("Correlation with later percentile")
    a.set_ylim(0.6, 1.0)
    a.set_xlim(0, 27)
    clean(b, "(b) By field, papers from 1985–1995", grid_axis="x")
    fields = sorted(fields, key=lambda r: float(r["r_3_10"]))
    ys = list(range(len(fields)))
    b.hlines(ys, [float(r["r_5_30"]) for r in fields], [float(r["r_3_10"]) for r in fields],
             color=GRID, linewidth=1.2, zorder=2)
    dots(b, [float(r["r_3_10"]) for r in fields], ys, BLUE, label="age 3 vs. age 10")
    dots(b, [float(r["r_5_30"]) for r in fields], ys, ORANGE, marker="D", ms=4.0, label="age 5 vs. age 30")
    b.set_yticks(ys)
    b.set_yticklabels([short_field(r["field"]) for r in fields], fontsize=6.8)
    b.set_xlabel("Correlation of percentiles")
    b.legend(loc="upper left")
    fig.tight_layout(w_pad=1.5)
    save(fig, "fig_paper_stability")


def fig_cohorts():
    rows = read("fig2_cohorts.csv")
    yr = [int(r["start_year"]) for r in rows]
    fig, (a, b) = plt.subplots(1, 2, figsize=(7.0, 2.9), facecolor=SURFACE)
    for ax, key, title, ylabel in ((a, "sym", "(a) PiP (τ = 0.5)", "PiP score"),
                                   (b, "h", "(b) Raw h-index", "h-index")):
        clean(ax, title)
        step = None if key == "sym" else "mid"
        ax.fill_between(yr, [float(r[f"{key}_p25"]) for r in rows], [float(r[f"{key}_p75"]) for r in rows],
                        color=BLUE_LIGHT, linewidth=0, step=step, label="25th–75th percentile")
        ax.plot(yr, [float(r[f"{key}_mean"]) for r in rows], color=BLUE, linewidth=1.6, label="mean")
        ax.plot(yr, [float(r[f"{key}_p90"]) for r in rows], color=BLUE, linewidth=1.0, linestyle=(0, (3, 2)),
                drawstyle="default" if key == "sym" else "steps-mid", label="90th percentile")
        ax.set_xlabel("Start year (first article)")
        ax.set_ylabel(ylabel)
        ax.set_xlim(1960, 2015)
    a.set_ylim(0, 1)
    b.set_ylim(0, 12)
    handles, labels = a.get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, fontsize=7, bbox_to_anchor=(0.5, -0.01))
    fig.tight_layout(rect=(0, 0.06, 1, 1), w_pad=1.5)
    save(fig, "fig_cohorts")


def fig_prediction():
    rows = read("fig5_prediction.csv")
    settings = [("same_age", "5", "age 5"), ("same_age", "10", "age 10"), ("same_age", "15", "age 15"),
                ("same_year", "2000", "2000"), ("same_year", "2005", "2005")]
    predictors = [("sym_tau50", "PiP (τ = 0.5)", BLUE, "o"), ("h_pct", "h-index percentile", ORANGE, "s"),
                  ("h_raw", "h-index (raw)", INK3, "^")]
    value = {(r["scenario"], r["setting"], r["predictor"]): r for r in rows}
    outcomes = [("spearman_top10", "(a) Future top-10% papers", "Spearman correlation"),
                ("auc_any_top1", "(b) Any future top-1% paper", "ROC AUC"),
                ("spearman_mean_pct", "(c) Mean future percentile", "Spearman correlation")]
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 3.1), facecolor=SURFACE)
    offsets = [0.0, 0.0, 0.2]
    for ax, (col, title, ylabel) in zip(axes, outcomes):
        clean(ax, title)
        ax.title.set_fontsize(7.6)
        for i, (sc, st, _) in enumerate(settings):
            a, b = value.get((sc, st, "sym_tau50")), value.get((sc, st, "h_pct"))
            if a and b:
                ax.plot([i, i], [float(b[col]), float(a[col])], color=INK3, linewidth=1.0, zorder=2)
        for (key, label, color, marker), off in zip(predictors, offsets):
            xs, ys = [], []
            for i, (sc, st, _) in enumerate(settings):
                r = value.get((sc, st, key))
                if r:
                    xs.append(i + off)
                    ys.append(float(r[col]))
            dots(ax, xs, ys, color, marker=marker, ms=4.6, label=label)
        ax.set_xticks(range(len(settings)))
        ax.set_xticklabels([s[2] for s in settings], fontsize=6.8)
        ax.set_ylabel(ylabel)
        ax.axvline(2.5, color=INK3, linewidth=0.7, zorder=1)
        for i in (0.5, 1.5, 3.5):
            ax.axvline(i, color=GRID, linewidth=0.6, zorder=1)
        ax.text(1, -0.16, "same career age", transform=ax.get_xaxis_transform(), ha="center", va="top",
                color=INK2, fontsize=6.6)
        ax.text(3.5, -0.16, "same year", transform=ax.get_xaxis_transform(), ha="center", va="top",
                color=INK2, fontsize=6.6)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, fontsize=7, bbox_to_anchor=(0.5, -0.01))
    fig.tight_layout(rect=(0, 0.08, 1, 1), w_pad=1.0)
    save(fig, "fig_prediction")


def fig_fields():
    rows = sorted(read("fig6_fields.csv"), key=lambda r: float(r["top10_pooled"]))
    fig, ax = plt.subplots(figsize=(4.8, 3.6), facecolor=SURFACE)
    clean(ax, "Authors in the top 10% of their cohort, by field", grid_axis="x")
    ys = list(range(len(rows)))
    pooled = [100 * float(r["top10_pooled"]) for r in rows]
    fn = [100 * float(r["top10_fn"]) for r in rows]
    ax.axvline(10, color=INK3, linewidth=0.8, linestyle=(0, (2, 2)), zorder=1)
    ax.text(10.3, len(rows) // 2, "10% = no field\nadvantage", color=INK2, fontsize=6.8, ha="left", va="center")
    ax.hlines(ys, pooled, fn, color=GRID, linewidth=1.4, zorder=2)
    dots(ax, pooled, ys, ORANGE, marker="D", ms=4.2, label="pooled score")
    dots(ax, fn, ys, BLUE, label="field-normalized score")
    ax.set_yticks(ys)
    ax.set_yticklabels([short_field(r["main_field"]) for r in rows], fontsize=6.8)
    ax.set_xlabel("Share of the field's authors (%), career age 10")
    ax.set_xlim(0, 18)
    ax.legend(loc="lower right")
    fig.tight_layout()
    save(fig, "fig_fields")


def fig_frontier():
    rows = read("fig7_frontier.csv")
    panels = [("same_age", "10", "_raw", "(a) Career age 10"),
              ("same_year", "2005", "_pct", "(b) All eligible authors in 2005, cohort percentiles")]
    baselines = [("h", "h-index"), ("i3", "I3 (sum of percentiles)"), ("top10", "top-10% count"),
                 ("top10_age", "top-10% per year"), ("top1", "top-1% count")]
    # label offsets in points (dx, dy); ha follows the sign of dx, 0 centers
    offsets = {
        "10": {"h": (6, 0), "pip_original": (-7, 6), "i3": (-7, -6), "top10": (0, -11), "top1": (6, 0)},
        "2005": {"h": (6, 0), "pip_original": (-7, 6), "i3": (-7, -6), "top10": (0, -11),
                 "top10_age": (-6, 9), "top1": (6, 0)},
    }
    tau_offsets = {
        "10": {"0": (6, 0), "25": (6, 0), "50": (7, 2), "60": (6, 4), "75": (0, 8), "90": (-6, 6)},
        "2005": {"0": (6, 0), "25": (6, 0), "50": (7, 2), "60": (6, 5), "75": (6, 7), "90": (-6, 4)},
    }
    taus = [("0", "τ = 0"), ("25", ".25"), ("50", ".5"), ("60", ".6"), ("75", ".75"), ("90", ".9")]
    ha = lambda dx: "left" if dx > 0 else ("right" if dx < 0 else "center")
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.3), facecolor=SURFACE, sharey=True)
    for ax, (sc, st, form, title) in zip(axes, panels):
        clean(ax, title, grid_axis="both")
        val = {r["predictor"]: (float(r["spearman_top10"]), float(r["spearman_mean_pct"]))
               for r in rows if r["scenario"] == sc and r["setting"] == st}
        xs = [val[f"pip_sym{t}{form}"][0] for t, _ in taus]
        ys = [val[f"pip_sym{t}{form}"][1] for t, _ in taus]
        ax.plot(xs, ys, color=BLUE, linewidth=1.6, zorder=3)
        dots(ax, xs, ys, BLUE, ms=5)
        dots(ax, [val[f"pip_sym50{form}"][0]], [val[f"pip_sym50{form}"][1]], BLUE, ms=8.5)
        for (t, lab), x, y in zip(taus, xs, ys):
            dx, dy = tau_offsets[st][t]
            ax.annotate(lab, (x, y), xytext=(dx, dy), textcoords="offset points", color=BLUE, fontsize=6.8,
                        ha=ha(dx), va="center", fontweight="semibold" if t == "50" else "normal")
        ps = val.get(f"t50{form}")
        if ps:
            ax.plot([ps[0]], [ps[1]], linestyle="none", marker="o", markersize=6.5, markerfacecolor=SURFACE,
                    markeredgecolor=BLUE, markeredgewidth=1.3, zorder=5)
            ax.annotate("PiP-simple", ps, xytext=(-8, -1), textcoords="offset points", color=BLUE, fontsize=6.6,
                        ha="right", va="center")
        for key, label in baselines:
            if key == "top10_age" and sc == "same_age":
                continue  # ranks exactly like the top-10% count within one career age
            name = f"{key}{form}" if f"{key}{form}" in val else f"{key}_raw"
            x, y = val[name]
            dots(ax, [x], [y], INK3, marker="s", ms=4.6)
            dx, dy = offsets[st][key]
            ax.annotate(label, (x, y), xytext=(dx, dy), textcoords="offset points", color=INK2, fontsize=6.6,
                        ha=ha(dx), va="center")
        mean_x, mean_y = (v + 0.0 if abs(v) >= 0.005 else 0.0 for v in val[f"mean_pct{form}"])
        art_x, art_y = (v + 0.0 if abs(v) >= 0.005 else 0.0 for v in val[f"articles{form}"])
        ax.text(0.02, 0.03, f"Off the chart: mean percentile ({mean_x:.2f}, {mean_y:.2f}),\n"
                f"article count ({art_x:.2f}, {art_y:.2f})", transform=ax.transAxes, color=INK2, fontsize=6.4,
                va="bottom")
        ax.set_xlabel("Predicts the number of future top-10% papers")
        ax.set_xlim(0.22, 0.48)
        ax.set_ylim(0.14, 0.38)
    axes[0].set_ylabel("Predicts the mean percentile of future papers")
    fig.tight_layout(w_pad=1.2)
    save(fig, "fig_frontier")


def spread_labels(ys, gap):
    """Nudge label positions apart (keeping their order) so that neighbors are at least `gap` apart."""
    order = sorted(range(len(ys)), key=lambda i: ys[i], reverse=True)
    out = list(ys)
    for k in range(1, len(order)):
        i, j = order[k - 1], order[k]
        if out[i] - out[j] < gap:
            out[j] = out[i] - gap
    return out


def interp_age(curve, target):
    ts = sorted(curve)
    for a, b in zip(ts, ts[1:]):
        if curve[a] < target <= curve[b]:
            return a + (target - curve[a]) / (curve[b] - curve[a])
    return None


def fig_early():
    rows = [r for r in read("fig8_early.csv") if r["pop_age"] == "3"]
    preds = [("pip", "PiP", BLUE, 2.0), ("h", "h-index", ORANGE, 1.6), ("top10", "top-10% count", INK3, 1.0),
             ("i3", "I3", INK3, 1.0), ("articles", "articles", INK3, 1.0), ("citations", "citations", INK3, 1.0)]
    panels = [("precision_top10_frac", "(a) Finding future stars: precision of the top 10%", "Share of picks who become stars",
               0.10, 0.021),
              ("auc_any_top1", "(b) Any future top-1% paper", "ROC AUC", 0.5, 0.0175)]
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.2), facecolor=SURFACE)
    for ax, (col, title, ylabel, chance, gap) in zip(axes, panels):
        clean(ax, title)
        ax.title.set_fontsize(7.8)
        curves = {}
        for key, label, color, lw in preds:
            c = {int(r["t"]): float(r[col]) for r in rows if r["predictor"] == key}
            curves[key] = c
            ts = sorted(c)
            ax.plot(ts, [c[t] for t in ts], color=color, linewidth=lw, zorder=4 if key in ("pip", "h") else 2)
            if key in ("pip", "h"):
                dots(ax, ts, [c[t] for t in ts], color, ms=4)
        ends = [curves[k][10] for k, *_ in preds]
        label_y = spread_labels(ends, gap)
        for (key, label, color, _), y in zip(preds, label_y):
            ax.text(10.2, y, label, color=color if key in ("pip", "h") else INK2, fontsize=6.8, va="center",
                    fontweight="semibold" if key == "pip" else "normal")
        # lead time: where PiP reaches the h-index's accuracy at age 10
        target = curves["h"][10]
        a = interp_age(curves["pip"], target)
        if a is not None:
            ax.annotate("", xy=(a, target), xytext=(10, target),
                        arrowprops=dict(arrowstyle="->", color=INK, linewidth=0.9, shrinkA=0, shrinkB=0))
            ax.text(a - 0.15, target + gap * 0.15, f"PiP gets there\n{10 - a:.1f} years earlier", color=INK,
                    fontsize=6.8, ha="right", va="bottom", zorder=6)
        ax.axhline(chance, color=INK3, linewidth=0.7, linestyle=(0, (1, 2)))
        ax.text(3.05, chance, "chance", color=INK3, fontsize=6.4, va="bottom")
        ax.set_xlim(3, 12.2)
        ax.set_xticks(range(3, 11))
        ax.set_xlabel("Career age at which the indicator is measured")
        ax.set_ylabel(ylabel)
    axes[0].set_ylim(0.08, 0.56)
    axes[1].set_ylim(0.48, 0.87)
    fig.tight_layout(w_pad=1.5)
    save(fig, "fig_early")


if __name__ == "__main__":
    setup()
    all_figs = [fig_pip_construction, fig_standing_curves, fig_paper_stability, fig_cohorts,
                fig_prediction, fig_fields, fig_frontier, fig_early]
    only = set(sys.argv[1:])
    for fn in all_figs:
        if not only or fn.__name__ in only:
            fn()

