"""Figure: the tau sweep for the symmetric PiP variant (FINDINGS.md section 9).

Reads the CSVs exported from BigQuery (paper_rp.variant_profile, variant_prediction, pip_variants) in data/ and
writes tau_sweep.pdf (for the paper) and tau_sweep.png (for viewing).

Run: python3 paper/figures/make_tau_sweep.py   (needs matplotlib; uses Inter if installed)
"""
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")

TAUS = [(0.0, "sym"), (0.25, "sym_tau25"), (0.5, "sym_tau50"), (0.6, "sym_tau60"),
        (0.75, "sym_tau75"), (0.9, "sym_tau90")]
X = [t for t, _ in TAUS]

# Palette: categorical slots 1-3 of the reference palette (validated light, all pairs); ink and grid tokens.
SERIES = {"articles": "#2a78d6", "top": "#eb6834", "quality": "#1baf7a"}
INK, INK2, INK3 = "#0b0b0b", "#52514e", "#8a8984"
GRID, SHADE, SURFACE = "#e6e5e1", "#f1f0ec", "#ffffff"


def read(name):
    with open(os.path.join(DATA, name)) as f:
        return list(csv.DictReader(f))


def setup_fonts():
    names = {f.name for f in font_manager.fontManager.ttflist}
    family = "Inter" if "Inter" in names else "DejaVu Sans"
    plt.rcParams.update({
        "font.family": family, "font.size": 8, "axes.titlesize": 8.5, "axes.labelsize": 8,
        "xtick.labelsize": 7.5, "ytick.labelsize": 7.5, "axes.edgecolor": INK3, "axes.labelcolor": INK2,
        "xtick.color": INK2, "ytick.color": INK2, "text.color": INK, "pdf.fonttype": 42,
    })


def style(ax, title, ylabel, ylim):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    ax.set_xlim(-0.04, 0.98)
    ax.set_xticks(X)
    ax.set_xticklabels(["0", ".25", ".5", ".6", ".75", ".9"])
    ax.set_ylim(*ylim)
    ax.set_title(title, loc="left", color=INK, fontweight="semibold", pad=6)
    ax.set_ylabel(ylabel)
    ax.set_xlabel("τ  (papers below this percentile earn no credit)")
    # Unstable range: medians drift across cohorts from tau = 0.75 up (section 9)
    ax.axvspan(0.70, 0.98, color=SHADE, zorder=0, linewidth=0)
    # Default
    ax.axvline(0.5, color=INK3, linewidth=0.8, linestyle=(0, (2, 2)), zorder=1)


def score_line(ax, ys):
    ax.plot(X, ys, color=INK, linewidth=1.6, zorder=3)
    ax.plot(X, ys, linestyle="none", marker="o", markersize=4.5, markerfacecolor=INK,
            markeredgecolor=SURFACE, markeredgewidth=1.0, zorder=4)


def reference(ax, y, label, style_, color, side="right", below=False):
    ax.axhline(y, color=color, linewidth=1.0, linestyle=style_, zorder=2)
    x, ha = (0.97, "right") if side == "right" else (-0.02, "left")
    offset = -2 if below else 2
    ax.annotate(label, (x, y), xytext=(0, offset), textcoords="offset points", color=INK2, fontsize=7,
                ha=ha, va="top" if below else "bottom")


def main():
    setup_fonts()
    profile = {r["variant"]: r for r in read("tau_sweep_profile.csv")}
    pred = {(r["scenario"], r["setting"], r["variant"]): r for r in read("tau_sweep_prediction.csv")}
    p10 = lambda v, col: float(pred[("same_age", "10", v)][col])

    fig, axes = plt.subplots(2, 2, figsize=(7.0, 5.2), facecolor=SURFACE)
    (a, b), (c, d) = axes

    # (a) What the score tracks
    style(a, "(a) What the score tracks", "Within-cohort Spearman", (0.0, 1.0))
    lines = [("articles", "vs_articles", "Article count"),
             ("top", "vs_top_papers", "Top-2.5% papers"),
             ("quality", "vs_mean_quality", "Mean paper percentile")]
    for key, col, label in lines:
        ys = [float(profile[v][col]) for _, v in TAUS]
        a.plot(X, ys, color=SERIES[key], linewidth=1.6, label=label, zorder=3)
        a.plot(X, ys, linestyle="none", marker="o", markersize=4.5, markerfacecolor=SERIES[key],
               markeredgecolor=SURFACE, markeredgewidth=1.0, zorder=4)
    # direct labels next to the tau = 0 end: above the article line, below the other two (clear of all lines)
    for key, col, label in lines:
        y0 = float(profile["sym"][col])
        a.annotate(label, (0.0, y0), xytext=(4, 5 if key == "articles" else -12), textcoords="offset points",
                   color=INK2, fontsize=7)
    leg = a.legend(loc="lower right", frameon=True, fontsize=7, handlelength=1.6, borderpad=0.4)
    leg.get_frame().set_facecolor(SURFACE); leg.get_frame().set_edgecolor("none"); leg.get_frame().set_alpha(0.92)
    a.text(0.5, 0.97, "default", color=INK2, fontsize=7, ha="center", va="top",
           transform=a.get_xaxis_transform())
    a.text(0.84, 0.97, "drifts across\ncohorts", color=INK2, fontsize=7, ha="center", va="top",
           transform=a.get_xaxis_transform())

    # (b) future quantity, (c) future quality, (d) future top-1%: symmetric PiP vs the original PiP-AUC and h-index
    # reference-label placement per panel, chosen so no label touches the curve
    panels = [(b, "(b) Predicts how many top-10% papers", "spearman_top10", "Spearman, age 10", (0.30, 0.48),
               dict(side="left", below=True), dict(side="left")),
              (c, "(c) Predicts quality of future papers", "spearman_mean_pct", "Spearman, age 10", (0.15, 0.40),
               dict(side="right", below=True), dict(side="right")),
              (d, "(d) Identifies future top-1% authors", "auc_any_top1", "ROC AUC, age 10", (0.70, 0.79),
               dict(side="left", below=True), dict(side="right"))]
    for ax, title, col, ylabel, ylim, pip_pos, h_pos in panels:
        style(ax, title, ylabel, ylim)
        reference(ax, p10("pip", col), "original PiP-AUC", (0, (4, 2)), INK2, **pip_pos)
        reference(ax, p10("h_raw", col), "h-index", (0, (1, 1.5)), INK3, **h_pos)
        score_line(ax, [p10(v, col) for _, v in TAUS])

    fig.text(0.5, 0.005, "← lower τ: more weight on quantity          higher τ: more weight on impact →",
             ha="center", va="bottom", color=INK2, fontsize=7.5)
    fig.tight_layout(rect=(0, 0.03, 1, 1), h_pad=1.6, w_pad=1.4)
    for ext, kw in (("pdf", {}), ("png", {"dpi": 220})):
        fig.savefig(os.path.join(HERE, f"tau_sweep.{ext}"), facecolor=SURFACE, **kw)
    print("wrote", os.path.join(HERE, "tau_sweep.pdf"), "and .png")


if __name__ == "__main__":
    main()
