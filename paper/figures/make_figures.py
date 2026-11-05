"""Paper figures (except the tau sweep, see make_tau_sweep.py), from the CSVs written by export_figure_data.sh.

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

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")

BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"          # reference palette slots 1-3 (validated)
BLUE_LIGHT = "#cfe0f6"                                         # lighter step of slot 1 for bands
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
    fig, axes = plt.subplots(1, 3, figsize=(7.0, 2.6), facecolor=SURFACE, sharey=True)
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
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xticks([0, 0.25, 0.5, 0.75, 1])
        ax.set_xticklabels(["0", ".25", ".5", ".75", "1"])
        ax.set_xlabel("Paper percentile threshold θ")
    axes[0].set_ylabel("Standing among peers g(θ)")
    axes[0].text(0.25, 0.04, "θ < τ: not counted", color=INK2, fontsize=6.6, ha="center", va="bottom")
    fig.tight_layout(w_pad=1.0)
    save(fig, "fig_standing_curves")


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
    a.set_xlabel("Age of the paper t₁ (years after publication)")
    a.set_ylabel("Correlation of percentiles at t₁ and later")
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
    offsets = [-0.18, 0.0, 0.18]
    for ax, (col, title, ylabel) in zip(axes, outcomes):
        clean(ax, title)
        ax.title.set_fontsize(7.6)
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
    fig_standing_curves()
    fig_paper_stability()
    fig_cohorts()
    fig_prediction()
    fig_fields()
    fig_frontier()
    fig_early()
