"""Server-side matplotlib plot generation.

All functions return base64-encoded PNG data URIs or empty string on error.
Figures are explicitly closed after rendering to prevent memory leaks.
"""

import base64
import logging
from io import BytesIO

import matplotlib
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

logger = logging.getLogger(__name__)

matplotlib.rcParams.update({"font.size": 16})


def _fig_to_data_uri(fig):
    """Render a Figure to a base64 PNG data URI and close it."""
    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    data = base64.b64encode(buf.getvalue()).decode("ascii")
    del fig
    return f"data:image/png;base64,{data}"


def generate_percentile_rank_plot(df, author_name):
    """Scatter: paper rank (X) vs citation percentile (Y), colored by age."""
    try:
        fig = Figure(figsize=(10, 10), dpi=100)
        ax = fig.subplots()
        scatter = ax.scatter(
            df["publication_rank"],
            df["num_citations_percentile"],
            c=df["age"],
            cmap="Blues_r",
            s=40,
        )
        cbar = fig.colorbar(scatter, ax=ax)
        cbar.set_label("Years since Publication")
        ax.set_title(f"Paper Percentile Scores for {author_name}")
        ax.set_yticks(np.arange(0, 110, 10))
        ax.grid(True, color="lightgray", linestyle="--")
        ax.set_xlabel("Paper Rank")
        ax.set_ylabel("Paper Percentile Score")
        return _fig_to_data_uri(fig)
    except Exception:
        logger.exception("Error generating percentile rank plot for %s", author_name)
        return ""


def generate_pip_plot(df, author_name):
    """Scatter: publication count percentile (X) vs citation percentile (Y)."""
    try:
        fig = Figure(figsize=(10, 10), dpi=100)
        ax = fig.subplots()
        scatter = ax.scatter(
            df["num_papers_percentile"],
            df["num_citations_percentile"],
            c=df["age"],
            cmap="Blues_r",
            s=40,
        )
        cbar = fig.colorbar(scatter, ax=ax)
        cbar.set_label("Years since Publication")
        ax.set_title(f"Paper Percentile Scores vs #Papers Percentile for {author_name}")
        ax.set_xlabel("Number of Papers Published Percentile")
        ax.set_ylabel("Paper Percentile Score")
        ax.grid(True, color="lightgray", linestyle="--")
        ax.set_xticks(np.arange(0, 110, 10))
        ax.set_yticks(np.arange(0, 110, 10))
        return _fig_to_data_uri(fig)
    except Exception:
        logger.exception("Error generating PiP plot for %s", author_name)
        return ""


def generate_pub_citation_plot(df):
    """Dual-axis: yearly citations (bar) + percentile lines for a publication."""
    try:
        df = df.copy()
        df["citation_year"] = pd.to_datetime(df["citation_year"], format="%Y")
        df = df.set_index("citation_year")

        fig = Figure(figsize=(10, 5), dpi=100)
        ax1 = fig.subplots()

        ax1.set_xlabel("Citation Year")
        ax1.set_ylabel("Yearly Citations", color="tab:blue")
        ax1.bar(df.index, df["yearly_citations"], color="tab:blue", width=200)
        ax1.tick_params(axis="y", labelcolor="tab:blue")
        ax1.grid(which="major", linestyle="--", linewidth=0.5, color="gray")

        ax2 = ax1.twinx()
        ax2.set_ylabel("% Citations", color="tab:red")
        ax2.plot(df.index, df["perc_yearly_citations"], color="tab:orange",
                 label="Yearly Citations Percentile", marker="o")
        ax2.plot(df.index, df["perc_cumulative_citations"], color="tab:red",
                 label="Cumulative Citations Percentile", marker="o")
        ax2.tick_params(axis="y", labelcolor="tab:red")
        ax2.set_ylim(0, 1)

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, labels1 + labels2, loc="lower left",
                   bbox_to_anchor=(0, 1))

        fig.suptitle("Citations over time")
        return _fig_to_data_uri(fig)
    except Exception:
        logger.exception("Error generating publication citations plot")
        return ""


def _generate_temporal_plot(df, y_value_col, y_perc_col, title, y_value_label):
    """Dual-axis temporal plot: metric value (left) + percentile (right)."""
    if df.empty or y_value_col not in df.columns or y_perc_col not in df.columns:
        return ""
    try:
        fig = Figure(figsize=(10, 5), dpi=100)
        ax1 = fig.subplots()

        ax1.set_xlabel("Year")
        ax1.set_ylabel(y_value_label, color="tab:blue")
        ax1.plot(df["state_year"], df[y_value_col], color="tab:blue", marker="o",
                 label=y_value_label)
        ax1.tick_params(axis="y", labelcolor="tab:blue")
        ax1.grid(True, linestyle="--", linewidth=0.5, color="gray")

        ax2 = ax1.twinx()
        ax2.set_ylabel("Percentile", color="tab:red")
        ax2.plot(df["state_year"], df[y_perc_col] * 100, color="tab:red",
                 marker="x", linestyle="--", label="Percentile")
        ax2.tick_params(axis="y", labelcolor="tab:red")
        ax2.set_ylim(0, 100)

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2,
                   loc="upper center", bbox_to_anchor=(0.5, -0.15),
                   fancybox=True, shadow=True, ncol=2)

        fig.suptitle(title)
        return _fig_to_data_uri(fig)
    except Exception:
        logger.exception("Error generating temporal plot: %s", title)
        return ""


def generate_author_h_index_plot(df):
    return _generate_temporal_plot(df, "h_index", "h_index_percentile",
                                   "H-Index Evolution Over Time", "H-Index Value")


def generate_author_total_citations_plot(df):
    return _generate_temporal_plot(df, "total_citations", "total_citations_percentile",
                                   "Total Citations Evolution Over Time", "Total Citations")


def generate_author_i10_index_plot(df):
    return _generate_temporal_plot(df, "i10_index", "i10_index_percentile",
                                   "i10-Index Evolution Over Time", "i10-Index Value")


def generate_author_h_index_5y_plot(df):
    return _generate_temporal_plot(df, "h_index_5y", "h_index_5y_percentile",
                                   "H-Index (Last 5 Years) Evolution Over Time",
                                   "H-Index (5y) Value")
