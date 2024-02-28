import matplotlib
from matplotlib.figure import Figure
import logging
import numpy as np
import pandas as pd
import base64
from io import BytesIO


def generate_percentile_rank_plot(dataframe, author_name):
    try:
        fig = Figure(figsize=(10, 10), dpi=100)
        ax = fig.subplots(1, 1)  # Adjusted for better resolution

        matplotlib.rcParams.update({"font.size": 16})

        marker_size = 40

        # First subplot (Rank vs Percentile Score)
        scatter = ax.scatter(
            dataframe["publication_rank"],
            dataframe["num_citations_percentile"],
            c=dataframe["age"],
            cmap="Blues_r",
            s=marker_size,
        )
        colorbar = fig.colorbar(scatter, ax=ax)
        colorbar.set_label("Years since Publication")
        ax.set_title(f"Paper Percentile Scores for {author_name}")
        ax.set_yticks(np.arange(0, 110, step=10))  # Adjust step as needed
        ax.grid(True, color="lightgray", linestyle="--")
        ax.set_xlabel("Paper Rank")
        ax.set_ylabel("Paper Percentile Score")

        buf = BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format="png")
        data = base64.b64encode(buf.getbuffer()).decode("ascii")

    except Exception as e:
        logging.error(f"Error in generate_plot for {author_name}: {e}")
        raise

    return f"data:image/png;base64,{data}"


def generate_pip_plot(dataframe, author_name):
    try:
        fig = Figure(figsize=(10, 10), dpi=100)
        ax = fig.subplots(1, 1)

        matplotlib.rcParams.update({"font.size": 16})

        marker_size = 40

        # Second subplot (Productivity Percentiles)
        scatter = ax.scatter(
            dataframe["num_papers_percentile"],
            dataframe["num_citations_percentile"],
            c=dataframe["age"],
            cmap="Blues_r",
            s=marker_size,
        )
        colorbar = fig.colorbar(scatter, ax=ax)
        colorbar.set_label("Years since Publication")
        ax.set_title(f"Paper Percentile Scores vs #Papers Percentile for {author_name}")
        ax.set_xlabel("Number of Papers Published Percentile")
        ax.set_ylabel("Paper Percentile Score")
        ax.grid(True, color="lightgray", linestyle="--")
        ax.set_xticks(np.arange(0, 110, step=10))  # Adjust step as needed
        ax.set_yticks(np.arange(0, 110, step=10))  # Adjust step as needed

        buf = BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format="png")
        data = base64.b64encode(buf.getbuffer()).decode("ascii")

    except Exception as e:
        logging.error(f"Error in generate_plot for {author_name}: {e}")
        raise

    return f"data:image/png;base64,{data}"


def generate_pub_citation_plot(df):
    try:

        df["citation_year"] = pd.to_datetime(df["citation_year"], format="%Y")

        corrected_df = (
            df
            .set_index("citation_year")
            .filter(["perc_yearly_citations", "perc_cumulative_citations", "yearly_citations"])
        )

        fig = Figure(figsize=(10, 5), dpi=100)
        ax1 = fig.subplots(1, 1)  # Adjusted for better resolution
        matplotlib.rcParams.update({"font.size": 16})
        # Plotting yearly_citations as a bar plot
        color = "tab:blue"
        ax1.set_xlabel("Citation Year")
        ax1.set_ylabel("Yearly Citations", color=color)
        ax1.bar(corrected_df.index, corrected_df["yearly_citations"], color=color, width=200)
        ax1.tick_params(axis="y", labelcolor=color)
        ax1.grid(which="major", linestyle="--", linewidth="0.5", color="gray")  # Gray dotted grid

        # Secondary y-axis
        ax2 = ax1.twinx()
        color = "tab:red"
        ax2.set_ylabel("% Citations", color=color)
        ax2.plot(
            corrected_df.index,
            corrected_df["perc_yearly_citations"],
            color="tab:orange",
            label="Yearly Citations Percentile",
            marker="o",
        )
        ax2.plot(
            corrected_df.index,
            corrected_df["perc_cumulative_citations"],
            color="tab:red",
            label="Cumulative Citations Percentile",
            marker="o",
        )
        ax2.tick_params(axis="y", labelcolor=color)
        ax2.set_ylim(0, 1)

        # Merge legends
        lines, labels = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax2.legend(lines + lines2, labels + labels2, loc="lower left", bbox_to_anchor=(0, 1))

        fig.suptitle("Citations over time")
        fig.tight_layout()
        buf = BytesIO()
        fig.savefig(buf, format="png")
        data = base64.b64encode(buf.getbuffer()).decode("ascii")

    except Exception as e:
        logging.error(f"Error generating publication citations plot: {e}")
        return ""
    return f"data:image/png;base64,{data}"


def generate_citations_over_time_plot(dataframe, publication_title):
    """
    Generate a plot showing the number of citations over time for a publication.
    """
    try:
        fig = Figure(figsize=(10, 6), dpi=100)
        ax = fig.subplots(1, 1)

        ax.plot(dataframe["citation_year"], dataframe["cumulative_citations"], marker="o", linestyle="-", color="blue")
        ax.set_title(f"Citations Over Time: {publication_title}")
        ax.set_xlabel("Year")
        ax.set_ylabel("Cumulative Citations")
        ax.grid(True, which="both", linestyle="--", linewidth=0.5)

        buf = BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format="png")
        data = base64.b64encode(buf.getbuffer()).decode("ascii")
    except Exception as e:
        logging.error(f"Error generating citations over time plot: {e}")
        return ""
    return f"data:image/png;base64,{data}"


def generate_percentiles_over_time_plot(dataframe, publication_title):
    """
    Generate a plot showing the percentile of citations over time for a publication.
    """
    try:
        fig = Figure(figsize=(10, 6), dpi=100)
        ax = fig.subplots(1, 1)

        ax.plot(dataframe["citation_year"], dataframe["perc_cumulative_citations"], marker="o", linestyle="-", color="green")
        ax.set_title(f"Percentiles Over Time: {publication_title}")
        ax.set_xlabel("Year")
        ax.set_ylabel("Percentile of Cumulative Citations")
        ax.grid(True, which="both", linestyle="--", linewidth=0.5)

        buf = BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format="png")
        data = base64.b64encode(buf.getbuffer()).decode("ascii")
    except Exception as e:
        logging.error(f"Error generating percentiles over time plot: {e}")
        return ""
    return f"data:image/png;base64,{data}"
