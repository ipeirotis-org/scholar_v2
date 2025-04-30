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

        corrected_df = df.set_index("citation_year").filter(
            ["perc_yearly_citations", "perc_cumulative_citations", "yearly_citations"]
        )

        fig = Figure(figsize=(10, 5), dpi=100)
        ax1 = fig.subplots(1, 1)  # Adjusted for better resolution
        matplotlib.rcParams.update({"font.size": 16})
        # Plotting yearly_citations as a bar plot
        color = "tab:blue"
        ax1.set_xlabel("Citation Year")
        ax1.set_ylabel("Yearly Citations", color=color)
        ax1.bar(
            corrected_df.index, corrected_df["yearly_citations"], color=color, width=200
        )
        ax1.tick_params(axis="y", labelcolor=color)
        ax1.grid(
            which="major", linestyle="--", linewidth="0.5", color="gray"
        )  # Gray dotted grid

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
        ax2.legend(
            lines + lines2, labels + labels2, loc="lower left", bbox_to_anchor=(0, 1)
        )

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

        ax.plot(
            dataframe["citation_year"],
            dataframe["cumulative_citations"],
            marker="o",
            linestyle="-",
            color="blue",
        )
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

        ax.plot(
            dataframe["citation_year"],
            dataframe["perc_cumulative_citations"],
            marker="o",
            linestyle="-",
            color="green",
        )
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


def _generate_temporal_plot(
    temporal_df,
    y_value_col,
    y_perc_col,
    title,
    y_value_label,
    y_perc_label="Percentile",
):
    """Helper function to generate a dual-axis temporal plot."""
    if (
        temporal_df.empty
        or y_value_col not in temporal_df.columns
        or y_perc_col not in temporal_df.columns
    ):
        logging.warning(f"Missing data for temporal plot: {title}")
        return ""  # Return empty string or placeholder image data

    try:
        fig = Figure(figsize=(10, 5), dpi=100)
        ax1 = fig.subplots(1, 1)
        matplotlib.rcParams.update({"font.size": 16})

        # Primary y-axis (Metric Value)
        color1 = "tab:blue"
        ax1.set_xlabel("Year")
        ax1.set_ylabel(y_value_label, color=color1)
        ax1.plot(
            temporal_df["state_year"],
            temporal_df[y_value_col],
            color=color1,
            marker="o",
            label=y_value_label,
        )
        ax1.tick_params(axis="y", labelcolor=color1)
        ax1.grid(True, which="both", linestyle="--", linewidth=0.5, color="gray")

        # Secondary y-axis (Percentile)
        ax2 = ax1.twinx()
        color2 = "tab:red"
        ax2.set_ylabel(y_perc_label, color=color2)
        ax2.plot(
            temporal_df["state_year"],
            temporal_df[y_perc_col] * 100,
            color=color2,
            marker="x",
            linestyle="--",
            label=y_perc_label,
        )  # Multiply percentile by 100
        ax2.tick_params(axis="y", labelcolor=color2)
        ax2.set_ylim(0, 100)  # Percentile axis from 0 to 100

        fig.suptitle(title)
        fig.tight_layout(
            rect=[0, 0.03, 1, 0.95]
        )  # Adjust layout to prevent title overlap

        # Combine legends
        lines, labels = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        # Place legend below plot
        ax1.legend(
            lines + lines2,
            labels + labels2,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.15),
            fancybox=True,
            shadow=True,
            ncol=2,
        )

        buf = BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")  # Use bbox_inches='tight'
        data = base64.b64encode(buf.getbuffer()).decode("ascii")
        return f"data:image/png;base64,{data}"

    except Exception as e:
        logging.error(f"Error generating temporal plot '{title}': {e}")
        return ""  # Return empty string on error


def generate_author_h_index_plot(temporal_df):
    return _generate_temporal_plot(
        temporal_df,
        y_value_col="h_index",
        y_perc_col="h_index_percentile",
        title="H-Index Evolution Over Time",
        y_value_label="H-Index Value",
    )


def generate_author_total_citations_plot(temporal_df):
    return _generate_temporal_plot(
        temporal_df,
        y_value_col="total_citations",
        y_perc_col="total_citations_percentile",
        title="Total Citations Evolution Over Time",
        y_value_label="Total Citations",
    )


def generate_author_i10_index_plot(temporal_df):
    return _generate_temporal_plot(
        temporal_df,
        y_value_col="i10_index",
        y_perc_col="i10_index_percentile",
        title="i10-Index Evolution Over Time",
        y_value_label="i10-Index Value",
    )


# Add similar functions for h_index_5y, i10_index_5y, total_recent_citations_5y, total_publications etc.
# Example for 5y H-index:
def generate_author_h_index_5y_plot(temporal_df):
    return _generate_temporal_plot(
        temporal_df,
        y_value_col="h_index_5y",
        y_perc_col="h_index_5y_percentile",
        title="H-Index (Last 5 Years) Evolution Over Time",
        y_value_label="H-Index (5y) Value",
    )


# --- END NEW ---
