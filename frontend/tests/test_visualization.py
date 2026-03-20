"""Tests for v3 frontend visualization module."""

import pandas as pd
import pytest

from frontend.visualization import (
    generate_percentile_rank_plot,
    generate_pip_plot,
    generate_pub_citation_plot,
    _generate_temporal_plot,
)


@pytest.fixture
def pip_dataframe():
    return pd.DataFrame({
        "publication_rank": [1, 2, 3],
        "num_citations_percentile": [90, 70, 50],
        "num_papers_percentile": [80, 60, 40],
        "age": [5, 3, 1],
    })


@pytest.fixture
def pub_citation_dataframe():
    return pd.DataFrame({
        "citation_year": [2020, 2021, 2022],
        "yearly_citations": [10, 15, 20],
        "perc_yearly_citations": [0.8, 0.85, 0.9],
        "perc_cumulative_citations": [0.7, 0.75, 0.82],
    })


@pytest.fixture
def temporal_dataframe():
    return pd.DataFrame({
        "state_year": [2018, 2019, 2020],
        "h_index": [5, 8, 12],
        "h_index_percentile": [0.6, 0.7, 0.8],
    })


class TestGeneratePercentileRankPlot:
    def test_returns_data_uri(self, pip_dataframe):
        result = generate_percentile_rank_plot(pip_dataframe, "Test Author")
        assert result.startswith("data:image/png;base64,")

    def test_returns_empty_on_bad_data(self):
        result = generate_percentile_rank_plot(pd.DataFrame(), "Test")
        assert result == ""


class TestGeneratePipPlot:
    def test_returns_data_uri(self, pip_dataframe):
        result = generate_pip_plot(pip_dataframe, "Test Author")
        assert result.startswith("data:image/png;base64,")


class TestGeneratePubCitationPlot:
    def test_returns_data_uri(self, pub_citation_dataframe):
        result = generate_pub_citation_plot(pub_citation_dataframe)
        assert result.startswith("data:image/png;base64,")

    def test_returns_empty_on_bad_data(self):
        result = generate_pub_citation_plot(pd.DataFrame())
        assert result == ""


class TestTemporalPlot:
    def test_returns_data_uri(self, temporal_dataframe):
        result = _generate_temporal_plot(
            temporal_dataframe, "h_index", "h_index_percentile",
            "Test Title", "H-Index"
        )
        assert result.startswith("data:image/png;base64,")

    def test_returns_empty_for_empty_df(self):
        result = _generate_temporal_plot(
            pd.DataFrame(), "h_index", "h_index_percentile",
            "Test", "H-Index"
        )
        assert result == ""

    def test_returns_empty_for_missing_column(self, temporal_dataframe):
        result = _generate_temporal_plot(
            temporal_dataframe, "nonexistent", "h_index_percentile",
            "Test", "H-Index"
        )
        assert result == ""
