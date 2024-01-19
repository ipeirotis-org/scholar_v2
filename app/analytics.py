import pandas as pd
import numpy as np
import logging

# Assuming percentile data is available in CSV files
percentile_df = pd.read_csv("../data/percentiles.csv").set_index("age")
percentile_df.columns = [float(p) for p in percentile_df.columns]

author_percentiles = pd.read_csv("../data/author_numpapers_percentiles.csv").set_index("years_since_first_pub")

def find_closest(series, number):
    """Finds the closest value in a series to a given number."""
    if series.empty:
        return np.nan
    differences = np.abs(series.index - number)
    closest_index = differences.argmin()
    return series.iloc[closest_index]

def normalize_paper_count(years_since_first_pub):
    """Normalizes the paper count based on the year since first publication."""
    differences = np.abs(np.array(author_percentiles.index) - years_since_first_pub)
    closest_year_index = np.argmin(differences)
    closest_year = author_percentiles.iloc[closest_year_index]

    for percentile in closest_year.index[1:]:
        if years_since_first_pub <= closest_year.loc[percentile]:
            return float(percentile) / 100
    return None

def score_papers(row):
    """Calculates the percentile score for a given paper."""
    age, citations = row["age"], row["citations"]
    if age not in percentile_df.index:
        closest_year = percentile_df.index[np.abs(percentile_df.index - age).argmin()]
        percentiles = percentile_df.loc[closest_year]
    else:
        percentiles = percentile_df.loc[age]

    if citations <= percentiles.min():
        return 0.0
    elif citations >= percentiles.max():
        return 100.0
    else:
        below = percentiles[percentiles <= citations].idxmax()
        above = percentiles[percentiles >= citations].idxmin()
        if above == below:
            return above
        else:
            lower_bound = percentiles[below]
            upper_bound = percentiles[above]
            weight = (citations - lower_bound) / (upper_bound - lower_bound)
            return below + weight * (above - below)

def calculate_paper_percentiles(publications_df):
    """Adds percentile scores to a DataFrame of publications."""
    try:
        publications_df["percentile_score"] = publications_df.apply(score_papers, axis=1).round(2)
        # Calculate additional analytics as needed
        return publications_df
    except Exception as e:
        logging.error(f"Error calculating paper percentiles: {e}")
        return publications_df
