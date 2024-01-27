import pandas as pd
import numpy as np
import logging

from data_access import get_firestore_cache, set_firestore_cache
from scholar import get_scholar_data

logging.basicConfig(level=logging.INFO)

# Load the dataframes at the start of the module
url = "../data/percentiles.csv"
percentile_df = pd.read_csv(url).set_index("age")
percentile_df.columns = [float(p) for p in percentile_df.columns]

url_author_percentiles = "../data/author_numpapers_percentiles.csv"
author_percentiles = pd.read_csv(url_author_percentiles).set_index(
    "years_since_first_pub"
)

url_pip_auc_percentiles = "../data/pip-auc-percentiles.csv"
pip_auc_percentiles_df = pd.read_csv(url_pip_auc_percentiles).set_index('pip_auc_score')



def get_numpaper_percentiles(year):
    # If the exact year is in the index, use that year
    if year in author_percentiles.index:
        s = author_percentiles.loc[year, :]
    else:
        # If the specific year is not in the index, find the closest year
        closest_year = min(author_percentiles.index, key=lambda x: abs(x - year))

        valid_range = range(0, 100)
        if not all(
            value in valid_range for value in author_percentiles.loc[closest_year]
        ):
            years = sorted(author_percentiles.index)
            year_index = years.index(closest_year)
            prev_year = years[max(0, year_index - 1)]
            next_year = years[min(len(years) - 1, year_index + 1)]
            s = (
                author_percentiles.loc[prev_year, :]
                + author_percentiles.loc[next_year, :]
            ) / 2
        else:
            s = author_percentiles.loc[closest_year, :]

    highest_indices = s.groupby(s).apply(lambda x: x.index[-1])
    sw = pd.Series(index=highest_indices.values, data=highest_indices.index)
    normalized_values = pd.Series(data=sw.index, index=sw.values)
    return normalized_values


def find_closest(series, number):
    if series.empty:
        return np.nan
    differences = np.abs(series - number)
    closest_index = differences.idxmin()
    return series[closest_index]



def score_papers(row):
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


def find_closest_pip_percentile(pip_auc_score):
    if pip_auc_percentiles_df.empty:
        return np.nan

    # Calculate the absolute differences between the pip_auc_score and the pip_auc_score column in the DataFrame
    differences = np.abs(pip_auc_percentiles_df['pip_auc_score'] - pip_auc_score)

    closest_index = differences.idxmin()

    return pip_auc_percentiles_df.loc[closest_index, 'pip_auc_percentile']


def get_author_statistics_by_id(scholar_id):
    cached_data = get_firestore_cache("author_stats", scholar_id)
    if cached_data:
        logging.info(
            f"Cache hit for author stats for '{scholar_id}'. Data fetched from Firestore."
        )

        author_info = cached_data.get("author_info", None)
        publications =  pd.DataFrame(cached_data.get("publications", []))
        total_publications = cached_data.get("total_publications", 0)
        pip_auc = cached_data.get("pip_auc", 0)
        pip_auc_percentile = find_closest_pip_percentile(pip_auc)
        return author_info, publications, total_publications, pip_auc, pip_auc_percentile

    author_info, publications, total_publications, error = get_scholar_data(scholar_id)

    if error:
        logging.error(f"Error fetching data for author with ID {scholar_id}: {error}")
        return None, pd.DataFrame(), 0, 0, np.nan

    if not publications:
        logging.error(
            f"No valid publication data found for author with ID {scholar_id}."
        )
        return None, pd.DataFrame(), 0, 0, np.nan

    publications_df = pd.DataFrame(publications)

    # Assuming score_papers and other necessary functions are defined in this module
    publications_df["percentile_score"] = publications_df.apply(
        score_papers, axis=1
    ).round(2)
    publications_df["paper_rank"] = (
        publications_df["percentile_score"]
        .rank(ascending=False, method="first")
        .astype(int)
    )
    publications_df = publications_df.sort_values("percentile_score", ascending=False)

    # Calculate AUC score
    auc_data = ( publications_df
                .filter(["num_papers_percentile", "percentile_score"])
                .drop_duplicates(subset="num_papers_percentile", keep="first")
               )
    pip_auc_score = np.trapz(
        auc_data["percentile_score"], auc_data["num_papers_percentile"]
    ) / (100 * 100)

    pip_auc_score = round(pip_auc_score, 4)

    # Calculate PiP-AUC Percentile
    pip_auc_percentile = find_closest_pip_percentile(pip_auc_score)

    set_firestore_cache(
        "author_stats",
        scholar_id,
        {
            "author_info": author_info,
            "publications": publications_df.to_dict(orient='records'),
            "total_publications": total_publications,
            "pip_auc": pip_auc_score
        },
    )

    return author_info, publications_df, total_publications, pip_auc_score, pip_auc_percentile
