import pandas as pd
import numpy as np
import logging
import datetime
from data_access import get_firestore_cache, set_firestore_cache
from scholar import get_author, get_publication, get_author_publications

logging.basicConfig(level=logging.INFO)

# Load the dataframes at the start of the module
paper_percentiles_url = "../data/percentiles.csv"
percentile_df = pd.read_csv(paper_percentiles_url).set_index("age")
percentile_df.index = percentile_df.index.astype(float)
percentile_df.columns = [float(p) for p in percentile_df.columns]

url_author_percentiles = "../data/author_numpapers_percentiles.csv"
author_percentiles = pd.read_csv(url_author_percentiles).set_index(
    "years_since_first_pub"
)

url_pip_auc_percentiles = "../data/pip-auc-percentiles.csv"
pip_auc_percentiles_df = pd.read_csv(url_pip_auc_percentiles)


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
    series = series.reset_index().set_index(0)
    differences = np.abs(series - number)
    closest_index = differences.idxmin()
    return float(closest_index.values[0])


def score_papers(row):
    age, citations = row["age"], row["num_citations"]

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
    differences = np.abs(pip_auc_percentiles_df["pip_auc_score"] - pip_auc_score)

    closest_index = differences.idxmin()

    return pip_auc_percentiles_df.loc[closest_index, "pip_auc_percentile"]



def get_age_paper_percentile(age):
    
    if age < 1: age = 1
    if age > 40: age = 40
    
    # Get the percentiles for the given age
    percentiles = percentile_df.T[age]
    percentiles.index = percentiles.index.astype(float)

    return percentiles
    


def find_percentile(number, percentiles):

    # If the number is less than the minimum percentile, return 0 percentile
    if number <= percentiles.min():
        return 0.0
    # If the number is greater than the maximum percentile, return 100 percentile
    elif number >= percentiles.max():
        return 100.0
    else:
        # Find the two closest percentiles
        below = percentiles[percentiles <= number].idxmax()
        above = percentiles[percentiles >= number].idxmin()

        # Interpolate the score (or simply use the closest percentile)
        if above == below:
            return above
        else:
            # Linear interpolation
            lower_bound = percentiles[below]
            upper_bound = percentiles[above]
            weight = (number - lower_bound) / (upper_bound - lower_bound)
            return below + weight * (above - below)
     

def calculate_publication_stats(publications):

    current_year = datetime.datetime.now().year
    publications_df = pd.DataFrame(publications)

    publications_df['age'] = current_year - publications_df['pub_year'].astype(int) + 1
    publications_df["percentile_score"] = publications_df.apply(score_papers, axis=1).round(2)
    publications_df["paper_rank"] = publications_df["percentile_score"].rank(ascending=False, method="first").astype(int)
    publications_df = publications_df.sort_values("percentile_score", ascending=False)

    num_papers_percentile = get_numpaper_percentiles(publications_df["age"].max())
    publications_df["num_papers_percentile"] = publications_df["paper_rank"].apply(lambda x: find_closest(num_papers_percentile, x))

    return publications_df.to_dict(orient="records")


def calculate_author_stats(publications):

    if len(publications)==0:
        return {
            "total_publications": 0,
            "total_publications_percentile": 0,
            "pip_auc": 0,
            "pip_auc_percentile": 0,
            "first_year_active": "Unknown",
            "years_active": "Unknown",
        }

        
    publications_df = pd.DataFrame(publications)

    current_year = datetime.datetime.now().year
    first_year_active = int(publications_df["pub_year"].values.min())
    years_active = current_year - first_year_active
    
    # Calculate AUC score
    auc_data = publications_df.filter(["num_papers_percentile", "percentile_score"])
    auc_data = auc_data.drop_duplicates(subset="num_papers_percentile", keep="first")
    pip_auc_score = np.trapz(auc_data["percentile_score"], auc_data["num_papers_percentile"]) / (100 * 100)
    pip_auc_score = round(pip_auc_score, 4)
    pip_auc_percentile = find_closest_pip_percentile(pip_auc_score)

    total_publications_percentile = round(publications_df["num_papers_percentile"].values.max(), 2)
    
    return {
        "total_publications": len(publications),
        "total_publications_percentile": total_publications_percentile,
        "pip_auc": pip_auc_score,
        "pip_auc_percentile": pip_auc_percentile,
        "first_year_active": first_year_active,
        "years_active": years_active,
    }

def sanitize_publication(pub):
    try:
        citations = pub.get("num_citations")
        if not citations:
            return None
        
        
        if "bib" not in pub:
            return None

        for c in pub['bib']: 
            pub[c] = pub["bib"].get(c)

        pub_year = pub.get("pub_year")
        if not pub_year:
            return None

        year = int(pub_year)
        if year < 1950:
            return None

        del pub['bib']
        del pub['citedby_url']
        del pub['cites_id']
        del pub['container_type']
        del pub['filled']
        del pub['source']

        return pub

    except Exception as e:
        logging.error(f"Error sanitizing publication data: {e}")
        return None  # Return None if there's an error

def get_author_stats(author_id):

    current_year = datetime.datetime.now().year
    author = get_author(author_id)
    
    if not author: return None

    pubs = []
    author_pubs = get_author_publications(author_id)
    for p in author_pubs:
        pub = sanitize_publication(p)
        if pub: pubs.append(pub)

    if len(pubs)>0:
        author['publications'] = calculate_publication_stats(pubs)
    else:
        author['publications'] = []

    full_stats = get_firestore_cache("author_stats",author_id)
    if full_stats:
        author['stats'] = full_stats
    else:
        author['stats'] = calculate_author_stats(author['publications'])

    # set_firestore_cache("author_stats",author_id,author)

    return author

        
