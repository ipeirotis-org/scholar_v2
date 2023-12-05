import matplotlib
import pandas as pd
import numpy as np
from scholarly import scholarly
from datetime import datetime
import matplotlib.pyplot as plt
import os
import logging
from sklearn.metrics import auc

matplotlib.use("Agg")

url = "../data/percentiles.csv"
percentile_df = pd.read_csv(url).set_index("age")
percentile_df.columns = [float(p) for p in percentile_df.columns]


url_author_percentiles = "../data/author_numpapers_percentiles.csv"
author_percentiles = pd.read_csv(url_author_percentiles).set_index(
    "years_since_first_pub"
)


def get_scholar_data(author_name, multiple=False):
    logging.info(f"Fetching data for author: {author_name}")

    try:
        search_query = scholarly.search_author(author_name)
    except Exception as e:
        logging.error(f"Error fetching author data: {e}")
        return None, None, None, str(e)

    authors = []
    try:
        for _ in range(10):  # Fetch up to 10 authors for the given name
            authors.append(next(search_query))
    except StopIteration:
        pass
    except Exception as e:
        logging.error(f"Error iterating through author data: {e}")
        return None, None, None, str(e)

    if not authors:
        logging.warning("No authors found.")
        return None, None, None, "No authors found."

    logging.info(f"Found {len(authors)} authors.")

    if multiple:
        for author in authors:
            sanitize_author_data(author)
        return authors, None, None, None

    if len(authors) > 1:
        author = max(authors, key=lambda a: a.get("citedby", 0))
    else:
        author = authors[0]

    try:
        author = scholarly.fill(author)
    except Exception as e:
        logging.error(f"Error fetching detailed author data: {e}")
        return None, None, None, str(e)

    now = datetime.now()
    timestamp = int(datetime.timestamp(now))
    date_str = now.strftime("%Y-%m-%d %H:%M:%S")

    publications = []
    for pub in author.get("publications", []):
        try:
            sanitize_publication_data(pub, timestamp, date_str)
            publications.append(pub)
        except Exception as e:
            logging.warning(f"Skipping a publication due to error: {e}")

    total_publications = len(publications)
    author["last_updated_ts"] = timestamp
    author["last_updated"] = date_str
    del author["publications"]

    return author, publications, total_publications, None


def sanitize_author_data(author):
    if "citedby" not in author:
        author["citedby"] = 0

    if "name" not in author:
        author["name"] = "Unknown"


def sanitize_publication_data(pub, timestamp, date_str):
    citedby = int(pub.get("num_citations", 0))
    pub["citedby"] = citedby
    pub["last_updated_ts"] = timestamp
    pub["last_updated"] = date_str

    # Handle potential serialization issues
    if "source" in pub and hasattr(pub["source"], "name"):
        pub["source"] = pub["source"].name
    else:
        pub.pop("source", None)  # Remove source if it's not serializable


def score_papers(row):
    age, citations = row["age"], row["citations"]
    if age not in percentile_df.index:
        nearest_age = percentile_df.index[np.abs(percentile_df.index - age).argmin()]
    else:
        nearest_age = age
    percentiles = percentile_df.loc[nearest_age]
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


def get_author_statistics(author_name):
    author, publications, total_publications, error = get_scholar_data(author_name)

    if error is not None or author is None:
        logging.error(f"Error fetching data for author {author_name}: {error}")
        return None, pd.DataFrame(), 0  # Return empty DataFrame and zero publications

    pubs = [
        {
            "citations": p["citedby"],
            "age": datetime.now().year - int(p["bib"].get("pub_year", 0)) + 1
            if p["bib"].get("pub_year")
            else None,
            "title": p["bib"].get("title"),
            "year": int(p["bib"].get("pub_year")) if p["bib"].get("pub_year") else None
        }
        for p in publications
        if "pub_year" in p["bib"]
    ]

    query_df = pd.DataFrame(pubs)
    query_df["percentile_score"] = query_df.apply(score_papers, axis=1).round(2)
    query_df["paper_rank"] = (
        query_df["percentile_score"].rank(ascending=False, method="first").astype(int)
    )
    query_df = query_df.sort_values("percentile_score", ascending=False)

    return author, query_df, total_publications



def best_year(yearly_data):
    best_score = -1
    best_year = None

    max_citations = max([data["citations"] for data in yearly_data.values()])
    max_publications = max([data["publications"] for data in yearly_data.values()])
    max_scores = max([data["scores"] for data in yearly_data.values()])

    for year, data in yearly_data.items():
        normalized_citations = (
            data["citations"] / max_citations if max_citations != 0 else 0
        )
        normalized_publications = (
            data["publications"] / max_publications if max_publications != 0 else 0
        )
        normalized_scores = data["scores"] / max_scores if max_scores != 0 else 0

        combined_metric = (
            normalized_citations + normalized_publications + normalized_scores
        )

        if combined_metric > best_score:
            best_score = combined_metric
            best_year = year

    return best_year


def get_yearly_data(author_name, start_year=None, end_year=None):
    author, publications, total_publications, error = get_scholar_data(author_name)

    if author is None:
        return None

    yearly_data = {}
    for pub in publications:
        year = int(pub["bib"].get("pub_year", 0))

        if start_year and year < start_year:
            continue
        if end_year and year > end_year:
            continue

        if year not in yearly_data:
            yearly_data[year] = {"citations": 0, "publications": 0, "scores": 0}
        yearly_data[year]["citations"] += pub["citedby"]
        yearly_data[year]["publications"] += 1

        pub_df = pd.DataFrame(
            [
                {
                    "citations": pub["citedby"],
                    "age": datetime.now().year - year + 1,
                    "title": pub["bib"].get("title"),
                }
            ]
        )
        pub_df["percentile_score"] = pub_df.apply(score_papers, axis=1)
        yearly_data[year]["scores"] += pub_df["percentile_score"].iloc[0]

    for year in yearly_data:
        yearly_data[year]["scores"] /= yearly_data[year]["publications"]

    return yearly_data


def normalize_paper_count(years_since_first_pub):
    # Convert the difference to a NumPy array before applying abs()
    differences = np.abs(np.array(author_percentiles.index) - years_since_first_pub)
    closest_year_index = np.argmin(differences)
    closest_year = author_percentiles.iloc[closest_year_index]

    for percentile in closest_year.index[1:]:
        if years_since_first_pub <= closest_year.loc[percentile]:
            return float(percentile) / 100
    return None


def calculate_pip_auc(dataframe):
    dataframe['normalized_rank'] = dataframe['paper_rank'] / dataframe['paper_rank'].max()
    dataframe['normalized_percentile_score'] = dataframe['percentile_score'] / 100.0
    sorted_df = dataframe.sort_values('normalized_rank')
    pip_auc = auc(sorted_df['normalized_rank'], sorted_df['normalized_percentile_score'])
    return pip_auc


def generate_plot(dataframe, author_name):
    plot_paths = []
    pip_auc_score = 0
    try:
        cleaned_name = "".join([c if c.isalnum() else "_" for c in author_name])

        # Generate the first plot
        plt.figure(figsize=(10, 6))
        ax1 = dataframe.plot.scatter(
            x="paper_rank",
            y="percentile_score",
            c="age",
            cmap="Blues_r",
            s=2,
            title=f"Paper Rank vs Percentile Score for {author_name}",
        )
        plt.xlabel("Paper Rank")
        plt.ylabel("Percentile Score")
        rank_plot_path = os.path.join(
            "static", f"{cleaned_name}_rank_productivity_plot.png"
        )
        plt.savefig(rank_plot_path)
        plt.close()
        plot_paths.append(rank_plot_path)

        # Generate the age-normalized plot
        plt.figure(figsize=(10, 6))
        ax2 = dataframe.plot.scatter(
            x='age',
            y='percentile_score',
            c='DarkBlue',
            title=f"Age-Normalized Paper Rank vs Percentile Score for {author_name}",
        )
        plt.xlabel("Age (Years since publication)")
        plt.ylabel("Percentile Score")
        age_normalized_plot_path = os.path.join(
            "static", f"{cleaned_name}_age_normalized_productivity_plot.png"
        )
        plt.savefig(age_normalized_plot_path)
        plt.close()
        plot_paths.append(age_normalized_plot_path)

        # Calculate PiP-AUC score
        pip_auc_score = calculate_pip_auc(dataframe)

    except Exception as e:
        logging.error(f"Error in generate_plot for {author_name}: {e}")
        raise

    return plot_paths, pip_auc_score

