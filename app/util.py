import matplotlib
import pandas as pd
import numpy as np
from scholarly import scholarly
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import os
import logging
from sklearn.metrics import auc
import pytz
from google.cloud import firestore
db = firestore.Client()

matplotlib.use("Agg")

url = "../data/percentiles.csv"
percentile_df = pd.read_csv(url).set_index("age")
percentile_df.columns = [float(p) for p in percentile_df.columns]


url_author_percentiles = "../data/author_numpapers_percentiles.csv"
author_percentiles = pd.read_csv(url_author_percentiles).set_index(
    "years_since_first_pub"
)

def get_firestore_cache(author_name):
    if not author_name.strip():  # Check if author_name is not empty and not just whitespace
        logging.error("Invalid author name for Firestore cache.")
        return None

    doc_ref = db.collection('scholar_cache').document(author_name)
    try:
        doc = doc_ref.get()
        if doc.exists:
            cached_data = doc.to_dict()
            cached_time = cached_data['timestamp']
            if isinstance(cached_time, datetime):  # Making sure it's a datetime object
                cached_time = cached_time.replace(tzinfo=pytz.utc)
            current_time = datetime.utcnow().replace(tzinfo=pytz.utc)
            if (current_time - cached_time).days < 7:
                return cached_data['data']
    except Exception as e:
        logging.error(f"Error accessing Firestore: {e}")
    return None

def set_firestore_cache(author_name, data):
    if not author_name.strip():  # Check if author_name is not empty and not just whitespace
        logging.error("Invalid author name for Firestore cache.")
        return

    doc_ref = db.collection('scholar_cache').document(author_name)
    cache_data = {
        'timestamp': datetime.utcnow().replace(tzinfo=pytz.utc),
    }
    if isinstance(data, list):
        cache_data['data'] = data
    else:
        cache_data.update(data)

    try:
        doc_ref.set(cache_data)
    except Exception as e:
        logging.error(f"Error updating Firestore: {e}")





def get_scholar_data(author_name, multiple=False):
    cached_data = get_firestore_cache(author_name)
    if cached_data:
        if 'publications' in cached_data:
            author_info = cached_data.get('author_info', None)  # Assuming you also cache author info
            publications = cached_data['publications']
            total_publications = len(publications)
            return author_info, publications, total_publications, None
        else:
            logging.error("Cached data is not in the expected format for a single author.")

    try:
        search_query = scholarly.search_author(author_name)
    except Exception as e:
        logging.error(f"Error fetching author data: {e}")
        return None, [], 0, str(e)

    authors = []
    try:
        for _ in range(10):  # Fetch up to 10 authors for the given name
            author = next(search_query)
            authors.append(author)
    except StopIteration:
        pass
    except Exception as e:
        logging.error(f"Error iterating through author data: {e}")
        return None, [], 0, str(e)

    if not authors:
        logging.warning("No authors found.")
        return None, [], 0, "No authors found."

    logging.info(f"Found {len(authors)} authors.")

    if multiple:
        for author in authors:
            sanitize_author_data(author)
        # Cache the multiple author data
        set_firestore_cache(author_name, {'publications': authors})
        return authors, None, None, None

    if len(authors) > 1:
        author = max(authors, key=lambda a: a.get("citedby", 0))
    else:
        author = authors[0]

    try:
        author = scholarly.fill(author)
    except Exception as e:
        logging.error(f"Error fetching detailed author data: {e}")
        return None, [], 0, str(e)

    now = datetime.now(pytz.utc)
    timestamp = int(now.timestamp())
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

    set_firestore_cache(author_name, {'publications': publications})

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


def get_numpaper_percentiles(year):
    author_percentiles = pd.read_csv(url_author_percentiles).set_index("years_since_first_pub")
    s = author_percentiles.loc[year,:]
    highest_indices = s.groupby(s).apply(lambda x: x.index[-1])
    sw = pd.Series(index=highest_indices.values, data=highest_indices.index)
    normalized_values = pd.Series(data=sw.index, index=sw.values)
    return normalized_values

def find_closest(series, number):
    differences = np.abs(series.index - number)
    closest_index = differences.argmin()
    return series.iloc[closest_index]
    

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
        return None, pd.DataFrame(), 0

    pubs = [
        {
            "citations": p["citedby"],
            "age": datetime.now().year - int(p["bib"].get("pub_year", 0)) + 1,
            "title": p["bib"].get("title"),
            "year": int(p["bib"].get("pub_year")) if p["bib"].get("pub_year") else None
        }
        for p in publications
        if "pub_year" in p["bib"] and p["citedby"] is not None
    ]

    query_df = pd.DataFrame(pubs)
    query_df["percentile_score"] = query_df.apply(score_papers, axis=1).round(2)
    query_df["paper_rank"] = query_df["percentile_score"].rank(ascending=False, method='first').astype(int)
    query_df = query_df.sort_values("percentile_score", ascending=False)

    year = query_df['age'].max()
    num_papers_percentile = get_numpaper_percentiles(year)
    query_df['num_papers_percentile'] = query_df['paper_rank'].apply(lambda x: find_closest(num_papers_percentile, x))
    query_df['num_papers_percentile'] = query_df['num_papers_percentile'].astype(float)

    query_df = query_df.sort_values('percentile_score', ascending=False)

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

    if error is not None or author is None or publications is None:
        logging.error(f"Error fetching data for author {author_name}: {error}")
        return {}  # Return an empty dictionary if there's an error or no data

    yearly_data = {}
    for pub in publications:  # Now publications is guaranteed not to be None
        year = int(pub["bib"].get("pub_year", 0))

        if start_year and year < start_year:
            continue
        if end_year and year > end_year:
            continue

        if year not in yearly_data:
            yearly_data[year] = {"citations": 0, "publications": 0, "scores": 0}
        yearly_data[year]["citations"] += pub["citedby"]
        yearly_data[year]["publications"] += 1

        # Calculate the percentile score and add it to the yearly score
        pub_df = pd.DataFrame([{
            "citations": pub["citedby"],
            "age": datetime.now().year - year + 1,
            "title": pub["bib"].get("title"),
        }])
        pub_df["percentile_score"] = pub_df.apply(score_papers, axis=1)
        yearly_data[year]["scores"] += pub_df["percentile_score"].iloc[0]

    # Normalize the scores by the number of publications
    for year in yearly_data:
        if yearly_data[year]["publications"] > 0:
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
        dataframe.plot.scatter(
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

        # Generate the line plot for percentile scores across author productivity percentiles
        plt.figure(figsize=(8, 8))
        dataframe.plot.scatter(
            x='num_papers_percentile',
            y='percentile_score',
            c='age',
            cmap='Blues_r',
            s=2,
            grid=True,
            xlim=(0, 100),
            ylim=(0, 100),
            title=f"Percentile Score Across Author Productivity Percentiles for {author_name}",
        )
        plt.xlabel("Author Productivity Percentile")
        plt.ylabel("Paper Percentile Score")
        line_plot_path = os.path.join(
            "static", f"{cleaned_name}_percentile_score_scatter_plot.png"
        )
        plt.savefig(line_plot_path)
        plt.close()
        plot_paths.append(line_plot_path)

        # Calculate AUC score
        auc_data = dataframe.filter(['num_papers_percentile', 'percentile_score']).drop_duplicates(subset='num_papers_percentile', keep='first')
        pip_auc_score = np.trapz(auc_data['percentile_score'], auc_data['num_papers_percentile']) / (100 * 100)
        print(f"AUC score: {pip_auc_score:.4f}")

    except Exception as e:
        logging.error(f"Error in generate_plot for {author_name}: {e}")
        raise

    return plot_paths, pip_auc_score



