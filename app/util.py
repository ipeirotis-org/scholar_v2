import pandas as pd
import matplotlib
import numpy as np
from scholarly import scholarly
from datetime import datetime, timedelta
from matplotlib.figure import Figure
import os
import logging
from sklearn.metrics import auc
import pytz
from google.cloud import firestore

db = firestore.Client()
url = "../data/percentiles.csv"
percentile_df = pd.read_csv(url).set_index("age")
percentile_df.columns = [float(p) for p in percentile_df.columns]


url_author_percentiles = "../data/author_numpapers_percentiles.csv"
author_percentiles = pd.read_csv(url_author_percentiles).set_index(
    "years_since_first_pub"
)

def get_firestore_cache(author_id):
    doc_ref = db.collection('scholar_cache').document(author_id)
    try:
        doc = doc_ref.get()
        if doc.exists:
            cached_data = doc.to_dict()
            cached_time = cached_data['timestamp']
            if isinstance(cached_time, datetime):  
                cached_time = cached_time.replace(tzinfo=pytz.utc)
            current_time = datetime.utcnow().replace(tzinfo=pytz.utc)
            if (current_time - cached_time).days < 30:
                return cached_data['data']
            else:
                return None 
    except Exception as e:
        logging.error(f"Error accessing Firestore: {e}")
    return None

def set_firestore_cache(author_id, data):
    firestore_author_id = str(author_id).lower()  # Ensure the ID is a string and lowercase
    doc_ref = db.collection('scholar_cache').document(firestore_author_id)

    cache_data = {
        'timestamp': datetime.utcnow().replace(tzinfo=pytz.utc),
        'data': {
            'author_info': data.get('author_info', {}),
            'publications': data.get('publications', [])
        }
    }
    if not firestore_author_id.strip():
        logging.error("Firestore document ID is empty or invalid.")
        return

    if not cache_data['data']['author_info'] or not cache_data['data']['publications']:
        logging.error("Invalid author_info or publications data for caching.")
        return

    try:
        doc_ref.set(cache_data)
    except Exception as e:
        logging.error(f"Error updating Firestore: {e}")




def get_scholar_data(author_name, multiple=False):
    firestore_author_name = author_name.lower()
    cached_data = get_firestore_cache(firestore_author_name)

    if cached_data:
        logging.info(f"Cache hit for author '{author_name}'. Data fetched from Firestore.")
        author_info = cached_data.get('author_info', None)
        publications = cached_data.get('publications', [])

        if author_info and publications:
            total_publications = len(publications)
            return author_info, publications, total_publications, None
        else:
            logging.error("Cached data is not in the expected format for a single author.")
            # If cached data is not in the expected format, proceed to fetch from Google Scholar
            return fetch_from_scholar(author_name, multiple)
    else:
        # Cache miss or cached data not useful, fetch from Google Scholar
        return fetch_from_scholar(author_name, multiple)

def fetch_from_scholar(author_name, multiple):
    logging.info(f"Cache miss for author '{author_name}'. Fetching data from Google Scholar.")
    try:
        search_query = scholarly.search_author(author_name)
    except Exception as e:
        logging.error(f"Error fetching author data: {e}")
        return None, [], 0, str(e)

    authors = []
    try:
        for _ in range(10 if multiple else 1):
            author = next(search_query, None)
            if author:
                authors.append(author)
    except Exception as e:
        logging.error(f"Error iterating through author data: {e}")
        return None, [], 0, str(e)

    if not authors:
        logging.warning("No authors found.")
        return None, [], 0, "No authors found."

    if multiple:
        for author in authors:
            sanitize_author_data(author)
        set_firestore_cache(author_name, {'publications': authors})
        return authors, None, None, None

    author = authors[0]
    try:
        author = scholarly.fill(author)
    except Exception as e:
        logging.error(f"Error fetching detailed author data: {e}")
        return None, [], 0, str(e)

    publications = [sanitize_publication_data(pub, int(datetime.now().timestamp()), datetime.now().strftime("%Y-%m-%d %H:%M:%S")) for pub in author.get('publications', [])]
    total_publications = len(publications)
    author_info = extract_author_info(author, total_publications)

    set_firestore_cache(author_name, {'author_info': author_info, 'publications': publications})
    return author_info, publications, total_publications, None


def extract_author_info(author, total_publications):
    return {
        'name': author.get('name', 'Unknown'),
        'affiliation': author.get('affiliation', 'Unknown'),
        'scholar_id': author.get('scholar_id', 'Unknown'),
        'citedby': author.get('citedby', 0),
        'total_publications': total_publications
    }






def sanitize_author_data(author):
    if "citedby" not in author:
        author["citedby"] = 0

    if "name" not in author:
        author["name"] = "Unknown"


def sanitize_publication_data(pub, timestamp, date_str):
    try:
        citedby = int(pub.get("num_citations", 0))
        pub["citedby"] = citedby
        pub["last_updated_ts"] = timestamp
        pub["last_updated"] = date_str

        # Handle potential serialization issues
        if "source" in pub and hasattr(pub["source"], "name"):
            pub["source"] = pub["source"].name
        else:
            pub.pop("source", None)  

        return pub 
    except Exception as e:
        logging.error(f"Error sanitizing publication data: {e}")
        return None  # Return None if there's an error


def get_numpaper_percentiles(year):
    # If the exact year is in the index, use that year
    if year in author_percentiles.index:
        s = author_percentiles.loc[year, :]
    else:
        # If the specific year is not in the index, find the closest year
        closest_year = min(author_percentiles.index, key=lambda x: abs(x - year))
        
        valid_range = range(0, 100)
        if not all(value in valid_range for value in author_percentiles.loc[closest_year]):
            years = sorted(author_percentiles.index)
            year_index = years.index(closest_year)
            prev_year = years[max(0, year_index - 1)]
            next_year = years[min(len(years) - 1, year_index + 1)]
            s = (author_percentiles.loc[prev_year, :] + author_percentiles.loc[next_year, :]) / 2
        else:
            s = author_percentiles.loc[closest_year, :]
    
    highest_indices = s.groupby(s).apply(lambda x: x.index[-1])
    sw = pd.Series(index=highest_indices.values, data=highest_indices.index)
    normalized_values = pd.Series(data=sw.index, index=sw.values)
    return normalized_values




def find_closest(series, number):
    if series.empty:
        return np.nan
    differences = np.abs(series.index - number)
    closest_index = differences.argmin()
    return series.iloc[closest_index]
    

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



def get_author_statistics_by_id(scholar_id):
    cached_data = get_firestore_cache(scholar_id)
    if cached_data:
        logging.info(f"Cache hit for author ID '{scholar_id}'. Data fetched from Firestore.")
        author_info = cached_data['author_info']
        publications_df = pd.DataFrame(cached_data['publications'])
        total_publications = len(publications_df)
        return author_info, publications_df, total_publications
    else:
        logging.info(f"Cache miss for author ID '{scholar_id}'. Fetching data from Google Scholar.")
        try:
            author = scholarly.search_author_id(scholar_id)
            if author:
                author = scholarly.fill(author)
                now = datetime.now(pytz.utc)
                timestamp = int(now.timestamp())
                date_str = now.strftime("%Y-%m-%d %H:%M:%S")

                pubs = []
                for p in author.get('publications', []):
                    if 'bib' in p and 'title' in p['bib']:
                        pub_year = p['bib'].get('pub_year')
                        if pub_year is None or int(pub_year)<1950: continue
                        sanitized_pub = sanitize_publication_data(p, timestamp, date_str)
                        if sanitized_pub and 'citedby' in sanitized_pub and sanitized_pub["citedby"]>0:
                            pub_info = {
                                "citations": sanitized_pub["citedby"],
                                "age": now.year - int(pub_year) + 1,
                                "title": p["bib"].get("title"),
                                "year": int(pub_year)
                            }
                            pubs.append(pub_info)

                if not pubs:
                    logging.error(f"No valid publication data found for author with ID {scholar_id}.")
                    return None, pd.DataFrame(), 0

                query_df = pd.DataFrame(pubs)
                query_df["percentile_score"] = query_df.apply(score_papers, axis=1).round(2)
                query_df["paper_rank"] = query_df["percentile_score"].rank(ascending=False, method='first').astype(int)
                query_df = query_df.sort_values("percentile_score", ascending=False)

                year = query_df['age'].max()
                num_papers_percentile = get_numpaper_percentiles(year)
                if num_papers_percentile.empty:
                    logging.error("Empty num_papers_percentile series.")
                    return None, pd.DataFrame(), 0
                query_df['num_papers_percentile'] = query_df['paper_rank'].apply(lambda x: find_closest(num_papers_percentile, x))
                query_df['num_papers_percentile'] = query_df['num_papers_percentile'].astype(float)

                query_df = query_df.sort_values('percentile_score', ascending=False)

                author_info = extract_author_info(author, len(pubs))
                set_firestore_cache(scholar_id, {'author_info': author_info, 'publications': query_df.to_dict(orient='records')})

                return author_info, query_df, len(pubs)
            else:
                logging.error(f"No author found with ID {scholar_id}.")
                return None, pd.DataFrame(), 0
        except Exception as e:
            logging.error(f"Error fetching data for author with ID {scholar_id}: {e}")
            return None, pd.DataFrame(), 0










def normalize_paper_count(years_since_first_pub):
    differences = np.abs(np.array(author_percentiles.index) - years_since_first_pub)
    closest_year_index = np.argmin(differences)
    closest_year = author_percentiles.iloc[closest_year_index]

    for percentile in closest_year.index[1:]:
        if years_since_first_pub <= closest_year.loc[percentile]:
            return float(percentile) / 100
    return None

def generate_plot(dataframe, author_name):
    plot_paths = []
    pip_auc_score = 0
    try:
        cleaned_name = "".join([c if c.isalnum() else "_" for c in author_name])
        fig = Figure(figsize=(20, 10), dpi=100)
        ax1, ax2 = fig.subplots(1, 2)  # Adjusted for better resolution
        
        matplotlib.rcParams.update({'font.size': 16})
       
        marker_size = 40

        # First subplot (Rank vs Percentile Score)
        scatter1 = ax1.scatter(
            dataframe["paper_rank"],
            dataframe["percentile_score"],
            c=dataframe['age'],
            cmap="Blues_r",
            s=marker_size 
        )
        colorbar1 = fig.colorbar(scatter1, ax=ax1) 
        colorbar1.set_label('Years since Publication')
        ax1.set_title(f"Paper Percentile Scores for {author_name}")
        ax1.set_xlabel("Paper Rank")
        ax1.set_ylabel("Paper Percentile Score")
        ax1.grid(True) 

        # Second subplot (Productivity Percentiles)
        scatter2 = ax2.scatter(
            dataframe['num_papers_percentile'],
            dataframe['percentile_score'],
            c=dataframe['age'],
            cmap='Blues_r',
            s=marker_size  
        )
        colorbar2 = fig.colorbar(scatter2, ax=ax2) 
        colorbar2.set_label('Years since Publication')
        ax2.set_title(f"Paper Percentile Scores vs #Papers Percentile for {author_name}")
        ax2.set_xlabel("Number of Papers Published Percentile")
        ax2.set_ylabel("Paper Percentile Score")
        ax2.grid(True) 
        ax2.set_xlim(0, 100)
        ax2.set_ylim(0, 100)

        fig.tight_layout()
        combined_plot_path = os.path.join("static", f"{cleaned_name}_combined_plot.png")
        fig.savefig(combined_plot_path)
        plot_paths.append(combined_plot_path)

        # Calculate AUC score
        auc_data = dataframe.filter(['num_papers_percentile', 'percentile_score']).drop_duplicates(subset='num_papers_percentile', keep='first')
        pip_auc_score = np.trapz(auc_data['percentile_score'], auc_data['num_papers_percentile']) / (100 * 100)
        # print(f"AUC score: {pip_auc_score:.4f}")

    except Exception as e:
        logging.error(f"Error in generate_plot for {author_name}: {e}")
        raise

    return plot_paths, round(pip_auc_score,4)


def check_and_add_author_to_cache(author_name):
    firestore_author_name = author_name.lower()
    doc_ref = db.collection('scholar_cache').document(firestore_author_name)
    doc = doc_ref.get()
    if not doc.exists:
        doc_ref.set({
            'name': author_name,
            'cached_on': datetime.utcnow()
        })

