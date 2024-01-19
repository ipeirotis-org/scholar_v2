import logging
from google.cloud import firestore
from scholarly import scholarly
from datetime import datetime
import pytz

from data_sanitization import sanitize_author_data, create_publications_dataframe


# Initialize Firestore client
db = firestore.Client()

def get_firestore_cache(author_id):
    """Retrieves cached data for a given author ID from Firestore."""
    doc_ref = db.collection('scholar_cache').document(author_id)
    try:
        doc = doc_ref.get()
        if doc.exists:
            cached_data = doc.to_dict()
            cached_time = cached_data['timestamp']
            current_time = datetime.utcnow().replace(tzinfo=pytz.utc)
            if (current_time - cached_time).days < 30:
                return cached_data['data']
        return None
    except Exception as e:
        logging.error(f"Error accessing Firestore: {e}")
        return None

def set_firestore_cache(author_id, data):
    """Sets new data in Firestore cache for a given author ID."""
    doc_ref = db.collection('scholar_cache').document(author_id)
    cache_data = {
        'timestamp': datetime.utcnow().replace(tzinfo=pytz.utc),
        'data': data
    }
    try:
        doc_ref.set(cache_data)
    except Exception as e:
        logging.error(f"Error updating Firestore: {e}")

def fetch_from_scholar(author_id):
    """
    Fetches data from Google Scholar for a given author ID, 
    then sanitizes and returns this data.
    """
    try:
        author = scholarly.search_author_id(author_id)
        if author:
            # Filling in the detailed author information
            author = scholarly.fill(author)

            # Sanitize author data
            sanitized_author = sanitize_author_data(author)

            # If publications exist, create a DataFrame from sanitized publication data
            if 'publications' in author:
                sanitized_author['publications'] = create_publications_dataframe(author['publications'])

            return sanitized_author
        else:
            logging.error(f"No author found with ID {author_id}.")
            return None
    except Exception as e:
        logging.error(f"Error fetching data for author with ID {author_id}: {e}")
        return None
        

def get_scholar_data(author_id):
    """Public interface to fetch scholar data, checks cache first."""
    cached_data = get_firestore_cache(author_id)
    if cached_data:
        logging.info(f"Cache hit for author '{author_id}'. Data fetched from Firestore.")
        return cached_data
    else:
        logging.info(f"Cache miss for author '{author_id}'. Fetching data from Google Scholar.")
        scholar_data = fetch_from_scholar(author_id)
        if scholar_data:
            set_firestore_cache(author_id, scholar_data)
        return scholar_data

def get_similar_authors(author_name):
    """Fetches similar authors' data from Google Scholar and caches it."""
    # Create a unique cache key based on the author's name
    cache_key = f"similar_authors_{author_name.lower().replace(' ', '_')}"

    # Check cache first
    cached_authors = get_firestore_cache(cache_key)
    if cached_authors:
        logging.info(f"Cache hit for similar authors of '{author_name}'.")
        return cached_authors

    logging.info(f"Cache miss for similar authors of '{author_name}'. Fetching data from Google Scholar.")

    # Fetch similar authors from Google Scholar
    similar_authors = []
    try:
        search_query = scholarly.search_author(author_name)
        for _ in range(10):
            try:
                author = next(search_query)
                if author:
                    similar_authors.append(author)
            except StopIteration:
                break
    except Exception as e:
        logging.error(f"Error fetching similar authors for '{author_name}': {e}")
        return []

    # Cache the results
    set_firestore_cache(cache_key, similar_authors)
    return similar_authors
