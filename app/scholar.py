import logging
from scholarly import scholarly
from datetime import datetime
import pytz
from data_access import get_firestore_cache, set_firestore_cache

def get_scholar_data(author_id):
    cached_data = get_firestore_cache("author", author_id)

    if cached_data:
        logging.info(f"Cache hit for author '{author_id}'. Data fetched from Firestore.")
        author_info = cached_data.get('author_info', None)
        publications = cached_data.get('publications', [])

        if author_info and publications:
            total_publications = len(publications)
            return author_info, publications, total_publications, None
        else:
            logging.error("Cached data is not in the expected format for a single author.")
            return fetch_from_scholar(author_id)
    else:
        return fetch_from_scholar(author_id)

def fetch_from_scholar(author_id):
    logging.info(f"Cache miss for author '{author_id}'. Fetching data from Google Scholar.")
    try:
        author = scholarly.search_author_id(author_id)
    except Exception as e:
        logging.error(f"Error fetching author data: {e}")
        return None, [], 0, str(e)

    try:
        author = scholarly.fill(author)
    except Exception as e:
        logging.error(f"Error fetching detailed author data: {e}")
        return None, [], 0, str(e)

    publications = [sanitize_publication_data(pub) for pub in author.get('publications', [])]
    total_publications = len(publications)
    author_info = extract_author_info(author, total_publications)

    set_firestore_cache("author", author_id, {'author_info': author_info, 'publications': publications})
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

def sanitize_publication_data(pub):
    try:
        citedby = int(pub.get("num_citations", 0))
        pub["citedby"] = citedby

        # Timestamps for last update
        current_time = datetime.utcnow()
        timestamp = int(current_time.timestamp())
        date_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
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


def get_similar_authors(author_name):
    # Check cache first
    cached_data = get_firestore_cache("queries", author_name)
    if cached_data:
        logging.info(f"Cache hit for similar authors of '{author_name}'. Data fetched from Firestore.")
        return cached_data

    authors = []
    try:
        search_query = scholarly.search_author(author_name)
        for _ in range(10):  # Limit to 10 authors for simplicity
            try:
                author = next(search_query)
                if author:
                    authors.append(author)
            except StopIteration:
                break
    except Exception as e:
        logging.error(f"Error fetching similar authors for '{author_name}': {e}")
        return []

    # Process authors
    clean_authors = [{
        "name": author.get("name", ""),
        "affiliation": author.get("affiliation", ""),
        "email": author.get("email", ""),
        "citedby": author.get("citedby", 0),
        "scholar_id": author.get("scholar_id", "")
    } for author in authors]

    # Cache the results
    set_firestore_cache("queries", author_name, clean_authors)

    return clean_authors
