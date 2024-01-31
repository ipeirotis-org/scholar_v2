import logging
from scholarly import scholarly
from datetime import datetime
import pytz
import json
from data_access import get_firestore_cache, set_firestore_cache


def convert_integers_to_strings(data):
    if isinstance(data, dict):
        return {key: convert_integers_to_strings(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_integers_to_strings(element) for element in data]
    elif isinstance(data, int):
        if abs(data) > 2**62:
            return str(data)
        else:
            return data
    else:
        return data


def get_author(author_id):
    cached_data = get_firestore_cache("scholar_raw_author", author_id)
    if cached_data:
        logging.info(f"Cache hit for raw scholar data for author: '{author_id}'.")
        return cached_data

    try:
        author = scholarly.search_author_id(author_id)
    except Exception as e:
        logging.error(f"Error fetching raw author data: {e}")
        return None

    try:
        logging.info(f"Filling author entry for {author_id}")
        author = scholarly.fill(author)
        serialized = convert_integers_to_strings(json.loads(json.dumps(author)))
        set_firestore_cache("scholar_raw_author", author_id, serialized)
        logging.info(f"Saved raw filled scholar data for {author_id}")
    except Exception as e:
        logging.error(f"Error fetching detailed author data: {e}")
        return None

    return serialized


def get_publication(author_id, author_pub_id):
    cached_data = get_firestore_cache("scholar_raw_pub", author_pub_id)
    if cached_data:
        logging.info(
            f"Cache hit for raw scholar data for publication: '{author_pub_id}'."
        )
        return cached_data

    cached_author = get_firestore_cache("scholar_raw_author", author_id)
    if not cached_author:
        author = get_author(author_id)
        # cached_author = get_firestore_cache("scholar_raw_author", author_id)

    pubs = author.get("publications")

    for pub in pubs:
        if pub["author_pub_id"] == author_pub_id:
            pub = scholarly.fill(pub)
            serialized = convert_integers_to_strings(json.loads(json.dumps(pub)))
            set_firestore_cache("scholar_raw_pub", pub["author_pub_id"], serialized)
            return serialized

    return None


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

        # TODO: Remove this once the rest of the code is clean
        pub['citations'] = citations 
        pub['year'] = year
        return pub

    except Exception as e:
        logging.error(f"Error sanitizing publication data: {e}")
        return None  # Return None if there's an error


def get_scholar_data(author_id):
    cached_data = get_firestore_cache("scholar_raw_author", author_id)
    if cached_data:
        logging.info(f"Cache hit for raw scholar data for author: '{author_id}'.")
        author = cached_data
    else:
        try:
            author = scholarly.search_author_id(author_id)
        except Exception as e:
            logging.error(f"Error fetching author data: {e}")
            return None, [], 0, str(e)

        try:
            logging.info(f"Filling author entry for {author_id}")
            author = scholarly.fill(author)
            serialized = convert_integers_to_strings(json.loads(json.dumps(author)))
            set_firestore_cache("scholar_raw_author", author_id, serialized)
            logging.info(f"Saved raw filled scholar data for {author_id}")
        except Exception as e:
            logging.error(f"Error fetching detailed author data: {e}")
            return None, [], 0, str(e)

    publications = []
    for pub in author.get("publications", []):
        spub = sanitize_publication_data(pub)
        if spub:
            publications.append(spub)

    total_publications = len(publications)
    author_info = extract_author_info(author, total_publications)

    return author_info, publications, total_publications, None


def extract_author_info(author, total_publications):
    return {
        "name": author.get("name", "Unknown"),
        "affiliation": author.get("affiliation", "Unknown"),
        "scholar_id": author.get("scholar_id", "Unknown"),
        "citedby": int(author.get("citedby", 0)),
        "total_publications": total_publications,
        "homepage": author.get("homepage", "Unknown"),
        "hindex": int(author.get("hindex", 0)),
    }


def sanitize_publication_data(pub):
    try:
        citations = int(pub.get("num_citations", 0))
        if not citations:
            return None

        pub_year = pub["bib"].get("pub_year")
        if not pub_year:
            return None

        year = int(pub_year)
        if year < 1950:
            return None

        now = datetime.now(pytz.utc)
        age = now.year - year + 1

        if "bib" in pub:
            title = pub["bib"].get("title")
        else:
            return None

        return {
            "citations": citations,
            "age": age,
            "title": title,
            "year": year,
        }

    except Exception as e:
        logging.error(f"Error sanitizing publication data: {e}")
        return None  # Return None if there's an error


def get_similar_authors(author_name):
    # Check cache first
    cached_data = get_firestore_cache("queries", author_name)
    if cached_data:
        logging.info(
            f"Cache hit for similar authors of '{author_name}'. Data fetched from Firestore."
        )
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
    clean_authors = [
        {
            "name": author.get("name", ""),
            "affiliation": author.get("affiliation", ""),
            "email": author.get("email", ""),
            "citedby": author.get("citedby", 0),
            "scholar_id": author.get("scholar_id", ""),
        }
        for author in authors
    ]

    # Cache the results
    set_firestore_cache("queries", author_name, clean_authors)

    return clean_authors
