import logging
from scholarly import scholarly, ProxyGenerator
from datetime import datetime
import pytz
import json
import requests

from google.cloud import secretmanager

from data_access import get_firestore_cache, set_firestore_cache


def access_secret_version(project_id, secret_id, version_id = "latest"):
    """
    Access the payload of the given secret version and return it.

    Args:
        project_id (str): Google Cloud project ID.
        secret_id (str): ID of the secret to access.
        version_id (str): ID of the version to access.
    Returns:
        str: The secret version's payload, or None if
        the version does not exist.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

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
        # author = scholarly.search_author_id(author_id)
        # author = scholarly.fill(author)
        # serialized = convert_integers_to_strings(json.loads(json.dumps(author)))
        
        # The URL to the API endpoint
        url = 'https://us-central1-scholar-version2.cloudfunctions.net/search_author_id'
        data = {'scholar_id': author_id}
        response = requests.post(url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
        serialized = convert_integers_to_strings(response.json())
        
        set_firestore_cache("scholar_raw_author", author_id, serialized)
        logging.info(f"Saved raw filled scholar data for {author_id}")

        return serialized
    except Exception as e:
        logging.error(f"Error fetching detailed author data: {e}")
        return None

    


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

    '''
    ScraperAPI_key = access_secret_version("scholar-version2", "ScraperAPI_key")
    pg = ProxyGenerator()
    pg.ScraperAPI(ScraperAPI_key, country_code='us', premium=True)
    scholarly.use_proxy(pg)
    '''

    
    for pub in pubs:
        if pub["author_pub_id"] == author_pub_id:
            pub = scholarly.fill(pub)
            serialized = convert_integers_to_strings(json.loads(json.dumps(pub)))
            set_firestore_cache("scholar_raw_pub", pub["author_pub_id"], serialized)
            return serialized

    return None


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
