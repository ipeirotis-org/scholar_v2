import logging
from scholarly import scholarly
from datetime import datetime
import pytz
import json
import requests
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from data_access import get_firestore_cache, set_firestore_cache

from config import Config


def get_author(author_id):
    cached_author, timestamp = get_firestore_cache(Config.FIRESTORE_COLLECTION_AUTHOR, author_id)
    if cached_author:
        return cached_author
    else:
        return None


def get_author_publications(author_id):
    db = firestore.Client()
    pubs_db = db.collection(Config.FIRESTORE_COLLECTION_PUB)

    author_pubs = (
        pubs_db
        .where(filter=FieldFilter("data.author_pub_id", ">=", author_id))
        .where(filter=FieldFilter("data.author_pub_id", "<", author_id + "ζ"))
    )

    publications = []
    for doc in author_pubs.stream(timeout=3600):
        pub = doc.to_dict().get("data", {})
        if pub:
            publications.append(pub)

    return publications


def get_author_last_modification(author_id):
    db = firestore.Client()
    pubs_db = db.collection(Config.FIRESTORE_COLLECTION_PUB)

    author_pubs = (
        pubs_db
        .where(filter=FieldFilter("data.author_pub_id", ">=", author_id))
        .where(filter=FieldFilter("data.author_pub_id", "<", author_id + "ζ"))
        .select(["timestamp"])
    )

    latest_pub_change = max([r.to_dict()['timestamp'] for r in author_pubs.get()])

    _, latest_author_change = get_firestore_cache("scholar_raw_author", author_id)
    
    return max(latest_pub_change, latest_author_change)


def get_publication(author_pub_id):
    cached_pub, timestamp = get_firestore_cache(Config.FIRESTORE_COLLECTION_PUB, author_pub_id)
    if cached_pub:
        return cached_pub
    else:
        return None


def get_similar_authors(author_name):
    # Check cache first
    cached_data, timestamp = get_firestore_cache("queries", author_name)
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
