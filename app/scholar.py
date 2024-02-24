import logging
from scholarly import scholarly
from shared.services.firestore_service import FirestoreService
from shared.repositories.author_repository import AuthorRepository
from shared.repositories.publication_repository import PublicationRepository
from shared.config import Config

# Setup logging
logging.basicConfig(level=logging.INFO)

# Initialize services and repositories
firestore_service = FirestoreService()
publication_repository = PublicationRepository(firestore_service)
author_repository = AuthorRepository(firestore_service, publication_repository)


def get_author(author_id):
    # Use the AuthorRepository to fetch author details
    return author_repository.get_author(author_id)

def get_author_publications(author_id):
    # Use the PublicationRepository to fetch publications by author ID
    return publication_repository.get_publications_by_author(author_id)

def get_author_last_modification(author_id):
    # This function now simply delegates the call to the AuthorRepository.
    return author_repository.get_author_last_modification(author_id)

def get_publication(author_pub_id):
    # Use the PublicationRepository to fetch a specific publication
    return publication_repository.get_publication(author_pub_id)

def get_similar_authors(author_name):
    # Fetch similar authors with caching logic
    cached_data, _ = firestore_service.get_firestore_cache("queries", author_name)
    if cached_data:
        logging.info(f"Cache hit for similar authors of '{author_name}'.")
        return cached_data

    authors = fetch_authors_from_scholarly(author_name)
    if authors:
        # Cache the fetched authors data
        firestore_service.set_firestore_cache("queries", author_name, authors)
    return authors

def fetch_authors_from_scholarly(author_name):
    # Fetch authors using the scholarly package
    authors = []
    try:
        search_query = scholarly.search_author(author_name)
        for _ in range(10):  # Limit to 10 authors for simplicity
            try:
                author = next(search_query)
                if author:
                    authors.append(process_author(author))
            except StopIteration:
                break
    except Exception as e:
        logging.error(f"Error fetching similar authors for '{author_name}': {e}")
    return authors

def process_author(author):
    # Process and return author data in a structured format
    return {
        "name": author.get("name", ""),
        "affiliation": author.get("affiliation", ""),
        "email": author.get("email", ""),
        "citedby": author.get("citedby", 0),
        "scholar_id": author.get("scholar_id", ""),
    }
