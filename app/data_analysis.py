import logging
from shared.services.firestore_service import FirestoreService
from shared.services.bigquery_service import BigQueryService
from shared.repositories.author_repository import AuthorRepository
from shared.repositories.publication_repository import PublicationRepository
from visualization import generate_plot

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize services and repositories
firestore_service = FirestoreService()
bigquery_service = BigQueryService()
author_repository = AuthorRepository(firestore_service, PublicationRepository(firestore_service))

def get_author_stats(author_id):
    # Fetch author details
    author = author_repository.get_author(author_id)
    if not author:
        logging.info(f"No author found with ID: {author_id}")
        return None

    # Check for last modification to determine if cache needs refresh
    author_last_modified = author_repository.get_author_last_modification(author_id)
    
    # Fetch and cache author publication stats
    author_pub_stats, pub_stats_timestamp = firestore_service.get_firestore_cache("author_pub_stats", author_id)
    if not author_pub_stats or author_last_modified > pub_stats_timestamp:
        author_pub_stats = bigquery_service.get_author_pub_stats(author_id)
        if author_pub_stats:
            firestore_service.set_firestore_cache("author_pub_stats", author_id, author_pub_stats)
    
    # Append publications to author object
    if author_pub_stats:
        author["publications"] = author_pub_stats
    else:
        author["publications"] = []

    # Fetch and cache author stats
    author_stats, stats_timestamp = firestore_service.get_firestore_cache("author_stats", author_id)
    if not author_stats or author_last_modified > stats_timestamp:
        author_stats = bigquery_service.get_author_stats(author_id)
        if author_stats:
            firestore_service.set_firestore_cache("author_stats", author_id, author_stats)
    
    # Append stats to author object
    if author_stats:
        author["stats"] = author_stats
    else:
        logging.info(f"No stats found for author ID: {author_id}")
        author["stats"] = {}

    return author



def generate_plots_for_publications(publications):
    """
    Generate plots for a list of publications.
    """
    plot_paths = []
    for publication in publications:
        publication_df = pd.DataFrame([publication])
        plot_path = generate_plot(publication_df, publication.get("title", "Publication Plot"))
        plot_paths.append(plot_path)
    return plot_paths




