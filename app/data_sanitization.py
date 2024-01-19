import logging
import pandas as pd

def sanitize_author_data(author):
    """
    Sanitizes the author data dictionary.
    """
    sanitized_author = {
        'name': author.get('name', 'Unknown'),
        'affiliation': author.get('affiliation', 'Unknown'),
        'scholar_id': author.get('scholar_id', 'Unknown'),
        'citedby': author.get('citedby', 0),
        'total_publications': len(author.get('publications', []))
    }
    return sanitized_author

def sanitize_publication_data(publications):
    """
    Sanitizes the list of publication data dictionaries.
    """
    sanitized_pubs = []
    for pub in publications:
        try:
            citedby = int(pub.get('num_citations', 0))
            sanitized_pub = {
                'title': pub['bib'].get('title', 'No Title'),
                'year': pub['bib'].get('pub_year'),
                'citedby': citedby
            }
            sanitized_pubs.append(sanitized_pub)
        except Exception as e:
            logging.error(f"Error sanitizing publication data: {e}")
    return sanitized_pubs

def create_publications_dataframe(publications):
    """
    Converts the list of sanitized publication data into a pandas DataFrame.
    """
    return pd.DataFrame(sanitize_publication_data(publications))
