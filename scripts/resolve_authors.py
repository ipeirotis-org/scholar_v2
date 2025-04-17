'''
resolve_authors.py

Prerequisites:
- Python 3.7+
- Install dependencies: pip install google-cloud-firestore serpapi
- Ensure your project root contains both `scripts/resolve_authors.py` and the `app/` directory with `scholar.py` exposing `fetch_authors_from_scholarly`
- Ensure Google Cloud credentials are configured (e.g., via GOOGLE_APPLICATION_CREDENTIALS)
- Set your SerpAPI key in the environment: export SERPAPI_API_KEY=<your_key>

Usage:
    From your project root (one level above scripts and app directories):
        python3 scripts/resolve_authors.py --batch-size 500

This script auto-adjusts its import path, so you can run it without manually setting PYTHONPATH.
'''

import argparse
import logging
import os
import sys
from google.cloud import firestore
from serpapi import GoogleSearch
from google.cloud.firestore import Query

# Ensure project root is on sys.path (so `app` package is importable)
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.scholar import fetch_authors_from_scholarly

# Read SerpAPI key
SERPAPI_API_KEY = "f868d1fd7a6407c1624292adb1bd7ef16563d8541ee6c698321ff00be21f34fe"

# Firestore collection names
PUB_COLLECTION = 'scholar_raw_pub'
AUTHOR_COLLECTION = 'scholar_raw_author'

# Initialize Firestore client
db = firestore.Client()

def is_profile_active(author_id, api_key, no_cache=False):
    """
    Returns True if SerpAPI returns a valid 'author' dict.
    Preserves True on SerpAPI error.
    """
    params = {
        'engine': 'google_scholar_author',
        'author_id': author_id,
        'api_key': api_key,
    }
    if no_cache:
        params['no_cache'] = 'true'
    try:
        search = GoogleSearch(params)
        result = search.get_dict()
        status = result.get('search_metadata', {}).get('status')
        if status != 'Success':
            logging.warning('SerpAPI status %s for %s', status, author_id)
            return True
        author = result.get('author') or {}
        return bool(author.get('name'))
    except Exception as e:
        logging.error('Error checking profile active for %s: %s', author_id, e)
        return True


def resolve_author(name, resolved_ids):
    """
    Resolve a single author name to a Google Scholar ID using:
      1. Exact name match
      2. Alias match
      3. Co-author graph disambiguation
    :param name: str
    :param resolved_ids: set of scholar_ids already resolved
    :return: dict{name, scholar_id, method}
    """
    # Step 1: Exact name match
    author_ref = db.collection(AUTHOR_COLLECTION)
    matches = author_ref.where('name', '==', name).get()
    if matches:
        # Single exact
        if len(matches) == 1:
            sid = matches[0].to_dict().get('scholar_id')
            if is_profile_active(sid, SERPAPI_API_KEY):
                return {'name': name, 'scholar_id': sid, 'method': 'local_exact'}
            logging.warning('Inactive profile for %s (%s), skipping local_exact', name, sid)
        # Alias match
        alias_matches = []
        for doc in matches:
            data = doc.to_dict()
            for alias in data.get('aliases', []):
                if alias == name:
                    alias_matches.append(doc)
                    break
        if len(alias_matches) == 1:
            sid = alias_matches[0].to_dict().get('scholar_id')
            if is_profile_active(sid, SERPAPI_API_KEY):
                return {'name': name, 'scholar_id': sid, 'method': 'local_alias'}
            logging.warning('Inactive profile for %s (%s), skipping local_alias', name, sid)
        # Step 2: Co-author graph
        coauthor_matches = []
        for doc in matches:
            data = doc.to_dict()
            for co in data.get('coauthors', []):
                if co.get('scholar_id') in resolved_ids:
                    coauthor_matches.append(doc)
                    break
        if len(coauthor_matches) == 1:
            sid = coauthor_matches[0].to_dict().get('scholar_id')
            if is_profile_active(sid, SERPAPI_API_KEY):
                return {'name': name, 'scholar_id': sid, 'method': 'coauthor_graph'}
            logging.warning('Inactive profile for %s (%s), skipping coauthor_graph', name, sid)

    # Fallback: unresolved
    logging.warning(f"Unresolved author: {name}")
    return {'name': name, 'scholar_id': None, 'method': 'unresolved'}


def update_publication(doc_ref, resolutions, batch):
    batch.update(doc_ref, {'author_resolutions': resolutions})


def main():
    parser = argparse.ArgumentParser(
        description='Resolve Scholar IDs for Firestore publications.'
    )
    parser.add_argument('--batch-size', type=int, default=500,
                        help='Batch commit size')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    batch = db.batch()
    total_pubs = total_authors = resolved = unresolved = 0
    batch_count = 0

    for doc in db.collection(PUB_COLLECTION).order_by("data.url_related_articles", direction=Query.ASCENDING).stream():
        total_pubs += 1
        pub = doc.to_dict()
        pub_id = pub.get('data', {}).get('author_pub_id', {})
        authors = pub.get('data', {}).get('bib', {}).get('author', '')
        names = [a.strip() for a in authors.split(' and ') if a.strip()]
        print(f"{pub_id} ==> [{authors}]")

        resolutions = []
        resolved_ids = set()
        for name in names:
            total_authors += 1
            res = resolve_author(name, resolved_ids)
            resolutions.append(res)
            if res['scholar_id']:
                resolved += 1
                resolved_ids.add(res['scholar_id'])
            else:
                unresolved += 1

        update_publication(doc.reference, resolutions, batch)
        batch_count += 1

        if batch_count >= args.batch_size:
            batch.commit()
            logging.info(f"Committed {batch_count} updates")
            batch = db.batch()
            batch_count = 0

    if batch_count > 0:
        batch.commit()
        logging.info(f"Committed final {batch_count} updates")

    pct = (resolved / total_authors * 100) if total_authors else 0
    print(f"Processed {total_pubs} pubs, {total_authors} authors")
    print(f"Resolved: {resolved} ({pct:.1f}%), Unresolved: {unresolved}")


if __name__ == '__main__':
    main()
