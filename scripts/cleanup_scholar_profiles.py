#!/usr/bin/env python3
"""
cleanup_gs_with_serpapi.py

Fetch Firestore docs older than N days, query SerpAPI’s Google Scholar Author API,
and delete any docs whose profiles can’t be fetched (e.g., missing author data).
"""

import os
import argparse
import logging
from datetime import datetime, timedelta

import pytz
from serpapi import GoogleSearch  # pip install serpapi :contentReference[oaicite:0]{index=0}
from google.cloud import firestore

# Logging setup
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)

def get_stale_docs(client, collection, days, limit, key_attr):
    cutoff = datetime.now(pytz.utc) - timedelta(days=days)
    query = (
        client.collection(collection)
              .where("timestamp", "<", cutoff)
              .order_by("timestamp")
              .limit(limit)
    )
    docs = []
    for doc in query.stream():
        data = doc.to_dict().get("data", {})
        author_id = data.get(key_attr)
        if author_id:
            docs.append((doc.id, author_id))
    logging.info("Found %d stale docs.", len(docs))
    return docs

def is_profile_active(author_id, api_key, no_cache=False):
    """
    Returns True if SerpAPI returns a valid 'author' dict.
    """
    params = {
        "engine": "google_scholar_author",
        "author_id": author_id,
        "api_key": api_key,
    }
    if no_cache:
        params["no_cache"] = "true"
    search = GoogleSearch(params)
    result = search.get_dict()
    status = result.get("search_metadata", {}).get("status")
    if status != "Success":
        logging.warning("SerpAPI status %s for %s", status, author_id)
        return True  # preserve on error
    author = result.get("author") or {}
    return bool(author.get("name"))  # no name → profile missing

def clean_inactive_with_serpapi(collection, days, limit, key_attr, api_key, no_cache):
    client = firestore.Client()
    stale = get_stale_docs(client, collection, days, limit, key_attr)
    for doc_id, author_id in stale:
        active = is_profile_active(author_id, api_key, no_cache)
        if not active:
            client.collection(collection).document(doc_id).delete()
            logging.info("Deleted %s (author %s).", doc_id, author_id)
        else:
            logging.info("Kept %s (author %s).", doc_id, author_id)

def main():
    p = argparse.ArgumentParser(
        description="Cleanup Firestore entries via SerpAPI Google Scholar Author."
    )
    p.add_argument("--collection", required=True)
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--key-attr", required=True)
    p.add_argument("--api-key",
                   default=os.getenv("SERPAPI_API_KEY"),
                   help="SerpAPI API key")
    p.add_argument("--no-cache", action="store_true",
                   help="Force fresh SerpAPI fetch (no cached results)")
    args = p.parse_args()

    if not args.api_key:
        raise ValueError("SerpAPI API key is required (--api-key or SERPAPI_API_KEY)")

    clean_inactive_with_serpapi(
        args.collection,
        args.days,
        args.limit,
        args.key_attr,
        args.api_key,
        args.no_cache
    )

if __name__ == "__main__":
    main()
