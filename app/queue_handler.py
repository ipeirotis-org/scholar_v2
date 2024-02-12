import json

import pandas as pd
from google.cloud import firestore
from google.cloud import tasks_v2

# Google Cloud project ID and queue location
project = "scholar-version2"
location = "northamerica-northeast1"

db = firestore.Client()
client = tasks_v2.CloudTasksClient()

# Construct the fully qualified queue name
authors_queue = client.queue_path(project, location, "process-authors")
pubs_queue = client.queue_path(project, location, "process-pubs")

collection_ref = db.collection("scholar_raw_author")


def put_author_in_queue(author_id):
    '''
    This function places a request to fetch a new copy of the author from Google Scholar
    and then stores the copy in the database. The request will not check if the author is in
    the database or not. The logic of checking the db should be elsewhere. Note that when we 
    fetch a new copy of the author, we also fetch 
    '''

    url = "https://us-east5-scholar-version2.cloudfunctions.net/search_author_id"
    # Construct the request body
    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            'headers': {'Content-type': 'application/json'},
            'body': json.dumps(
                {"scholar_id": author_id}
            ).encode(),
        }
    }
    # Add the task to the queue
    response = client.create_task(request={"parent": pubs_queue, "task": task})


def put_pub_in_queue(pub_entry):
    '''
    This function places a request to fetch a new copy of the author from Google Scholar
    and then stores the copy in the database. The request will not check if the author is in
    the database or not. The logic of checking the db should be elsewhere. Note that when we 
    fetch a new copy of the author, we also fetch 
    '''

    url = "https://us-east5-scholar-version2.cloudfunctions.net/fill_publication"
    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-type": "application/json"},
            "body": json.dumps(
                {"pub": pub_entry}
            ).encode(),  # Correctly serialize the dictionary
        }
    }
    response = client.create_task(request={"parent": pubs_queue, "task": task})

def get_authors_to_refresh(num_authors=1):

    query = collection_ref.order_by(
        "timestamp", direction=firestore.Query.ASCENDING
    ).limit(num_authors)

    total_authors = 0
    author_ids = []
    for doc in query.stream():
        author = doc.to_dict().get("data", None)
        if not author:
            continue

        author_id = author.get("scholar_id")
        if author_id != doc.id:
            collection_ref.document(doc.id).delete()
            continue

        author_ids.append(author_id)

        if len(author_ids) >= num_authors:
            break

    return author_ids


def refresh_authors(num_authors=1):

    total_authors = 0
    total_pubs = 0
    refresh = get_authors_to_refresh(num_authors)
    authors = []
    
    for scholar_id in refresh:


        doc = collection_ref.document(scholar_id)
        author = doc.get().to_dict().get("data", None)
        if not author:
            continue

        total_authors += 1

        author_id = author.get("scholar_id")
        put_author_in_queue(author_id)
        
        publications = author.get("publications", [])
        for pub in publications:
            put_pub_in_queue(pub)

        entry = {
            "doc_id": doc.id,
            "author_id": author_id,
            "publications": len(publications),
            "name": author.get("name"),
        }
        authors.append(entry)

        if author_id != doc.id:
            collection_ref.document(doc.id).delete()

    return {
        "total_authors": total_authors,
        "total_publications": total_pubs,
        "authors": authors,
    }
