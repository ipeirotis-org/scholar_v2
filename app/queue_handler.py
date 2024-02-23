import json
import logging

import pandas as pd
from google.cloud import firestore
from google.cloud import tasks_v2
from google.cloud import bigquery

from config import Config

logging.basicConfig(level=logging.INFO)
db = firestore.Client()
tasks = tasks_v2.CloudTasksClient()
bq = bigquery.Client()

# Construct the fully qualified queue name
authors_queue = tasks.queue_path(Config.PROJECT_ID, Config.QUEUE_LOCATION, Config.QUEUE_NAME_AUTHORS)
pubs_queue = tasks.queue_path(Config.PROJECT_ID, Config.QUEUE_LOCATION, Config.QUEUE_NAME_PUBS)

collection_ref = db.collection(Config.FIRESTORE_COLLECTION_AUTHOR)


def put_author_in_queue(author_id):
    """
    This function places a request to fetch a new copy of the author from Google Scholar
    and then stores the copy in the database. The request will not check if the author is in
    the database or not. The logic of checking the db should be elsewhere. Note that when we
    fetch a new copy of the author, we also fetch
    """

    task_name = f"{Config.QUEUE_PATH_AUTHORS}/tasks/{author_id}"

    existing_tasks = tasks.list_tasks(request={"parent": authors_queue})
    for task in existing_tasks:
        if task_name == task.name:
            # If a duplicate task is found, return without enqueueing
            return

    url = Config.API_SEARCH_AUTHOR_ID

    payload = json.dumps({"scholar_id": author_id})
    task = {
        "name": task_name,
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-type": "application/json"},
            "body": payload.encode(),
        },
    }
    # Add the task to the queue
    try:
        response = tasks.create_task(request={"parent": authors_queue, "task": task})
        return response
    except Exception as e:
        logging.error(f"Could not create task {author_id}")
        return None


def put_pub_in_queue(pub_entry):
    """
    This function places a request to fetch a new copy of the author from Google Scholar
    and then stores the copy in the database. The request will not check if the author is in
    the database or not. The logic of checking the db should be elsewhere. Note that when we
    fetch a new copy of the author, we also fetch
    """

    task_id = pub_entry["author_pub_id"].replace(":", "__")
    task_name = f"{Config.QUEUE_PATH_PUBS}/tasks/{task_id}"

    url = Config.API_FILL_PUBLICATION
    task = {
        "name": task_name,
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-type": "application/json"},
            "body": json.dumps(
                {"pub": pub_entry}
            ).encode(),  # Correctly serialize the dictionary
        },
    }
    try:
        response = tasks.create_task(request={"parent": pubs_queue, "task": task})
        return response
    except Exception as e:
        logging.error(f"Could not create task {task_id}")
        return None


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


def get_authors_to_fix(num_authors=10):
    QUERY = f"""
        SELECT
          *
        FROM
          `scholar-version2.firestore_views.debug_authors_missing_pubs_in_db`
        WHERE
          err > 0.05 AND author_last_updated < '2024-02-16'
        ORDER BY
          err DESC
    """

    query_job = bq.query(QUERY)
    results = query_job.result()  # Waits for the query to finish
    df = results.to_dataframe()
    if df.shape[0] > num_authors:
        df = df.sample(n=num_authors)
    if df.shape[0] == 0:
        return []
    refresh = df.scholar_id.values
    return [r for r in refresh]


def refresh_authors(refresh=[], num_authors=1):
    if len(refresh) == 0:
        refresh = get_authors_to_fix(num_authors)

    total_authors = 0
    total_pubs = 0
    authors = []

    for scholar_id in refresh:
        doc = collection_ref.document(scholar_id).get()
        author = doc.to_dict()
        if not author:
            
            resp = put_author_in_queue(scholar_id)
            if not resp: continue

            total_authors += 1
            entry = {
                "author_id": scholar_id,
            }
            authors.append(entry)
            continue

        author = doc.to_dict().get("data", None)
        if not author:
            
            resp = put_author_in_queue(scholar_id)
            if not resp: continue

            total_authors += 1
            entry = {
                "doc_id": doc.id,
                "author_id": scholar_id,
            }
            authors.append(entry)
            continue

        author_id = author.get("scholar_id")
        resp = put_author_in_queue(scholar_id)
        if not resp: continue
        
        publications = author.get("publications", [])

        total_authors += 1
        total_pubs += len(publications)

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
