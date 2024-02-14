import json
import logging

import pandas as pd
from google.cloud import firestore
from google.cloud import tasks_v2
from google.cloud import bigquery

# Google Cloud project ID and queue location
project = "scholar-version2"
location = "northamerica-northeast1"
logging.basicConfig(level=logging.INFO)
db = firestore.Client()
tasks = tasks_v2.CloudTasksClient()
bq = bigquery.Client()

# Construct the fully qualified queue name
authors_queue = tasks.queue_path(project, location, "process-authors")
pubs_queue = tasks.queue_path(project, location, "process-pubs")

collection_ref = db.collection("scholar_raw_author")


def put_author_in_queue(author_id):
    """
    This function places a request to fetch a new copy of the author from Google Scholar
    and then stores the copy in the database. The request will not check if the author is in
    the database or not. The logic of checking the db should be elsewhere. Note that when we
    fetch a new copy of the author, we also fetch
    """

    task_name = f"projects/{project}/locations/{location}/queues/process-authors/tasks/{author_id}"

    existing_tasks = tasks.list_tasks(request={"parent": authors_queue})
    for task in existing_tasks:
        if task_name == task.name:
            # If a duplicate task is found, return without enqueueing
            return

    url = "https://us-east5-scholar-version2.cloudfunctions.net/search_author_id"

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
    except Exception as e:
        logging.error(f"Could not create task {author_id}")


def put_pub_in_queue(pub_entry):
    """
    This function places a request to fetch a new copy of the author from Google Scholar
    and then stores the copy in the database. The request will not check if the author is in
    the database or not. The logic of checking the db should be elsewhere. Note that when we
    fetch a new copy of the author, we also fetch
    """

    task_id = pub_entry["author_pub_id"].replace(":", "__")
    task_name = (
        f"projects/{project}/locations/{location}/queues/process-pubs/tasks/{task_id}"
    )

    url = "https://us-east5-scholar-version2.cloudfunctions.net/fill_publication"
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
    except Exception as e:
        logging.error(f"Could not create task {task_id}")


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
          pubs_in_pubsdb = 0
        LIMIT 1000
    """

    query_job = bq.query(QUERY)
    results = query_job.result()  # Waits for the query to finish
    df = results.to_dataframe().sample(n=num_authors)
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
            total_authors += 1
            put_author_in_queue(scholar_id)
            entry = {
                "author_id": scholar_id,
            }
            authors.append(entry)
            continue

        author = doc.to_dict().get("data", None)
        if not author:
            total_authors += 1
            put_author_in_queue(scholar_id)
            entry = {
                "doc_id": doc.id,
                "author_id": scholar_id,
            }
            authors.append(entry)
            continue

        author_id = author.get("scholar_id")
        put_author_in_queue(author_id)

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
