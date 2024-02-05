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


def refresh_authors(num_authors=1):
    query = collection_ref.order_by("timestamp", direction=firestore.Query.DESCENDING).limit(num_authors)

    total_authors = 0
    total_pubs = 0
    authors = []
    for doc in query.stream():
        author = doc.to_dict().get("data", None)
        if not author:
            continue

        total_authors += 1

        author_id = author.get("scholar_id")
        publications = author["publications"]

        entry = {
            "author_id": author_id,
            "publications": len(publications),
            "name": author.get("name"),
        }
        authors.append(entry)

        # print(f'{author_id}: {len(publications)} publications')
        url = f"https://scholar.ipeirotis.org/api/author/{author_id}?no_cache=true"
        # Construct the request body
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.GET,
                "url": url,
            }
        }
        # Add the task to the queue
        response = client.create_task(request={"parent": authors_queue, "task": task})

        for pub in publications:
            total_pubs += 1

            pub_id = pub["author_pub_id"]
            url = f"https://scholar.ipeirotis.org/api/author/{author_id}/publication/{pub_id}"
            # Construct the request body
            task = {
                "http_request": {
                    "http_method": tasks_v2.HttpMethod.GET,
                    "url": url,
                    #'headers': {'Content-type': 'application/json'},
                    #'body': f'{{"pub": "{pub}"}}'.encode()
                }
            }
            # Add the task to the queue
            response = client.create_task(request={"parent": pubs_queue, "task": task})

    return {
        "total_authors": total_authors,
        "total_publications": total_pubs,
        "authors": authors,
    }
