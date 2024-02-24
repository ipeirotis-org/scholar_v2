import functions_framework
import json
from scholarly import scholarly
from flask import make_response
import logging
from datetime import datetime

from google.cloud import tasks_v2

from shared.config import Config

from shared.services.firestore_service import FirestoreService
firestore_service = FirestoreService()


logging.basicConfig(level=logging.INFO)

client = tasks_v2.CloudTasksClient()


# Google Cloud project ID and queue location
project = "scholar-version2"
location = "northamerica-northeast1"


# Construct the fully qualified queue name
pubs_queue = client.queue_path(project, location, "process-pubs")



@functions_framework.http
def search_author_id(request):
    """HTTP Cloud Function.
    Args:
       request (flask.Request): The request object.
    Returns:
       The response text, or any set of values that can be turned into a
       Response object using `make_response`.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    scholar_id = request_json.get("scholar_id", request_args.get("scholar_id"))

    if not scholar_id:
        return "Missing author id", 400

    author = get_author(scholar_id)
    if author is None:
        return "Error getting data from Google Scholar", 500

    response = make_response(author)
    response.headers["Content-Type"] = "application/json"

    return response, 200


def convert_integers_to_strings(data):
    if isinstance(data, dict):
        return {key: convert_integers_to_strings(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_integers_to_strings(element) for element in data]
    elif isinstance(data, int):
        if abs(data) > 2**62:
            return str(data)
        else:
            return data
    else:
        return data


def get_author(author_id):
    try:
        logging.info(f"Fetching author entry for {author_id}")
        author = scholarly.search_author_id(author_id)
        author = scholarly.fill(author)

    except Exception as e:
        logging.error(f"Error fetching detailed author data for {author_id}: {e}")
        return None

    try:
        logging.info(f"Putting publications in queue for author {author_id}")
        for pub in author["publications"]:
            url = "https://northamerica-northeast1-scholar-version2.cloudfunctions.net/fill_publication"
            task_id = pub['author_pub_id'].replace(":", "__")
            task_name = f"projects/{project}/locations/{location}/queues/process-pubs/tasks/{task_id}"
    
    
            task = {
                "name": task_name,
                "http_request": {
                    "http_method": tasks_v2.HttpMethod.POST,
                    "url": url,
                    "headers": {"Content-type": "application/json"},
                    "body": json.dumps(
                        {"pub": pub}
                    ).encode(),  # Correctly serialize the dictionary
                }
            }
            response = client.create_task(request={"parent": pubs_queue, "task": task})
    except  Exception as e:
        logging.error(
            f"Error enqueueing publications for author {author_id} in Firebase: {e}"
        )
        return None

    try:
        # Keep only the IDs and num_citations of the publications, to save space
        abbrv = []
        for pub in author["publications"]:
            if not pub.get("author_pub_id"):
                continue

            entry = {
                "author_pub_id": pub.get("author_pub_id"),
                "num_citations": pub.get("num_citations", 0),
                "filled": False,
                "bib": dict(),
            }
            if pub.get("bib") and "pub_year" in pub.get("bib"):
                entry["bib"]["pub_year"] = pub["bib"]["pub_year"]

            abbrv.append(entry)

        author["publications"] = abbrv

    except  Exception as e:
        logging.error(f"Error bookkeeping pub entries for author {author_id}: {e}")
        return None

    try:
        logging.info(f"Serializing author {author_id}")
        serialized = convert_integers_to_strings(json.loads(json.dumps(author)))

        # We leave the co-authors as-is, unless they do not fit in Firestore.
        if len(json.dumps(serialized)) > 500000:
            del author["coauthors"]

        # In extreme cases, we will truncate publications
        while True:
            serialized = convert_integers_to_strings(json.loads(json.dumps(author)))
            # This is a hack to deal with the fact that cache can only hold 0.5Mb docs
            if len(json.dumps(serialized)) > 500000:
                full = len(author["publications"])
                half = int(full / 2)
                author["publications"] = author["publications"][:half]
            else:
                break
    except  Exception as e:
        logging.error(f"Error serializing {author_id} : {e}")
        return None

    try:
        logging.info(f"Storing author {author_id} in Firebase")
        firestore_service.set_firestore_cache("scholar_raw_author", author_id, serialized)

    except  Exception as e:
        logging.error(f"Error storing author entry {author_id} in Firebase: {e}")
        return None

    return serialized

