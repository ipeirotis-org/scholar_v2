import functions_framework
import json
import logging
from datetime import datetime
from scholarly import scholarly
from flask import make_response

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
    response.headers['Content-Type'] = 'application/json'
    
    return response, 200

@functions_framework.http
def fill_publication(request):
    """HTTP Cloud Function.
    Args:
       request (flask.Request): The request object.
    Returns:
       The response text, or any set of values that can be turned into a
       Response object using `make_response`.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    pub = request_json.get("pub", request_args.get("pub"))
    if not pub:
        return "Missing pub", 400

    pub = fill_pub(pub)
    if pub is None:
        return "Error fill pblication from Google Scholar", 500

    response = make_response(pub)
    response.headers['Content-Type'] = 'application/json'
    
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

        # We want to keep track of the last time we updated the file
        now = datetime.now()
        timestamp = int(datetime.timestamp(now))
        date_str = now.strftime("%Y-%m-%d %H:%M:%S")

        author["last_updated_ts"] = timestamp
        author["last_updated"] = date_str
        
        serialized = convert_integers_to_strings(json.loads(json.dumps(author)))

        return serialized

    except Exception as e:
        logging.error(f"Error fetching detailed author data: {e}")
        return None

def fill_pub(pub):

    try:
        logging.info(f"Fetching pub entry for publication {pub['author_pub_id']}")
        pub = scholarly.fill(pub)

        # We want to keep track of the last time we updated the file
        now = datetime.now()
        timestamp = int(datetime.timestamp(now))
        date_str = now.strftime("%Y-%m-%d %H:%M:%S")

        pub["last_updated_ts"] = timestamp
        pub["last_updated"] = date_str
        
        serialized = convert_integers_to_strings(json.loads(json.dumps(pub)))

        return serialized

    except Exception as e:
        logging.error(f"Error fetching detailed author data: {e}")
        return None

