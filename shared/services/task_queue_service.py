# shared/services/task_queue_service.py

import json
import logging
from google.cloud import tasks_v2
# Import the exception for handling existing tasks
from google.api_core.exceptions import AlreadyExists 
from ..config import Config


class TaskQueueService:
    def __init__(self):
        self.tasks_client = tasks_v2.CloudTasksClient()
        self.project_id = Config.PROJECT_ID
        self.queue_location = Config.QUEUE_LOCATION
        self.authors_queue_name = Config.QUEUE_NAME_AUTHORS
        self.pubs_queue_name = Config.QUEUE_NAME_PUBS
        # Construct full queue paths
        self.authors_queue = self.tasks_client.queue_path(
            self.project_id, self.queue_location, self.authors_queue_name
        )
        self.pubs_queue = self.tasks_client.queue_path(
            self.project_id, self.queue_location, self.pubs_queue_name
        )

    def enqueue_author_task(self, author_id):
        """Enqueues a task to process an author, handling duplicates."""
        # Construct the full task name for idempotency
        task_name = self.tasks_client.task_path(
            self.project_id, self.queue_location, self.authors_queue_name, author_id
        )
        url = Config.API_SEARCH_AUTHOR_ID
        payload = json.dumps({"scholar_id": author_id})

        task = self._create_http_task(task_name, url, payload)
        return self._enqueue_task(task, self.authors_queue, f"author {author_id}")

    def enqueue_publication_task(self, pub_entry):
        """Enqueues a task to process a publication, handling duplicates."""
        # Sanitize pub_id for use as task ID component
        task_id_part = pub_entry["author_pub_id"].replace(":", "__").replace("/", "___") 
        # Construct the full task name for idempotency
        task_name = self.tasks_client.task_path(
            self.project_id, self.queue_location, self.pubs_queue_name, task_id_part
        )
        url = Config.API_FILL_PUBLICATION
        payload = json.dumps({"pub": pub_entry})

        task = self._create_http_task(task_name, url, payload)
        return self._enqueue_task(task, self.pubs_queue, f"publication {task_id_part}")

    def check_pending_tasks(self, author_id):
        """
        Checks if processing might be pending for an author.

        Note: This method no longer checks the Cloud Tasks queue directly due to
        inefficiency. It relies on the application logic (e.g., checking data
        presence/status in Firestore) to determine if processing is needed.
        Returns False as a placeholder, assuming the primary check occurs elsewhere.
        Consider implementing a status field in Firestore for a more robust check.
        """
        logging.warning(f"check_pending_tasks for {author_id} no longer queries the task queue directly. Check application state instead.")
        # Placeholder: In the current app design, main.py checks data availability.
        # If data is missing, it assumes processing is needed/pending.
        return False

    def get_number_of_tasks_in_queue(self):
        """
        Returns the number of tasks currently in the queues.

        Note: Cloud Tasks API does not provide an efficient way to get the queue size.
        This method returns None to indicate the count is unavailable.
        The frontend should be updated to handle this (e.g., show a generic message).
        """
        logging.warning("get_number_of_tasks_in_queue cannot provide an accurate count efficiently. Returning None.")
        # Cannot be efficiently determined via API.
        return None

    # Removed _check_duplicate_task method

    def _create_http_task(self, task_name, url, payload):
        """Creates the task configuration dictionary."""
        return {
            "name": task_name,
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": url,
                "headers": {"Content-type": "application/json"},
                "body": payload.encode(),
                # Consider adding OIDC token for authentication if functions are secured
                # "oidc_token": {
                #     "service_account_email": "YOUR_SERVICE_ACCOUNT_EMAIL"
                # }
            },
        }

    def _enqueue_task(self, task, queue, task_description):
        """Attempts to enqueue a task, handling AlreadyExists exceptions."""
        try:
            response = self.tasks_client.create_task(
                request={"parent": queue, "task": task}
            )
            logging.info(f"Task enqueued for {task_description}: {response.name}")
            return response
        except AlreadyExists:
            logging.info(f"Task for {task_description} already exists: {task.get('name')}")
            # Task already exists, treat as success (or neutral) in terms of queueing
            return None 
        except Exception as e:
            logging.error(f"Error enqueuing task for {task_description} ({task.get('name')}): {e}")
            # Failed to enqueue for other reasons
            return None