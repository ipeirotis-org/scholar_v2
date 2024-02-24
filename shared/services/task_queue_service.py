import json
import logging
from google.cloud import tasks_v2
from ..config import Config 

class TaskQueueService:
    def __init__(self):
        self.tasks_client = tasks_v2.CloudTasksClient()
        self.project_id = Config.PROJECT_ID
        self.queue_location = Config.QUEUE_LOCATION
        self.authors_queue_name = Config.QUEUE_NAME_AUTHORS
        self.pubs_queue_name = Config.QUEUE_NAME_PUBS
        self.authors_queue = self.tasks_client.queue_path(self.project_id, self.queue_location, self.authors_queue_name)
        self.pubs_queue = self.tasks_client.queue_path(self.project_id, self.queue_location, self.pubs_queue_name)

    def enqueue_author_task(self, author_id):
        task_name = f"{self.authors_queue}/tasks/{author_id}"
        url = Config.API_SEARCH_AUTHOR_ID
        payload = json.dumps({"scholar_id": author_id})

        if self._check_duplicate_task(task_name, self.authors_queue):
            logging.info(f"Task for author_id {author_id} already enqueued.")
            return None

        task = self._create_http_task(task_name, url, payload)
        return self._enqueue_task(task, self.authors_queue)

    def enqueue_publication_task(self, pub_entry):
        task_id = pub_entry["author_pub_id"].replace(":", "__")
        task_name = f"{self.pubs_queue}/tasks/{task_id}"
        url = Config.API_FILL_PUBLICATION
        payload = json.dumps({"pub": pub_entry})

        if self._check_duplicate_task(task_name, self.pubs_queue):
            logging.info(f"Task for publication {task_id} already enqueued.")
            return None

        task = self._create_http_task(task_name, url, payload)
        return self._enqueue_task(task, self.pubs_queue)

    def _check_duplicate_task(self, task_name, queue):
        existing_tasks = self.tasks_client.list_tasks(request={"parent": queue})
        for task in existing_tasks:
            if task_name == task.name:
                return True
        return False

    def _create_http_task(self, task_name, url, payload):
        return {
            "name": task_name,
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": url,
                "headers": {"Content-type": "application/json"},
                "body": payload.encode(),
            },
        }

    def _enqueue_task(self, task, queue):
        try:
            response = self.tasks_client.create_task(request={"parent": queue, "task": task})
            logging.info(f"Task enqueued: {response.name}")
            return response
        except Exception as e:
            logging.error(f"Error enqueuing task: {e}")
            return None
