import logging
from shared.services.task_queue_service import TaskQueueService

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize services
task_queue_service = TaskQueueService()


def put_author_in_queue(author_id):
    """
    Enqueue a task to fetch a new copy of the author from Google Scholar
    and store it in the database.
    """
    response = task_queue_service.enqueue_author_task(author_id)
    if response is None:
        logging.error(f"Could not create task for author ID: {author_id}")
    return response


def pending_tasks(author_id):
    return task_queue_service.check_pending_tasks(author_id)


def number_of_tasks_in_queue():
    return task_queue_service.get_number_of_tasks_in_queue()
