# Percentile- & age-based analysis of Google Scholar data

See the [blog post](https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html) for details.

## System Architecture

* This is a simple Flask app running on Google Run (Cloud Run = managed offering of Kubernetes)
* We use the `scholarly` Python library to fetch data from Google Scholar.
* We put all scholarly calls that hit Google Scholar into Cloud Functions. It works much better for scalability than using the same code from the Flask server that runs on Cloud Run.
* We have set up two task queues (authors and publications) to launch many tasks for fetching authors and publications.
* We have a Cloud Scheduler that fetches the authors from the database that have not been refreshed for a while and fetches their latest versions from Google Scholar.
