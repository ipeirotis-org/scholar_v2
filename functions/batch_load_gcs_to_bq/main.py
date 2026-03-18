import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import os
import json
from datetime import datetime, timezone
import re
import traceback

# Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT", "scholar-version2")
BQ_DATASET_ID = "scholar_raw_data"
BQ_AUTHOR_TABLE_NAME = "author"
BQ_PUB_TABLE_NAME = "pub"
GCS_BUCKET_NAME = "scholar_data_share"
TEMP_GCS_BUCKET_NAME = os.environ.get("TEMP_GCS_BUCKET", GCS_BUCKET_NAME)
TEMP_GCS_PREFIX = "bq_load_temp/"

SOURCE_AUTHORS_PREFIX = "authors_json/"
ARCHIVE_AUTHORS_PREFIX = "authors_archive/"
SOURCE_PUBLICATIONS_PREFIX = "publications_json/"
ARCHIVE_PUBLICATIONS_PREFIX = "publications_archive/"
DEAD_LETTER_PREFIX = "dead_letter/"

# Max files to process in a single NDJSON batch to avoid timeouts
DEFAULT_BATCH_SIZE = 50

# Initialize clients globally
try:
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()
except Exception as e:
    print(json.dumps({
        "message": "Critical: Error initializing Google Cloud clients",
        "error": str(e),
        "severity": "CRITICAL"
    }))
    raise


def _log(message, severity="INFO", **extra):
    """Helper to emit structured JSON logs."""
    payload = {"message": message, "severity": severity}
    payload.update(extra)
    print(json.dumps(payload))


def list_gcs_files(bucket_name, source_prefix):
    """List all JSON files under source_prefix, returned as a flat list."""
    blobs = storage_client.list_blobs(bucket_name, prefix=source_prefix)
    files = []

    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue
        files.append({
            "gcs_uri": f"gs://{bucket_name}/{blob.name}",
            "name": blob.name,
            "blob_object": blob,
            "updated_time": blob.updated.isoformat() if blob.updated else datetime.now(timezone.utc).isoformat(),
        })

    _log(f"Found {len(files)} .json files under gs://{bucket_name}/{source_prefix}")
    return files


def move_to_dead_letter(file_info, source_prefix, reason):
    """Move a bad file to the dead_letter/ prefix so it is not retried."""
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    original_blob = file_info["blob_object"]
    dead_letter_name = file_info["name"].replace(source_prefix, DEAD_LETTER_PREFIX, 1)

    try:
        bucket.copy_blob(original_blob, bucket, dead_letter_name)
        original_blob.delete()
        _log(f"Moved bad file to dead letter: {file_info['gcs_uri']}",
             severity="WARNING", reason=reason,
             dead_letter_path=f"gs://{GCS_BUCKET_NAME}/{dead_letter_name}")
    except Exception as e:
        _log(f"Failed to move file to dead letter: {file_info['gcs_uri']}",
             severity="ERROR", error=str(e))


def prepare_ndjson_line(file_info, source_prefix):
    """Download a single JSON file and convert it to an NDJSON line.

    Returns (ndjson_line_str, file_info) on success, or (None, reason) on failure.
    """
    try:
        content = file_info["blob_object"].download_as_text()

        if not content.strip():
            return None, "empty_file"

        original = json.loads(content)
        wrapped = {"data": original}

        row = {
            "document_id": os.path.basename(file_info["name"]),
            "timestamp": file_info["updated_time"],
            "DATA": json.dumps(wrapped),
        }
        return json.dumps(row), None
    except json.JSONDecodeError as e:
        return None, f"invalid_json: {e}"
    except Exception as e:
        return None, f"download_error: {e}"


def generate_ndjson_and_upload(ndjson_lines, suffix):
    """Upload a list of NDJSON line strings to a temp GCS location.

    Returns the gs:// URI of the uploaded file.
    """
    ndjson_content = "\n".join(ndjson_lines)
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    temp_blob_name = f"{TEMP_GCS_PREFIX}{timestamp_str}_{suffix}"

    temp_bucket = storage_client.bucket(TEMP_GCS_BUCKET_NAME)
    temp_blob = temp_bucket.blob(temp_blob_name)
    temp_blob.upload_from_string(ndjson_content, content_type="application/jsonl")

    uri = f"gs://{TEMP_GCS_BUCKET_NAME}/{temp_blob_name}"
    _log(f"Uploaded NDJSON ({len(ndjson_lines)} rows) to {uri}")
    return uri


def load_ndjson_to_bigquery(ndjson_gcs_uri, bq_table_id):
    """Load an NDJSON file from GCS into BigQuery. Returns True on success."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("document_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("data", "STRING", mode="NULLABLE"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = None
    try:
        load_job = bigquery_client.load_table_from_uri(
            ndjson_gcs_uri, bq_table_id, job_config=job_config
        )
        _log("BigQuery load job submitted.",
             job_id=load_job.job_id, table_id=bq_table_id, source_uri=ndjson_gcs_uri)

        load_job.result()  # Wait for completion

        if load_job.error_result:
            _log(f"BigQuery job {load_job.job_id} FAILED.",
                 severity="ERROR", job_id=load_job.job_id,
                 error_result=load_job.error_result, errors=load_job.errors)
            return False

        if load_job.errors:
            _log(f"BigQuery job {load_job.job_id} completed with warnings.",
                 severity="WARNING", job_id=load_job.job_id, errors=load_job.errors)
            return False

        _log(f"BigQuery job {load_job.job_id} completed successfully.",
             job_id=load_job.job_id, output_rows=load_job.output_rows)
        return True

    except Exception as e:
        _log(f"Exception during BigQuery load from {ndjson_gcs_uri}.",
             severity="ERROR",
             job_id=load_job.job_id if load_job else "N/A",
             error=str(e), traceback=traceback.format_exc())
        return False


def archive_gcs_files(files_to_archive, source_prefix, archive_prefix):
    """Move successfully loaded files from source to archive prefix."""
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    archived = 0
    failed = []

    for file_info in files_to_archive:
        original_blob = file_info["blob_object"]
        archive_name = file_info["name"].replace(source_prefix, archive_prefix, 1)
        try:
            bucket.copy_blob(original_blob, bucket, archive_name)
            original_blob.delete()
            archived += 1
        except Exception as e:
            _log(f"Error archiving {original_blob.name}",
                 severity="ERROR", error=str(e))
            failed.append(file_info["name"])

    _log(f"Archived {archived}/{len(files_to_archive)} files.",
         severity="INFO" if not failed else "WARNING",
         failed_count=len(failed))


def cleanup_temp_gcs_file(gcs_uri):
    """Delete a temporary NDJSON file from GCS."""
    if not gcs_uri:
        return
    try:
        bucket_name, blob_name = gcs_uri.replace("gs://", "").split("/", 1)
        storage_client.bucket(bucket_name).blob(blob_name).delete()
    except Exception as e:
        _log(f"Error deleting temp file {gcs_uri}", severity="WARNING", error=str(e))


def process_batch(files_batch, source_prefix, archive_prefix, bq_table_id, entity_type, batch_num):
    """Process a single batch of files: prepare NDJSON, load to BQ, archive.

    Bad files are moved to dead_letter/ individually so they don't block future runs.
    Returns the number of files successfully loaded and archived.
    """
    ndjson_lines = []
    good_files = []

    for file_info in files_batch:
        line, error_reason = prepare_ndjson_line(file_info, source_prefix)
        if line is not None:
            ndjson_lines.append(line)
            good_files.append(file_info)
        else:
            move_to_dead_letter(file_info, source_prefix, error_reason)

    if not ndjson_lines:
        _log(f"Batch {batch_num}: No valid files for {entity_type}.", severity="WARNING")
        return 0

    suffix = f"{entity_type.lower()}_batch{batch_num}.ndjson"
    ndjson_uri = generate_ndjson_and_upload(ndjson_lines, suffix)

    try:
        if load_ndjson_to_bigquery(ndjson_uri, bq_table_id):
            _log(f"Batch {batch_num}: BQ load succeeded for {len(good_files)} {entity_type} files. Archiving.")
            archive_gcs_files(good_files, source_prefix, archive_prefix)
            return len(good_files)
        else:
            _log(f"Batch {batch_num}: BQ load FAILED for {entity_type}. Files NOT archived.",
                 severity="ERROR")
            return 0
    finally:
        cleanup_temp_gcs_file(ndjson_uri)


def process_source_path(source_prefix, archive_prefix, bq_table_id, entity_type, batch_size):
    """Process all files under a source prefix in batches."""
    _log(f"Starting processing for {entity_type}",
         source=f"gs://{GCS_BUCKET_NAME}/{source_prefix}",
         bigquery_table=bq_table_id, batch_size=batch_size)

    all_files = list_gcs_files(GCS_BUCKET_NAME, source_prefix)
    if not all_files:
        _log(f"No {entity_type} files to process.")
        return

    total_archived = 0
    num_batches = (len(all_files) + batch_size - 1) // batch_size

    for i in range(num_batches):
        batch = all_files[i * batch_size : (i + 1) * batch_size]
        _log(f"Processing batch {i + 1}/{num_batches} ({len(batch)} files) for {entity_type}")
        total_archived += process_batch(
            batch, source_prefix, archive_prefix, bq_table_id, entity_type, i + 1
        )

    _log(f"Finished {entity_type}: {total_archived}/{len(all_files)} files loaded and archived.",
         batches_processed=num_batches)


@functions_framework.http
def batch_load_gcs_to_bq(request):
    """HTTP entry point. Accepts optional ?batch_size=N query parameter."""
    try:
        batch_size = int(request.args.get("batch_size", DEFAULT_BATCH_SIZE))
        batch_size = max(1, min(batch_size, 500))  # Clamp to [1, 500]

        _log("Batch GCS to BigQuery run STARTING.", severity="NOTICE", batch_size=batch_size)

        author_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_AUTHOR_TABLE_NAME}"
        process_source_path(SOURCE_AUTHORS_PREFIX, ARCHIVE_AUTHORS_PREFIX, author_table, "Author", batch_size)

        pub_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_PUB_TABLE_NAME}"
        process_source_path(SOURCE_PUBLICATIONS_PREFIX, ARCHIVE_PUBLICATIONS_PREFIX, pub_table, "Publication", batch_size)

        _log("Batch GCS to BigQuery run FINISHED.", severity="NOTICE")
        return ("Batch processing and archival finished successfully.", 200)

    except Exception as e:
        _log("Critical error in batch_load_gcs_to_bq handler.",
             severity="CRITICAL", error=str(e), traceback=traceback.format_exc())
        return (f"Critical Error: {e}", 500)
