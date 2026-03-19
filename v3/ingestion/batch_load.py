"""Batch load JSON files from GCS into BigQuery.

Streaming architecture — never materializes the full blob list in memory.
Files are processed in small batches (default 50) to bound memory usage.

Flow:
  1. Stream GCS blob listing one at a time
  2. Collect into batches of BATCH_SIZE
  3. For each batch:
     a. Download each file, validate JSON, build NDJSON line
     b. Upload NDJSON to a temp GCS location
     c. Load into BigQuery via load_table_from_uri
     d. On success: archive source files
     e. Bad files go to dead_letter/ so they don't block future runs
  4. Clean up temp NDJSON file after each batch
"""

import json
import logging
import os
import traceback
from datetime import datetime, timezone

import functions_framework
from google.cloud import bigquery, storage

from v3.ingestion.config import Config

logger = logging.getLogger(__name__)

_storage_client = None
_bigquery_client = None


def _get_storage_client():
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client(project=Config.PROJECT_ID)
    return _storage_client


def _get_bigquery_client():
    global _bigquery_client
    if _bigquery_client is None:
        _bigquery_client = bigquery.Client(project=Config.PROJECT_ID)
    return _bigquery_client


def _log(message, severity="INFO", **extra):
    """Emit structured JSON log."""
    payload = {"message": message, "severity": severity}
    payload.update(extra)
    print(json.dumps(payload))


# ── GCS streaming ────────────────────────────────────────────────────────────


def iter_gcs_files(bucket_name, source_prefix, max_files=None):
    """Yield JSON file info dicts from GCS, one at a time (streaming).

    Never materializes the full blob list in memory.
    Yields at most max_files items if specified.
    """
    client = _get_storage_client()
    blobs = client.list_blobs(bucket_name, prefix=source_prefix)
    count = 0

    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue
        yield {
            "gcs_uri": f"gs://{bucket_name}/{blob.name}",
            "name": blob.name,
            "blob_object": blob,
            "updated_time": (
                blob.updated.isoformat()
                if blob.updated
                else datetime.now(timezone.utc).isoformat()
            ),
        }
        count += 1
        if max_files and count >= max_files:
            _log(f"Reached max_files limit ({max_files}) for {source_prefix}")
            return

    _log(f"Found {count} .json files under gs://{bucket_name}/{source_prefix}")


# ── File handling ────────────────────────────────────────────────────────────


def move_to_dead_letter(file_info, source_prefix, reason):
    """Move a bad file to dead_letter/ so it is not retried."""
    client = _get_storage_client()
    bucket = client.bucket(Config.BUCKET_NAME)
    original_blob = file_info["blob_object"]
    dead_letter_name = file_info["name"].replace(
        source_prefix, Config.DEAD_LETTER_PREFIX, 1
    )

    try:
        bucket.copy_blob(original_blob, bucket, dead_letter_name)
        original_blob.delete()
        _log(
            f"Moved bad file to dead letter: {file_info['gcs_uri']}",
            severity="WARNING",
            reason=reason,
            dead_letter_path=f"gs://{Config.BUCKET_NAME}/{dead_letter_name}",
        )
    except Exception as e:
        _log(
            f"Failed to move file to dead letter: {file_info['gcs_uri']}",
            severity="ERROR",
            error=str(e),
        )


def prepare_ndjson_line(file_info, source_prefix):
    """Download a single JSON file and convert it to an NDJSON line.

    Returns (ndjson_line_str, None) on success, or (None, reason) on failure.
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


def archive_files(files_to_archive, source_prefix, archive_prefix):
    """Move successfully loaded files from source to archive prefix."""
    client = _get_storage_client()
    bucket = client.bucket(Config.BUCKET_NAME)
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
            _log(
                f"Error archiving {original_blob.name}",
                severity="ERROR",
                error=str(e),
            )
            failed.append(file_info["name"])

    _log(
        f"Archived {archived}/{len(files_to_archive)} files.",
        severity="INFO" if not failed else "WARNING",
        failed_count=len(failed),
    )


# ── NDJSON + BigQuery loading ────────────────────────────────────────────────


def upload_ndjson(ndjson_lines, suffix):
    """Upload a list of NDJSON line strings to a temp GCS location.

    Returns the gs:// URI of the uploaded file.
    """
    ndjson_content = "\n".join(ndjson_lines)
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    temp_blob_name = f"{Config.TEMP_PREFIX}{timestamp_str}_{suffix}"

    client = _get_storage_client()
    temp_bucket = client.bucket(Config.TEMP_BUCKET_NAME)
    temp_blob = temp_bucket.blob(temp_blob_name)
    temp_blob.upload_from_string(ndjson_content, content_type="application/jsonl")

    uri = f"gs://{Config.TEMP_BUCKET_NAME}/{temp_blob_name}"
    _log(f"Uploaded NDJSON ({len(ndjson_lines)} rows) to {uri}")
    return uri


def load_to_bigquery(ndjson_gcs_uri, bq_table_id):
    """Load an NDJSON file from GCS into BigQuery. Returns True on success."""
    client = _get_bigquery_client()
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
        load_job = client.load_table_from_uri(
            ndjson_gcs_uri, bq_table_id, job_config=job_config
        )
        _log(
            "BigQuery load job submitted.",
            job_id=load_job.job_id,
            table_id=bq_table_id,
            source_uri=ndjson_gcs_uri,
        )

        load_job.result()  # Wait for completion

        if load_job.error_result:
            _log(
                f"BigQuery job {load_job.job_id} FAILED.",
                severity="ERROR",
                job_id=load_job.job_id,
                error_result=load_job.error_result,
                errors=load_job.errors,
            )
            return False

        if load_job.errors:
            _log(
                f"BigQuery job {load_job.job_id} completed with warnings.",
                severity="WARNING",
                job_id=load_job.job_id,
                errors=load_job.errors,
            )
            return False

        _log(
            f"BigQuery job {load_job.job_id} completed successfully.",
            job_id=load_job.job_id,
            output_rows=load_job.output_rows,
        )
        return True

    except Exception as e:
        _log(
            f"Exception during BigQuery load from {ndjson_gcs_uri}.",
            severity="ERROR",
            job_id=load_job.job_id if load_job else "N/A",
            error=str(e),
            traceback=traceback.format_exc(),
        )
        return False


def cleanup_temp_file(gcs_uri):
    """Delete a temporary NDJSON file from GCS."""
    if not gcs_uri:
        return
    try:
        bucket_name, blob_name = gcs_uri.replace("gs://", "").split("/", 1)
        _get_storage_client().bucket(bucket_name).blob(blob_name).delete()
    except Exception as e:
        _log(f"Error deleting temp file {gcs_uri}", severity="WARNING", error=str(e))


# ── Batch orchestration ──────────────────────────────────────────────────────


def process_batch(
    files_batch, source_prefix, archive_prefix, bq_table_id, entity_type, batch_num
):
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
        _log(
            f"Batch {batch_num}: No valid files for {entity_type}.",
            severity="WARNING",
        )
        return 0

    suffix = f"{entity_type.lower()}_batch{batch_num}.ndjson"
    ndjson_uri = upload_ndjson(ndjson_lines, suffix)

    try:
        if load_to_bigquery(ndjson_uri, bq_table_id):
            _log(
                f"Batch {batch_num}: BQ load succeeded for {len(good_files)} {entity_type} files. Archiving."
            )
            archive_files(good_files, source_prefix, archive_prefix)
            return len(good_files)
        else:
            _log(
                f"Batch {batch_num}: BQ load FAILED for {entity_type}. Files NOT archived.",
                severity="ERROR",
            )
            return 0
    finally:
        # Free the NDJSON lines from memory before cleanup
        ndjson_lines.clear()
        cleanup_temp_file(ndjson_uri)


def process_entity(
    source_prefix,
    archive_prefix,
    bq_table_id,
    entity_type,
    batch_size,
    max_files=None,
):
    """Process files under a source prefix in streaming batches.

    Iterates GCS blobs one at a time, collecting batch_size files before
    processing each batch. Never holds more than batch_size blob references
    in memory. Stops after max_files total to stay within time/memory limits.
    """
    if max_files is None:
        max_files = Config.MAX_FILES_PER_RUN

    _log(
        f"Starting processing for {entity_type}",
        source=f"gs://{Config.BUCKET_NAME}/{source_prefix}",
        bigquery_table=bq_table_id,
        batch_size=batch_size,
        max_files=max_files,
    )

    total_archived = 0
    total_seen = 0
    batch_num = 0
    current_batch = []

    for file_info in iter_gcs_files(
        Config.BUCKET_NAME, source_prefix, max_files=max_files
    ):
        current_batch.append(file_info)
        total_seen += 1

        if len(current_batch) >= batch_size:
            batch_num += 1
            _log(
                f"Processing batch {batch_num} ({len(current_batch)} files) for {entity_type}"
            )
            total_archived += process_batch(
                current_batch,
                source_prefix,
                archive_prefix,
                bq_table_id,
                entity_type,
                batch_num,
            )
            current_batch = []

    # Process remaining files
    if current_batch:
        batch_num += 1
        _log(
            f"Processing batch {batch_num} ({len(current_batch)} files) for {entity_type}"
        )
        total_archived += process_batch(
            current_batch,
            source_prefix,
            archive_prefix,
            bq_table_id,
            entity_type,
            batch_num,
        )

    if total_seen == 0:
        _log(f"No {entity_type} files to process.")
    else:
        _log(
            f"Finished {entity_type}: {total_archived}/{total_seen} files loaded and archived.",
            batches_processed=batch_num,
        )


# ── Cloud Function entry point ───────────────────────────────────────────────


@functions_framework.http
def batch_load_gcs_to_bq(request):
    """HTTP entry point. Accepts optional ?batch_size=N and ?max_files=N."""
    try:
        batch_size = int(request.args.get("batch_size", Config.BATCH_SIZE))
        batch_size = max(1, min(batch_size, 500))

        max_files = int(request.args.get("max_files", Config.MAX_FILES_PER_RUN))
        max_files = max(1, min(max_files, 10000))

        _log(
            "Batch GCS to BigQuery run STARTING.",
            severity="NOTICE",
            batch_size=batch_size,
            max_files=max_files,
        )

        author_table = Config.bq_table_id(Config.BQ_AUTHOR_TABLE)
        process_entity(
            Config.SOURCE_AUTHORS_PREFIX,
            Config.ARCHIVE_AUTHORS_PREFIX,
            author_table,
            "Author",
            batch_size,
            max_files,
        )

        pub_table = Config.bq_table_id(Config.BQ_PUB_TABLE)
        process_entity(
            Config.SOURCE_PUBS_PREFIX,
            Config.ARCHIVE_PUBS_PREFIX,
            pub_table,
            "Publication",
            batch_size,
            max_files,
        )

        _log("Batch GCS to BigQuery run FINISHED.", severity="NOTICE")
        return ("Batch processing and archival finished successfully.", 200)

    except Exception as e:
        _log(
            "Critical error in batch_load_gcs_to_bq handler.",
            severity="CRITICAL",
            error=str(e),
            traceback=traceback.format_exc(),
        )
        return (f"Critical Error: {e}", 500)
