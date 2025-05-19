import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import os
import json
from datetime import datetime, timezone
import re # For parsing YYYY/MM/DD from paths

# Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT", "scholar-version2")
BQ_DATASET_ID = "scholar_raw_data"
BQ_AUTHOR_TABLE_NAME = "author"
BQ_PUB_TABLE_NAME = "pub"
GCS_BUCKET_NAME = "scholar_data_share" # Source and Archive bucket
TEMP_GCS_BUCKET_NAME = os.environ.get("TEMP_GCS_BUCKET", GCS_BUCKET_NAME)
TEMP_GCS_PREFIX = "bq_load_temp/"

SOURCE_AUTHORS_PREFIX = "authors_json/"
ARCHIVE_AUTHORS_PREFIX = "authors_archive/"
SOURCE_PUBLICATIONS_PREFIX = "publications_json/"
ARCHIVE_PUBLICATIONS_PREFIX = "publications_archive/"

# Initialize clients
try:
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()
except Exception as e:
    print(f"Error initializing Google Cloud clients: {e}")
    raise

def list_gcs_files_by_folder(bucket_name, source_prefix):
    """
    Lists GCS .json files, grouping them by their YYYY/MM/DD parent folder.
    Returns a dictionary where keys are 'YYYY/MM/DD' and values are lists of file_info.
    """
    files_by_folder = {}
    blobs = storage_client.list_blobs(bucket_name, prefix=source_prefix)
    date_folder_pattern = re.compile(r"(\d{4}/\d{2}/\d{2})/")

    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue

        match = date_folder_pattern.search(blob.name[len(source_prefix):])
        if match:
            date_folder = match.group(1)
            if date_folder not in files_by_folder:
                files_by_folder[date_folder] = []
            
            files_by_folder[date_folder].append({
                "gcs_uri": f"gs://{bucket_name}/{blob.name}",
                "name": blob.name, # Full path
                "blob_object": blob, # Keep blob object for easier move later
                "updated_time": blob.updated.isoformat() if blob.updated else datetime.now(timezone.utc).isoformat(),
            })
    return files_by_folder

def generate_ndjson_and_upload(files_info_list, temp_ndjson_filename_suffix):
    """
    Generates an NDJSON string from a list of file_info objects, uploads to temp GCS.
    """
    ndjson_lines = []
    processed_file_details = [] # To keep track of files included in this NDJSON

    for file_info in files_info_list:
        try:
            json_content_str = file_info["blob_object"].download_as_text()
            row_data = {
                "document_id": os.path.basename(file_info["name"]),
                "timestamp": file_info["updated_time"],
                "DATA": json_content_str,
            }
            ndjson_lines.append(json.dumps(row_data))
            processed_file_details.append(file_info) # Add to list for later archival
        except Exception as e:
            print(f"Error processing file {file_info['gcs_uri']} for NDJSON: {e}")
            # Optionally, decide if this error should halt the batch or just skip the file

    if not ndjson_lines:
        print(f"No data to upload for {temp_ndjson_filename_suffix}.")
        return None, []

    ndjson_content = "\n".join(ndjson_lines)
    # Ensure unique temp file name, e.g., by including timestamp or a unique ID
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    temp_blob_name = f"{TEMP_GCS_PREFIX}{timestamp_str}_{temp_ndjson_filename_suffix}"
    
    temp_bucket = storage_client.bucket(TEMP_GCS_BUCKET_NAME)
    temp_blob = temp_bucket.blob(temp_blob_name)
    temp_blob.upload_from_string(ndjson_content, content_type="application/jsonl")
    uploaded_uri = f"gs://{TEMP_GCS_BUCKET_NAME}/{temp_blob_name}"
    print(f"Uploaded NDJSON data to {uploaded_uri}")
    return uploaded_uri, processed_file_details


def load_ndjson_to_bigquery(ndjson_gcs_uri, bq_table_id):
    """Loads data from an NDJSON file in GCS to a BigQuery table."""
    if not ndjson_gcs_uri:
        print(f"No NDJSON URI provided for BigQuery table {bq_table_id}, skipping load.")
        return False

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("document_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("DATA", "STRING", mode="NULLABLE"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    try:
        load_job = bigquery_client.load_table_from_uri(
            ndjson_gcs_uri, bq_table_id, job_config=job_config
        )
        print(f"Starting BigQuery load job {load_job.job_id} for {bq_table_id} from {ndjson_gcs_uri}")
        load_job.result() # Waits for the job to complete.
        if load_job.errors:
            print(f"BigQuery load job {load_job.job_id} finished with errors: {load_job.errors}")
            return False
        print(f"BigQuery load job {load_job.job_id} completed successfully.")
        return True
    except Exception as e:
        print(f"Error submitting/running BigQuery load job for {bq_table_id} from {ndjson_gcs_uri}: {e}")
        return False

def archive_gcs_files(bucket_name, files_to_archive_info, source_prefix, archive_prefix):
    """Moves files from source_prefix to archive_prefix within the same bucket."""
    source_bucket = storage_client.bucket(bucket_name)
    archived_count = 0
    for file_info in files_to_archive_info:
        original_blob = file_info["blob_object"]
        # Construct archive path by replacing source prefix with archive prefix
        archive_blob_name = file_info["name"].replace(source_prefix, archive_prefix, 1)
        
        try:
            # Copy to archive location
            new_blob = source_bucket.copy_blob(original_blob, source_bucket, archive_blob_name)
            # Delete original blob
            original_blob.delete()
            print(f"Successfully archived gs://{bucket_name}/{original_blob.name} to gs://{bucket_name}/{new_blob.name}")
            archived_count +=1
        except Exception as e:
            print(f"Error archiving file gs://{bucket_name}/{original_blob.name}: {e}")
            # Decide how to handle partial failures: retry, log for manual intervention, etc.
    print(f"Archived {archived_count}/{len(files_to_archive_info)} files.")


def cleanup_temp_gcs_file(gcs_uri):
    """Deletes a file from GCS."""
    if not gcs_uri: return
    try:
        bucket_name, blob_name = gcs_uri.replace("gs://", "").split("/", 1)
        blob = storage_client.bucket(bucket_name).blob(blob_name)
        blob.delete()
        print(f"Successfully deleted temporary file: {gcs_uri}")
    except Exception as e:
        print(f"Error deleting temporary file {gcs_uri}: {e}")


def process_source_path(source_prefix, archive_prefix, bq_table_id_full, entity_type_name):
    """Generic function to process files for a given source path."""
    print(f"\n--- Processing {entity_type_name} Files from gs://{GCS_BUCKET_NAME}/{source_prefix} ---")
    
    files_by_folder = list_gcs_files_by_folder(GCS_BUCKET_NAME, source_prefix)

    if not files_by_folder:
        print(f"No {entity_type_name} files found in any YYYY/MM/DD subdirectories under {source_prefix}.")
        return

    for date_folder, files_in_folder in files_by_folder.items():
        print(f"Processing {len(files_in_folder)} {entity_type_name} files from folder: {source_prefix}{date_folder}")
        if not files_in_folder:
            continue

        # Sanitize date_folder for use in filename
        safe_date_folder = date_folder.replace("/", "")
        ndjson_gcs_uri, successfully_prepped_files = generate_ndjson_and_upload(
            files_in_folder, f"{entity_type_name.lower()}_{safe_date_folder}.ndjson"
        )

        if ndjson_gcs_uri and successfully_prepped_files:
            bq_load_successful = load_ndjson_to_bigquery(ndjson_gcs_uri, bq_table_id_full)
            
            if bq_load_successful:
                print(f"BigQuery load successful for {entity_type_name} from {date_folder}. Archiving original files.")
                archive_gcs_files(GCS_BUCKET_NAME, successfully_prepped_files, source_prefix, archive_prefix)
            else:
                print(f"BigQuery load failed for {entity_type_name} from {date_folder}. Original files will not be archived for this batch.")
            
            cleanup_temp_gcs_file(ndjson_gcs_uri)
        elif ndjson_gcs_uri: # URI exists but no successfully_prepped_files (should not happen if URI is not None)
             cleanup_temp_gcs_file(ndjson_gcs_uri) # Clean up temp file if it was created but BQ load won't run
        else:
            print(f"NDJSON generation failed or no files to process for {entity_type_name} in {date_folder}.")


@functions_framework.http
def batch_load_gcs_to_bq(request):
    """
    Orchestrates the batch loading of all GCS JSON files to BigQuery and archives them.
    Triggered by Cloud Scheduler.
    """
    try:
        print("Starting batch GCS to BigQuery processing and archival run.")

        # Process Authors
        author_table_id_full = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_AUTHOR_TABLE_NAME}"
        process_source_path(SOURCE_AUTHORS_PREFIX, ARCHIVE_AUTHORS_PREFIX, author_table_id_full, "Author")

        # Process Publications
        pub_table_id_full = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_PUB_TABLE_NAME}"
        process_source_path(SOURCE_PUBLICATIONS_PREFIX, ARCHIVE_PUBLICATIONS_PREFIX, pub_table_id_full, "Publication")

        print("\nBatch processing and archival finished.")
        return ("Batch processing and archival finished.", 200)

    except Exception as e:
        print(f"Critical error in orchestrator_v2: {e}")
        # Consider logging the traceback for detailed debugging
        import traceback
        traceback.print_exc()
        return (f"Error: {e}", 500)