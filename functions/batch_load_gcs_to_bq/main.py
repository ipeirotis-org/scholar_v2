import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import os
import json
from datetime import datetime, timezone
import re # For parsing YYYY/MM/DD from paths
import traceback # For detailed exception logging

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
    raise  # Stop execution if clients can't be initialized

def list_gcs_files_by_folder(bucket_name, source_prefix):
    files_by_folder = {}
    blobs = storage_client.list_blobs(bucket_name, prefix=source_prefix)
    date_folder_pattern = re.compile(r"(\d{4}/\d{2}/\d{2})/")
    processed_files_count = 0

    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue
        processed_files_count += 1
        match = date_folder_pattern.search(blob.name[len(source_prefix):])
        if match:
            date_folder = match.group(1)
            if date_folder not in files_by_folder:
                files_by_folder[date_folder] = []
            
            files_by_folder[date_folder].append({
                "gcs_uri": f"gs://{bucket_name}/{blob.name}",
                "name": blob.name,
                "blob_object": blob,
                "updated_time": blob.updated.isoformat() if blob.updated else datetime.now(timezone.utc).isoformat(),
            })
    print(json.dumps({
        "message": f"Total .json files scanned under gs://{bucket_name}/{source_prefix}: {processed_files_count}. Grouped into {len(files_by_folder)} date folders.",
        "severity": "INFO"
    }))
    return files_by_folder
def generate_ndjson_and_upload(files_info_list, temp_ndjson_filename_suffix, entity_type_name, date_folder):
    ndjson_lines = []
    successfully_prepped_files_info = []

    log_payload_gen = {
        "message": f"Starting NDJSON generation for {entity_type_name}, folder {date_folder}",
        "file_count": len(files_info_list),
        "severity": "INFO"
    }
    print(json.dumps(log_payload_gen))

    for file_info in files_info_list:
        try:
            original_json_content_str = file_info["blob_object"].download_as_text()
            
            if not original_json_content_str.strip() or \
               not (original_json_content_str.startswith('{') and original_json_content_str.endswith('}')):
                print(json.dumps({
                    "message": f"Skipping empty or non-object JSON file: {file_info['gcs_uri']}",
                    "severity": "WARNING"
                }))
                continue

            # --- Modification Start ---
            # Parse the original JSON content
            original_json_dict = json.loads(original_json_content_str)
            # Create the new structure with the original content nested under "data"
            wrapped_json_dict = {"data": original_json_dict}
            # Convert the new wrapped dictionary back to a JSON string for the 'DATA' field
            new_data_field_as_string = json.dumps(wrapped_json_dict)
            # --- Modification End ---

            row_data = {
                "document_id": os.path.basename(file_info["name"]),
                "timestamp": file_info["updated_time"],
                "DATA": new_data_field_as_string, # Use the new wrapped JSON string
            }
            ndjson_lines.append(json.dumps(row_data)) # This line becomes an NDJSON line
            successfully_prepped_files_info.append(file_info)
        except json.JSONDecodeError as jde:
            print(json.dumps({
                "message": f"JSONDecodeError processing file {file_info['gcs_uri']}. Invalid JSON content.",
                "error": str(jde),
                "file_uri": file_info['gcs_uri'],
                "severity": "ERROR"
            }))
        except Exception as e:
            print(json.dumps({
                "message": f"Error processing file {file_info['gcs_uri']} for NDJSON generation.",
                "error": str(e),
                "file_uri": file_info['gcs_uri'],
                "severity": "ERROR"
            }))

    if not ndjson_lines:
        print(json.dumps({
            "message": f"No valid data to upload for {temp_ndjson_filename_suffix} from folder {date_folder}.",
            "severity": "WARNING"
        }))
        return None, []

    if ndjson_lines:
        print(json.dumps({
            "message": "Sample NDJSON line being generated (first line if multiple) - after nesting",
            "sample_line": ndjson_lines[0], # This is a string representing a BQ row
            "total_lines": len(ndjson_lines),
            "severity": "DEBUG" 
        }))
    
    ndjson_content = "\n".join(ndjson_lines) # Each item in ndjson_lines is already a JSON string for a row
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    temp_blob_name = f"{TEMP_GCS_PREFIX}{timestamp_str}_{temp_ndjson_filename_suffix}"
    
    temp_bucket = storage_client.bucket(TEMP_GCS_BUCKET_NAME)
    temp_blob = temp_bucket.blob(temp_blob_name)
    temp_blob.upload_from_string(ndjson_content, content_type="application/jsonl")
    uploaded_uri = f"gs://{TEMP_GCS_BUCKET_NAME}/{temp_blob_name}"
    
    print(json.dumps({
        "message": f"NDJSON data for {entity_type_name}, folder {date_folder} uploaded to {uploaded_uri}",
        "source_file_count": len(successfully_prepped_files_info),
        "ndjson_lines": len(ndjson_lines),
        "severity": "INFO"
    }))
    return uploaded_uri, successfully_prepped_files_info


def load_ndjson_to_bigquery(ndjson_gcs_uri, bq_table_id):
    if not ndjson_gcs_uri:
        print(json.dumps({
            "message": f"No NDJSON URI provided for BigQuery table {bq_table_id}, skipping load.",
            "severity": "WARNING"
        }))
        return False

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("document_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("data", "STRING", mode="NULLABLE"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = None # Initialize to None
    try:
        load_job = bigquery_client.load_table_from_uri(
            ndjson_gcs_uri, bq_table_id, job_config=job_config
        )
        print(json.dumps({
            "message": "BigQuery load job submitted.",
            "job_id": load_job.job_id,
            "table_id": bq_table_id,
            "source_uri": ndjson_gcs_uri,
            "severity": "INFO"
        }))
        
        load_job.result()  # Waits for the job to complete.

        # After job.result(), check job properties for errors
        if load_job.error_result:
            print(json.dumps({
                "message": f"BigQuery job {load_job.job_id} for table {bq_table_id} FAILED.",
                "job_id": load_job.job_id,
                "error_result": load_job.error_result, # This is a dict
                "errors": load_job.errors, # This is a list of dicts
                "severity": "ERROR"
            }))
            # The "SELECT list must not be empty" error might be buried in error_result or errors.
            if "SELECT list must not be empty" in json.dumps(load_job.error_result):
                 print(json.dumps({"message": "FOUND 'SELECT list must not be empty' in BQ job error_result.", "severity": "ERROR"}))
            return False
        
        # Even if no error_result, job.errors might contain non-fatal issues or warnings
        if load_job.errors and len(load_job.errors) > 0:
            print(json.dumps({
                "message": f"BigQuery job {load_job.job_id} for table {bq_table_id} completed but with some errors/warnings.",
                "job_id": load_job.job_id,
                "errors": load_job.errors,
                "severity": "WARNING"
            }))
            # Depending on policy, you might still treat this as a failure for archival purposes.
            # For now, let's be strict: any errors means it's not a clean success.
            return False

        # Check if rows were actually loaded - useful for empty source files or all-error scenarios
        destination_table = bigquery_client.get_table(bq_table_id)
        print(json.dumps({
            "message": f"BigQuery job {load_job.job_id} for table {bq_table_id} completed successfully.",
            "job_id": load_job.job_id,
            "output_rows": load_job.output_rows, # Rows written by this job
            "total_rows_in_table_after_job": destination_table.num_rows,
            "severity": "INFO"
        }))
        return True # Success only if no error_result and no errors array

    except Exception as e:
        # This will catch exceptions from job.result() if the job failed fundamentally,
        # or from the initial submission if that failed.
        tb_str = traceback.format_exc()
        error_payload = {
            "message": f"Exception during BigQuery load for table {bq_table_id} from {ndjson_gcs_uri}.",
            "job_id": load_job.job_id if load_job else "Not available (submission might have failed)",
            "exception_type": type(e).__name__,
            "error": str(e),
            "traceback": tb_str,
            "severity": "ERROR"
        }
        print(json.dumps(error_payload))
        if "SELECT list must not be empty" in str(e) or "SELECT list must not be empty" in tb_str:
             print(json.dumps({"message": "FOUND 'SELECT list must not be empty' in BQ job exception.", "severity": "ERROR"}))
        return False


def archive_gcs_files(bucket_name, files_to_archive_info, source_prefix, archive_prefix):
    source_bucket = storage_client.bucket(bucket_name)
    archived_count = 0
    failed_to_archive = []

    log_payload_archive_start = {
        "message": "Starting archival process.",
        "file_count_to_archive": len(files_to_archive_info),
        "source_prefix": source_prefix,
        "archive_prefix": archive_prefix,
        "severity": "INFO"
    }
    print(json.dumps(log_payload_archive_start))

    for file_info in files_to_archive_info:
        original_blob = file_info["blob_object"]
        archive_blob_name = file_info["name"].replace(source_prefix, archive_prefix, 1)
        
        try:
            # It's a move: copy then delete original
            new_blob = source_bucket.copy_blob(original_blob, source_bucket, archive_blob_name)
            original_blob.delete()
            # print(f"Successfully archived gs://{bucket_name}/{original_blob.name} to gs://{bucket_name}/{new_blob.name}")
            archived_count +=1
        except Exception as e:
            log_payload_archive_error = {
                "message": "Error archiving file.",
                "source_file": f"gs://{bucket_name}/{original_blob.name}",
                "target_archive_file": f"gs://{bucket_name}/{archive_blob_name}",
                "error": str(e),
                "severity": "ERROR"
            }
            print(json.dumps(log_payload_archive_error))
            failed_to_archive.append(file_info["name"])
            
    print(json.dumps({
        "message": "Archival process summary.",
        "archived_count": archived_count,
        "attempted_count": len(files_to_archive_info),
        "failed_count": len(failed_to_archive),
        "failed_files_sample": failed_to_archive[:5], # Log a sample of failed files
        "severity": "INFO" if not failed_to_archive else "WARNING"
    }))


def cleanup_temp_gcs_file(gcs_uri):
    if not gcs_uri: return
    try:
        bucket_name, blob_name = gcs_uri.replace("gs://", "").split("/", 1)
        blob = storage_client.bucket(bucket_name).blob(blob_name)
        blob.delete()
        print(json.dumps({
            "message": "Successfully deleted temporary NDJSON file.",
            "file_uri": gcs_uri,
            "severity": "INFO"
        }))
    except Exception as e:
        print(json.dumps({
            "message": "Error deleting temporary NDJSON file.",
            "file_uri": gcs_uri,
            "error": str(e),
            "severity": "WARNING"
        }))

def process_source_path(source_prefix, archive_prefix, bq_table_id_full, entity_type_name):
    print(json.dumps({
        "message": f"Starting processing for entity type: {entity_type_name}",
        "source_prefix": f"gs://{GCS_BUCKET_NAME}/{source_prefix}",
        "archive_prefix": f"gs://{GCS_BUCKET_NAME}/{archive_prefix}",
        "bigquery_table": bq_table_id_full,
        "severity": "INFO"
    }))
    
    files_by_folder = list_gcs_files_by_folder(GCS_BUCKET_NAME, source_prefix)

    if not files_by_folder:
        print(json.dumps({
            "message": f"No {entity_type_name} files found in any YYYY/MM/DD subdirectories under {source_prefix}.",
            "severity": "INFO"
        }))
        return

    total_folders_processed = 0
    total_files_archived_for_entity = 0

    for date_folder, files_in_folder in files_by_folder.items():
        total_folders_processed += 1
        log_payload_folder = {
            "message": f"Processing folder for {entity_type_name}.",
            "date_folder": date_folder,
            "file_count_in_folder": len(files_in_folder),
            "source_path_prefix": f"{source_prefix}{date_folder}",
            "severity": "INFO"
        }
        print(json.dumps(log_payload_folder))

        if not files_in_folder:
            continue

        safe_date_folder = date_folder.replace("/", "")
        ndjson_gcs_uri, successfully_prepped_files_info = generate_ndjson_and_upload(
            files_in_folder, f"{entity_type_name.lower()}_{safe_date_folder}.ndjson",
            entity_type_name, date_folder
        )

        if ndjson_gcs_uri and successfully_prepped_files_info:
            bq_load_successful = load_ndjson_to_bigquery(ndjson_gcs_uri, bq_table_id_full)
            
            if bq_load_successful:
                print(json.dumps({
                    "message": f"BigQuery load successful for {entity_type_name} from folder {date_folder}. Archiving original files.",
                    "severity": "INFO"
                }))
                archive_gcs_files(GCS_BUCKET_NAME, successfully_prepped_files_info, source_prefix, archive_prefix)
                total_files_archived_for_entity += len(successfully_prepped_files_info)
            else:
                # Critical: If BQ load failed, do NOT archive the source files for this batch.
                print(json.dumps({
                    "message": f"BigQuery load FAILED for {entity_type_name} from folder {date_folder}. Original files for this batch will NOT be archived.",
                    "ndjson_uri_problematic": ndjson_gcs_uri,
                    "severity": "ERROR"
                }))
            
            cleanup_temp_gcs_file(ndjson_gcs_uri) # Clean up temp NDJSON regardless of BQ success
        elif ndjson_gcs_uri: 
             cleanup_temp_gcs_file(ndjson_gcs_uri)
        else:
            print(json.dumps({
                "message": f"NDJSON generation failed or no files to process for {entity_type_name} in folder {date_folder}.",
                "severity": "WARNING"
            }))
    
    print(json.dumps({
        "message": f"Finished processing for entity: {entity_type_name}.",
        "folders_processed_count": total_folders_processed,
        "total_files_archived_for_entity": total_files_archived_for_entity,
        "severity": "INFO"
    }))


@functions_framework.http
def batch_load_gcs_to_bq(request):
    try:
        print(json.dumps({"message": "Batch GCS to BigQuery processing and archival run V3 STARTING.", "severity": "NOTICE"}))

        author_table_id_full = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_AUTHOR_TABLE_NAME}"
        process_source_path(SOURCE_AUTHORS_PREFIX, ARCHIVE_AUTHORS_PREFIX, author_table_id_full, "Author")

        pub_table_id_full = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_PUB_TABLE_NAME}"
        process_source_path(SOURCE_PUBLICATIONS_PREFIX, ARCHIVE_PUBLICATIONS_PREFIX, pub_table_id_full, "Publication")

        print(json.dumps({"message": "Batch GCS to BigQuery processing and archival run V3 FINISHED.", "severity": "NOTICE"}))
        return ("Batch processing and archival finished successfully.", 200)

    except Exception as e:
        tb_str = traceback.format_exc()
        print(json.dumps({
            "message": "Critical error in orchestrator_v3 HTTP handler.",
            "exception_type": type(e).__name__,
            "error": str(e),
            "traceback": tb_str,
            "severity": "CRITICAL"
        }))
        return (f"Critical Error: {e}", 500)