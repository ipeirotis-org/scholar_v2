"""Apply incremental S2 dataset diffs using BigQuery MERGE.

Diff files from the S2 Datasets API contain:
- update_files: Full records to upsert (same schema as base dataset)
- delete_files: Records containing only the primary key to delete

Strategy:
1. Download diff files to GCS under a temporary prefix
2. Load into temporary BigQuery tables
3. Apply deletes, then upserts via MERGE
4. Drop temporary tables
"""

import logging
import uuid

from google.cloud import bigquery

from dataset_ingestion.config import Config
from dataset_ingestion.downloader import download_dataset
from dataset_ingestion.loader import DATASET_SCHEMAS, _get_bq_client

logger = logging.getLogger(__name__)

# Primary key for each dataset (used in MERGE ON clause and DELETE)
DATASET_PRIMARY_KEYS = {
    "papers": "corpusid",
    "citations": "citationid",
    "authors": "authorid",
}

# Delete file schemas (just the primary key)
DELETE_SCHEMAS = {
    "papers": [bigquery.SchemaField("corpusid", "INTEGER", mode="REQUIRED")],
    "citations": [bigquery.SchemaField("citationid", "INTEGER", mode="REQUIRED")],
    "authors": [bigquery.SchemaField("authorid", "STRING", mode="REQUIRED")],
}

# Execution-scoped suffix to prevent temp table collisions between
# overlapping runs (e.g. manual trigger + scheduler, or retries).
_execution_id = uuid.uuid4().hex[:8]


def _temp_table(dataset_name, suffix):
    """Generate a unique temporary table ID scoped to this execution."""
    return Config.bq_table(f"_tmp_{dataset_name}_{suffix}_{_execution_id}")


def _load_temp_table(release_id, dataset_name, file_type, schema):
    """Download diff files to GCS and load into a temp BigQuery table.

    Args:
        release_id: Release ID for GCS path.
        dataset_name: e.g. "papers".
        file_type: "updates" or "deletes".
        schema: BigQuery schema for the temp table.

    Returns:
        Temp table ID, or None if no files to load.
    """
    gcs_prefix = f"{dataset_name}_diff_{file_type}"
    temp_table_id = _temp_table(dataset_name, file_type)
    source_uri = Config.gcs_uri_pattern(release_id, gcs_prefix)

    logger.info("Loading %s %s into temp table %s", dataset_name, file_type, temp_table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
        ignore_unknown_values=True,
    )

    client = _get_bq_client()
    load_job = client.load_table_from_uri(source_uri, temp_table_id, job_config=job_config)
    load_job.result()

    table = client.get_table(temp_table_id)
    logger.info("Temp table %s: %d rows", temp_table_id, table.num_rows)
    return temp_table_id


def _apply_diff_dml(dataset_name, delete_table_id, update_table_id):
    """Apply delete and upsert atomically in a BigQuery transaction.

    Wraps both DML statements in BEGIN TRANSACTION / COMMIT TRANSACTION
    so they succeed or fail together. If either statement fails, BigQuery
    rolls back both, avoiding a partially applied state.
    """
    pk = DATASET_PRIMARY_KEYS[dataset_name]
    main_table = Config.bq_table_ref(dataset_name)
    schema = DATASET_SCHEMAS[dataset_name]

    statements = ["BEGIN TRANSACTION;"]

    if delete_table_id:
        statements.append(
            f"DELETE FROM {main_table} t "
            f"WHERE t.{pk} IN (SELECT {pk} FROM `{delete_table_id}`);"
        )

    if update_table_id:
        all_columns = [f.name for f in schema]
        update_sets = ", ".join(f"t.{col} = s.{col}" for col in all_columns if col != pk)
        insert_columns = ", ".join(all_columns)
        insert_values = ", ".join(f"s.{col}" for col in all_columns)

        statements.append(f"""
        MERGE {main_table} t
        USING `{update_table_id}` s
        ON t.{pk} = s.{pk}
        WHEN MATCHED THEN
          UPDATE SET {update_sets}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values});
        """)

    statements.append("COMMIT TRANSACTION;")

    script = "\n".join(statements)

    logger.info("Applying diff DML for %s (atomic transaction)...", dataset_name)
    client = _get_bq_client()
    job = client.query(script)
    job.result()
    logger.info("Diff DML committed for %s", dataset_name)


def _drop_temp_tables(dataset_name):
    """Drop temporary tables created during diff processing."""
    client = _get_bq_client()
    for suffix in ["updates", "deletes"]:
        table_id = _temp_table(dataset_name, suffix)
        client.delete_table(table_id, not_found_ok=True)
        logger.info("Dropped temp table %s", table_id)


def apply_diff(release_id, dataset_name, diff):
    """Apply a single diff (one release step) for a dataset.

    Stages both delete and update temp tables before applying any DML,
    so the main table is never left in a partially applied state.

    Args:
        release_id: Target release ID (for GCS paths).
        dataset_name: "papers", "citations", or "authors".
        diff: Dict with update_files and delete_files lists.

    Returns:
        Dict with deleted and upserted counts.
    """
    update_files = diff.get("update_files", [])
    delete_files = diff.get("delete_files", [])
    result = {"deleted": 0, "upserted": 0}

    try:
        # Phase 1: Download and stage both temp tables (no DML yet)
        delete_table = None
        update_table = None

        if delete_files:
            dl = download_dataset(release_id, f"{dataset_name}_diff_deletes", delete_files)
            if dl["failed"] > 0:
                raise RuntimeError(f"{dl['failed']} delete file downloads failed")
            delete_table = _load_temp_table(
                release_id, dataset_name, "deletes", DELETE_SCHEMAS[dataset_name]
            )

        if update_files:
            dl = download_dataset(release_id, f"{dataset_name}_diff_updates", update_files)
            if dl["failed"] > 0:
                raise RuntimeError(f"{dl['failed']} update file downloads failed")
            update_table = _load_temp_table(
                release_id, dataset_name, "updates", DATASET_SCHEMAS[dataset_name]
            )

        # Record staged row counts for telemetry before DML
        client = _get_bq_client()
        if delete_table:
            result["deleted"] = client.get_table(delete_table).num_rows
        if update_table:
            result["upserted"] = client.get_table(update_table).num_rows

        # Phase 2: Apply delete + upsert atomically in a single transaction
        if delete_table or update_table:
            _apply_diff_dml(dataset_name, delete_table, update_table)

    finally:
        _drop_temp_tables(dataset_name)

    return result
