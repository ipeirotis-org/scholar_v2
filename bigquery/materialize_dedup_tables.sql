-- Materialize deduplicated author and publication tables.
--
-- The raw tables (author, pub) accumulate every version via WRITE_APPEND.
-- These materialized tables deduplicate to the latest record per document_id,
-- eliminating the need for downstream views to re-run ROW_NUMBER() dedup
-- on every query.
--
-- Clustered by document_id for efficient single-author/publication lookups.
-- Refreshed daily before snapshot materialization.
--
-- Usage:
--   bq query --project_id=scholar-version2 --use_legacy_sql=false < bigquery/materialize_dedup_tables.sql

-- Author dedup table: one row per author (most recent by timestamp).
CREATE OR REPLACE TABLE `scholar-version2.scholar_raw_data.author_latest_table`
CLUSTER BY document_id
AS
SELECT document_id, timestamp, data
FROM (
  SELECT
    document_id,
    timestamp,
    data,
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY timestamp DESC) AS rn
  FROM `scholar-version2.scholar_raw_data.author`
)
WHERE rn = 1;

-- Publication dedup table: one row per publication (most recent by timestamp).
CREATE OR REPLACE TABLE `scholar-version2.scholar_raw_data.pub_latest_table`
CLUSTER BY document_id
AS
SELECT document_id, timestamp, data
FROM (
  SELECT
    document_id,
    timestamp,
    data,
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY timestamp DESC) AS rn
  FROM `scholar-version2.scholar_raw_data.pub`
)
WHERE rn = 1;
