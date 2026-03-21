-- Materialize deduplicated and PARSED author and publication tables.
--
-- The raw tables (author, pub) accumulate every version via WRITE_APPEND
-- and store data as JSON strings. These materialized tables:
--   1. Deduplicate to the latest record per document_id
--   2. Parse JSON into typed columns for efficient downstream queries
--
-- This eliminates JSON_EXTRACT_SCALAR overhead in ALL downstream views,
-- enabling proper clustering on typed columns (e.g., scholar_id).
--
-- Refreshed daily before snapshot materialization.
--
-- Usage:
--   bq query --project_id=scholar-version2 --use_legacy_sql=false < bigquery/materialize_dedup_tables.sql

-- Author dedup table: one row per author (most recent by timestamp).
-- Typed columns extracted from JSON for direct use by downstream views.
CREATE OR REPLACE TABLE `scholar-version2.scholar_raw_data.author_latest_table`
CLUSTER BY scholar_id
AS
WITH deduped AS (
  SELECT
    document_id,
    timestamp,
    data,
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY timestamp DESC) AS rn
  FROM `scholar-version2.scholar_raw_data.author`
)
SELECT
  document_id,
  timestamp,
  JSON_EXTRACT_SCALAR(data, '$.data.scholar_id') AS scholar_id,
  JSON_EXTRACT_SCALAR(data, '$.data.name') AS name,
  JSON_EXTRACT_SCALAR(data, '$.data.affiliation') AS affiliation,
  JSON_EXTRACT_SCALAR(data, '$.data.email_domain') AS email_domain,
  CAST(JSON_EXTRACT_SCALAR(data, '$.data.hindex') AS INT64) AS hindex,
  CAST(JSON_EXTRACT_SCALAR(data, '$.data.hindex5y') AS INT64) AS hindex5y,
  CAST(JSON_EXTRACT_SCALAR(data, '$.data.citedby') AS INT64) AS citedby,
  CAST(JSON_EXTRACT_SCALAR(data, '$.data.citedby5y') AS INT64) AS citedby5y,
  CAST(JSON_EXTRACT_SCALAR(data, '$.data.i10index') AS INT64) AS i10index,
  CAST(JSON_EXTRACT_SCALAR(data, '$.data.i10index5y') AS INT64) AS i10index5y,
  JSON_EXTRACT_ARRAY(data, '$.data.publications') AS publications,
  JSON_EXTRACT_ARRAY(data, '$.data.coauthors') AS coauthors,
  data
FROM deduped
WHERE rn = 1;

-- Publication dedup table: one row per publication (most recent by timestamp).
-- Typed columns extracted from JSON for direct use by downstream views.
CREATE OR REPLACE TABLE `scholar-version2.scholar_raw_data.pub_latest_table`
CLUSTER BY author_pub_id
AS
WITH deduped AS (
  SELECT
    document_id,
    timestamp,
    data,
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY timestamp DESC) AS rn
  FROM `scholar-version2.scholar_raw_data.pub`
)
SELECT
  document_id,
  timestamp,
  JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id') AS author_pub_id,
  SPLIT(JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id'), ':')[SAFE_OFFSET(0)] AS scholar_id,
  CAST(JSON_EXTRACT_SCALAR(data, '$.data.bib.pub_year') AS INT64) AS pub_year,
  JSON_EXTRACT_SCALAR(data, '$.data.bib.title') AS title,
  JSON_EXTRACT_SCALAR(data, '$.data.bib.author') AS author,
  CAST(JSON_EXTRACT_SCALAR(data, '$.data.num_citations') AS INT64) AS num_citations,
  JSON_QUERY(data, '$.data.cites_per_year') AS cites_per_year,
  data
FROM deduped
WHERE rn = 1;
