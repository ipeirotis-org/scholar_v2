-- Deduplication views for scholar_raw_data tables.
--
-- The raw tables (author, pub) use WRITE_APPEND, accumulating every historical
-- version of each document. These _latest views deduplicate to the most recent
-- record per document using ROW_NUMBER().
--
-- Schema: Each row has {document_id, timestamp, data} where data is a JSON
-- string containing the full document wrapped under a "data" key.
--
-- These views are the ONLY source that downstream analytics views should read.
-- They replace the legacy firestore_export.scholar_raw_*_raw_latest tables.
--
-- Deploy:
--   bq query --project_id=scholar-version2 --use_legacy_sql=false < v3/ingestion/dedup_views.sql

-- Author dedup view: one row per author (most recent by timestamp).
-- Two-level dedup: first by document_id, then by scholar_id.
-- Needed because legacy records used document_id = scholar_id while newer
-- records use document_id = scholar_id.json.
CREATE OR REPLACE VIEW `scholar-version2.scholar_raw_data.author_latest` AS
SELECT
  -- Normalize document_id: strip .json suffix from legacy records
  CASE
    WHEN ENDS_WITH(document_id, '.json')
    THEN SUBSTR(document_id, 1, LENGTH(document_id) - 5)
    ELSE document_id
  END AS document_id,
  timestamp,
  data
FROM (
  SELECT
    document_id,
    timestamp,
    data,
    ROW_NUMBER() OVER (
      PARTITION BY JSON_EXTRACT_SCALAR(data, '$.data.scholar_id')
      ORDER BY timestamp DESC
    ) AS rn
  FROM (
    SELECT
      document_id,
      timestamp,
      data,
      ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY timestamp DESC) AS rn_doc
    FROM `scholar-version2.scholar_raw_data.author`
  )
  WHERE rn_doc = 1
)
WHERE rn = 1;

-- Publication dedup view: one row per publication (most recent by timestamp).
-- Two-level dedup: first by document_id, then by author_pub_id.
-- Needed because legacy records used document_id = scholar_id:pub_id while
-- newer records use document_id = scholar_id_pub_id.json.
CREATE OR REPLACE VIEW `scholar-version2.scholar_raw_data.pub_latest` AS
SELECT
  -- Normalize document_id: strip .json suffix from legacy records
  CASE
    WHEN ENDS_WITH(document_id, '.json')
    THEN SUBSTR(document_id, 1, LENGTH(document_id) - 5)
    ELSE document_id
  END AS document_id,
  timestamp,
  data
FROM (
  SELECT
    document_id,
    timestamp,
    data,
    ROW_NUMBER() OVER (
      PARTITION BY JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id')
      ORDER BY timestamp DESC
    ) AS rn
  FROM (
    SELECT
      document_id,
      timestamp,
      data,
      ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY timestamp DESC) AS rn_doc
    FROM `scholar-version2.scholar_raw_data.pub`
  )
  WHERE rn_doc = 1
)
WHERE rn = 1;
