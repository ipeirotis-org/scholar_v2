-- Migration: recreate raw data tables with clustering by document_id.
--
-- BigQuery does not support adding clustering to existing tables via ALTER TABLE.
-- To cluster existing tables, they must be recreated with a CTAS.
--
-- WARNING: This is a destructive operation that recreates the tables.
-- Ensure no concurrent writes are happening. Run during maintenance windows.
--
-- The author and pub tables use WRITE_APPEND, so document_id is the natural
-- cluster key for dedup queries. Clustering by document_id allows BigQuery
-- to prune blocks when filtering by document_id, reducing full-table scan
-- costs during dedup table materialization.
--
-- Note: The downstream views now read from author_latest_table / pub_latest_table
-- (materialized, clustered by document_id), so this migration primarily benefits
-- the daily dedup materialization job itself.
--
-- Usage (one-time, during maintenance):
--   bq query --project_id=scholar-version2 --use_legacy_sql=false < bigquery/cluster_raw_tables.sql

-- Step 1: Recreate author table with clustering
CREATE OR REPLACE TABLE `scholar-version2.scholar_raw_data.author`
CLUSTER BY document_id
AS SELECT * FROM `scholar-version2.scholar_raw_data.author`;

-- Step 2: Recreate pub table with clustering
CREATE OR REPLACE TABLE `scholar-version2.scholar_raw_data.pub`
CLUSTER BY document_id
AS SELECT * FROM `scholar-version2.scholar_raw_data.pub`;
