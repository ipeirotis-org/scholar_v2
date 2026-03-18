-- Materialize expensive percentile views into regular tables.
--
-- This script replaces live PERCENT_RANK() window-function views with
-- pre-computed tables. Run it periodically (daily is sufficient since
-- citation data changes slowly).
--
-- Execution order matters: each table depends on the previous one.
-- The views remain as the source-of-truth definitions; these tables
-- are cheap-to-query snapshots.
--
-- Usage:
--   bq query --project_id=scholar-version2 --use_legacy_sql=false < bigquery/statistics/materialize_stats.sql

-- Step 0: Drop legacy materialized view (one-time migration cleanup)
DROP MATERIALIZED VIEW IF EXISTS `scholar-version2.statistics.stats_author_metrics_temporal`;

-- Step 1: Publication citation percentiles
-- Foundation table — all other percentile tables depend on this.
CREATE OR REPLACE TABLE `scholar-version2.statistics.stats_publication_current_table`
CLUSTER BY scholar_id, pub_year
AS SELECT * FROM `scholar-version2.statistics.stats_publication_current`;

-- Step 2: Author current metrics + percentiles
CREATE OR REPLACE TABLE `scholar-version2.statistics.stats_author_current_table`
CLUSTER BY scholar_id, year_of_first_pub
AS SELECT * FROM `scholar-version2.statistics.stats_author_current`;

-- Step 3: PiP interpolation inputs
CREATE OR REPLACE TABLE `scholar-version2.statistics.stats_author_publication_pip_inputs_current_table`
CLUSTER BY scholar_id
AS SELECT * FROM `scholar-version2.statistics.stats_author_publication_pip_inputs_current`;

-- Step 4: PiP-AUC scores + percentiles
CREATE OR REPLACE TABLE `scholar-version2.statistics.stats_author_pip_scores_current_table`
CLUSTER BY scholar_id
AS SELECT * FROM `scholar-version2.statistics.stats_author_pip_scores_current`;

-- Step 5: Temporal author metrics + percentiles
CREATE OR REPLACE TABLE `scholar-version2.statistics.stats_author_metrics_temporal`
CLUSTER BY scholar_id, state_year
AS SELECT * FROM `scholar-version2.statistics.stats_author_metrics_temporal_view`;
