-- Level 6: PiP-AUC score percentile distribution for temporal data.
--
-- Stores the distinct (year_of_first_pub, state_year, pip_auc_score) → percentile mapping.
-- Partitioned by (year_of_first_pub, state_year) to compare authors against their peers
-- at the same point in historical time.
--
-- PERCENT_RANK() is computed over ALL rows (preserving frequency), then DISTINCT
-- collapses tied values since they all receive the same rank.
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by ranked_author_pip_scores_temporal to do fast floor lookups.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_pip_auc_scores_temporal`
CLUSTER BY year_of_first_pub, state_year
AS
SELECT DISTINCT
  year_of_first_pub,
  state_year,
  pip_auc_score,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY pip_auc_score ASC) AS percentile
FROM `scholar-version2.statistics.stats_author_pip_scores_temporal_view`
WHERE year_of_first_pub IS NOT NULL;
