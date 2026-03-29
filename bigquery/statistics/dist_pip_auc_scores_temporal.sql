-- Level 6: PiP-AUC score percentile distribution for temporal data (approximate quantiles).
--
-- Builds quantile breakpoints from the pre-computed ranked_author_pip_scores_temporal_table
-- which already has pip_auc_score for every author × state_year. No AUC recomputation needed.
--
-- Only active_authors benchmark for temporal.
--
-- PREREQUISITE: ranked_author_pip_scores_temporal_table must be materialized first
-- (by bigquery-materialize.yml daily job).
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_pip_auc_scores_temporal`
CLUSTER BY benchmark, year_of_first_pub, state_year
AS
WITH
  ActiveAuthors AS (
    SELECT a.authorid AS scholar_id
    FROM `scholar-version2.s2_data.authors` a
    JOIN `scholar-version2.s2_data.author_paper_stats` ps ON a.authorid = ps.authorid
    WHERE a.hindex >= 3 AND COALESCE(ps.total_publications, 0) >= 3
  )
SELECT DISTINCT * FROM (
  SELECT 'active_authors' AS benchmark, year_of_first_pub, state_year,
         value AS pip_auc_score, offset / 1000.0 AS percentile
  FROM (
    SELECT year_of_first_pub, state_year,
           APPROX_QUANTILES(pip_auc_score, 1000) AS quantiles
    FROM `scholar-version2.statistics.ranked_author_pip_scores_temporal_table`
    WHERE scholar_id IN (SELECT scholar_id FROM ActiveAuthors)
    GROUP BY year_of_first_pub, state_year
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
);
