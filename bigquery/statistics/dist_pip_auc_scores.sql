-- PiP-AUC score percentile distribution table (approximate quantiles).
--
-- Builds quantile breakpoints from stats_author_pip_scores_current (the raw
-- PiP-AUC scores table, WITHOUT percentile column). This avoids a circular
-- dependency: ranked_author_pip_scores_current needs dist_pip_auc_scores to
-- compute pip_auc_score_percentile, so dist cannot read from ranked.
--
-- Stores 1000 quantile breakpoints per (benchmark, year_of_first_pub).
-- At query time, ranked_author_pip_scores_current does a floor lookup.
--
-- Benchmarks:
--   'all_authors'    — full S2 population
--   'active_authors' — authors with hindex >= 3 AND total_publications >= 3
--
-- PREREQUISITE: stats_author_pip_scores_current view (or _table) must exist.
-- During pipeline materialization, view references are substituted with
-- _table references for performance.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_pip_auc_scores`
CLUSTER BY benchmark, year_of_first_pub
AS
WITH
  ActiveAuthors AS (
    SELECT a.authorid AS scholar_id
    FROM `scholar-version2.s2_data.authors` a
    JOIN `scholar-version2.s2_data.author_paper_stats` ps ON a.authorid = ps.authorid
    WHERE a.hindex >= 3 AND COALESCE(ps.total_publications, 0) >= 3
  )
SELECT DISTINCT * FROM (
  -- all_authors benchmark
  SELECT 'all_authors' AS benchmark, year_of_first_pub,
         value AS pip_auc_score, offset / 1000.0 AS percentile
  FROM (
    SELECT year_of_first_pub, APPROX_QUANTILES(pip_auc_score, 1000) AS quantiles
    FROM `scholar-version2.statistics.stats_author_pip_scores_current`
    GROUP BY year_of_first_pub
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
  UNION ALL
  -- active_authors benchmark
  SELECT 'active_authors' AS benchmark, year_of_first_pub,
         value AS pip_auc_score, offset / 1000.0 AS percentile
  FROM (
    SELECT year_of_first_pub, APPROX_QUANTILES(pip_auc_score, 1000) AS quantiles
    FROM `scholar-version2.statistics.stats_author_pip_scores_current`
    WHERE scholar_id IN (SELECT scholar_id FROM ActiveAuthors)
    GROUP BY year_of_first_pub
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
);
