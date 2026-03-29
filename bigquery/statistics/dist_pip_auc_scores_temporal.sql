-- Level 6: PiP-AUC score percentile distribution for temporal data (approximate quantiles).
--
-- Stores 1000 quantile breakpoints per (benchmark, year_of_first_pub, state_year).
-- Each row maps a pip_auc_score to its percentile (0.000 to 1.000).
--
-- Only active_authors benchmark for temporal (all_authors too expensive and not used).
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by ranked_author_pip_scores_temporal for fast floor lookups.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_pip_auc_scores_temporal`
CLUSTER BY benchmark, year_of_first_pub, state_year
AS
WITH
  TemporalScores AS (
    SELECT scholar_id, year_of_first_pub, state_year, pip_auc_score
    FROM `scholar-version2.statistics.stats_author_pip_scores_temporal_view`
    WHERE year_of_first_pub IS NOT NULL
  ),
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
    FROM TemporalScores
    WHERE scholar_id IN (SELECT scholar_id FROM ActiveAuthors)
    GROUP BY year_of_first_pub, state_year
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
);
