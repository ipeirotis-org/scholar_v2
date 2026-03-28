-- Level 6: PiP-AUC score percentile distribution for temporal data.
--
-- Stores the distinct (benchmark, year_of_first_pub, state_year, pip_auc_score) → percentile mapping.
-- Partitioned by (benchmark, year_of_first_pub, state_year) to compare authors against their
-- benchmark peers at the same point in historical time.
--
-- Benchmarks:
--   'all_authors'    — full S2 population
--   'active_authors' — authors with hindex >= 3 AND total_publications >= 3 (current state)
--
-- PERCENT_RANK() is computed over ALL rows (preserving frequency), then DISTINCT
-- collapses tied values since they all receive the same rank.
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by ranked_author_pip_scores_temporal to do fast floor lookups.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_pip_auc_scores_temporal`
CLUSTER BY benchmark, year_of_first_pub, state_year
AS
WITH
  TemporalScores AS (
    SELECT scholar_id, year_of_first_pub, state_year, pip_auc_score
    FROM `scholar-version2.statistics.stats_author_pip_scores_temporal_view`
    WHERE year_of_first_pub IS NOT NULL
  ),
  -- Define active authors based on current metrics
  ActiveAuthors AS (
    SELECT a.authorid AS scholar_id
    FROM `scholar-version2.s2_data.authors` a
    JOIN `scholar-version2.s2_data.author_paper_stats` ps ON a.authorid = ps.authorid
    WHERE a.hindex >= 3 AND COALESCE(ps.total_publications, 0) >= 3
  )
-- all_authors benchmark
SELECT DISTINCT
  'all_authors' AS benchmark,
  year_of_first_pub,
  state_year,
  pip_auc_score,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY pip_auc_score ASC) AS percentile
FROM TemporalScores
UNION ALL
-- active_authors benchmark
SELECT DISTINCT
  'active_authors' AS benchmark,
  year_of_first_pub,
  state_year,
  pip_auc_score,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY pip_auc_score ASC) AS percentile
FROM TemporalScores
WHERE scholar_id IN (SELECT scholar_id FROM ActiveAuthors);
