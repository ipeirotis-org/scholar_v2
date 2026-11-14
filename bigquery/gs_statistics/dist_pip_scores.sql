-- PiP score (tau = 0.5) percentile distribution table (approximate quantiles).
--
-- Builds quantile breakpoints of pip_score from stats_author_pip_scores_current
-- (raw scores, without percentile columns) to avoid circular dependencies with
-- ranked_author_pip_scores_current.
--
-- Stores 1000 quantile breakpoints per (benchmark, year_of_first_pub).
-- At query time, ranked_author_pip_scores_current does a floor lookup.
--
-- Benchmark:
--   'active_authors' — authors with hindex >= 3 AND total_publications >= 3
--
-- Rebuilt weekly by gs_crawler (CRAWL_MODE=materialize).

CREATE OR REPLACE TABLE `scholar-version2.gs_statistics.dist_pip_scores`
CLUSTER BY benchmark, year_of_first_pub
AS
WITH
  ActiveAuthors AS (
    SELECT scholar_id
    FROM `scholar-version2.gs_statistics.stats_author_current`
    WHERE hindex >= 3 AND total_publications >= 3
  )
SELECT DISTINCT * FROM (
  SELECT 'active_authors' AS benchmark, year_of_first_pub,
         value AS pip_score, offset / 1000.0 AS percentile
  FROM (
    SELECT year_of_first_pub, APPROX_QUANTILES(pip_score, 1000) AS quantiles
    FROM `scholar-version2.gs_statistics.stats_author_pip_scores_current`
    WHERE scholar_id IN (SELECT scholar_id FROM ActiveAuthors)
      AND pip_score IS NOT NULL
    GROUP BY year_of_first_pub
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
);
