-- PiP-AUC score percentile distribution table (approximate quantiles).
--
-- Stores 1000 quantile breakpoints per (benchmark, year_of_first_pub).
-- Each row maps a pip_auc_score to its percentile (0.000 to 1.000).
--
-- At query time, ranked_author_pip_scores_current does a floor lookup:
--   MAX(percentile) WHERE pip_auc_score <= author's_score
--
-- Benchmarks:
--   'all_authors'    — full S2 population
--   'active_authors' — authors with hindex >= 3 AND total_publications >= 3
--
-- Depends on: dist_publication_citations and dist_author_metrics being current.
-- Refreshed quarterly by bigquery-materialize-distributions.yml.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_pip_auc_scores`
CLUSTER BY benchmark, year_of_first_pub
AS
WITH
  RankedPublications AS (
    SELECT
      scholar_id,
      num_citations_percentile,
      num_papers_percentile,
      COALESCE(LAG(num_citations_percentile) OVER(PARTITION BY scholar_id ORDER BY num_papers_percentile), num_citations_percentile) AS prev_num_citations_percentile,
      COALESCE(LAG(num_papers_percentile)    OVER(PARTITION BY scholar_id ORDER BY num_papers_percentile), 0)                       AS prev_num_papers_percentile
    FROM `scholar-version2.statistics.stats_author_publication_pip_inputs_current`
  ),
  TrapezoidAreas AS (
    SELECT
      scholar_id,
      (num_papers_percentile - prev_num_papers_percentile) * (num_citations_percentile + prev_num_citations_percentile) / 2 AS area
    FROM RankedPublications
  ),
  AUC AS (
    SELECT
      scholar_id,
      ROUND(SUM(area), 4) AS pip_auc_score
    FROM TrapezoidAreas
    GROUP BY scholar_id
  ),
  AllScores AS (
    SELECT
      A.scholar_id,
      AuthStats.year_of_first_pub,
      A.pip_auc_score
    FROM AUC A
    JOIN `scholar-version2.statistics.stats_author_current` AuthStats ON A.scholar_id = AuthStats.scholar_id
  ),
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
    FROM AllScores GROUP BY year_of_first_pub
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
  UNION ALL
  -- active_authors benchmark
  SELECT 'active_authors' AS benchmark, year_of_first_pub,
         value AS pip_auc_score, offset / 1000.0 AS percentile
  FROM (
    SELECT year_of_first_pub, APPROX_QUANTILES(pip_auc_score, 1000) AS quantiles
    FROM AllScores WHERE scholar_id IN (SELECT scholar_id FROM ActiveAuthors)
    GROUP BY year_of_first_pub
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
);
