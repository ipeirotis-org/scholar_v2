-- PiP-AUC score percentile distribution table.
--
-- Stores distinct (benchmark, year_of_first_pub, pip_auc_score) → percentile rows.
-- At query time, stats_author_pip_scores_current does a floor lookup against
-- this table instead of running PERCENT_RANK() over all authors.
--
-- Benchmarks:
--   'all_authors'    — full S2 population
--   'active_authors' — authors with hindex >= 3 AND total_publications >= 3
--
-- Depends on: dist_publication_citations and dist_author_metrics being current,
-- so that the underlying stats_publication_current and stats_author_current
-- views are fast when computing pip scores for all authors.
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_pip_auc_scores`
CLUSTER BY benchmark, year_of_first_pub
AS
WITH
  -- Reuse the pip computation logic from stats_author_pip_scores_current
  -- (without the final PERCENT_RANK — that's what this table provides)
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
  -- Define active authors for benchmark filtering
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
  pip_auc_score,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY pip_auc_score ASC) AS percentile
FROM AllScores
UNION ALL
-- active_authors benchmark
SELECT DISTINCT
  'active_authors' AS benchmark,
  year_of_first_pub,
  pip_auc_score,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY pip_auc_score ASC) AS percentile
FROM AllScores
WHERE scholar_id IN (SELECT scholar_id FROM ActiveAuthors);
