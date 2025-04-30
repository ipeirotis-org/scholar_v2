CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_author_pip_scores_current` AS
WITH
  RankedPublications AS (
  SELECT
    scholar_id,
    author_pub_id,
    num_citations_percentile,
    num_papers_percentile,
    LAG(num_citations_percentile) OVER(PARTITION BY scholar_id ORDER BY num_papers_percentile) AS prev_num_citations_percentile,
    LAG(num_papers_percentile) OVER(PARTITION BY scholar_id ORDER BY num_papers_percentile) AS prev_num_papers_percentile
  FROM
    `scholar-version2.statistics.stats_author_publication_pip_inputs_current`
   ),
  TrapezoidAreas AS (
  SELECT
    scholar_id,
    (num_papers_percentile - prev_num_papers_percentile) * (num_citations_percentile + prev_num_citations_percentile) / 2 AS area
  FROM
    RankedPublications
  WHERE
    -- Need to check prev_num_papers_percentile as well in case the first point has null prev_num_citations_percentile
    prev_num_citations_percentile IS NOT NULL AND prev_num_papers_percentile IS NOT NULL
   ),
  AUC AS (
  SELECT
    scholar_id,
    ROUND(SUM(area),4) AS pip_auc_score
  FROM
    TrapezoidAreas
  GROUP BY
    scholar_id
  )
-- Final SELECT joining AUC score with author's first pub year for percentile calculation
SELECT
  A.scholar_id,
  AuthStats.year_of_first_pub, -- Get year_of_first_pub from stats_author_current
  pip_auc_score,
  PERCENT_RANK() OVER (PARTITION BY AuthStats.year_of_first_pub ORDER BY pip_auc_score ASC) AS pip_auc_score_percentile
FROM
  AUC A -- Alias AUC CTE as A
JOIN
  -- This join is correct: Use stats_author_current to get year_of_first_pub
  `scholar-version2.statistics.stats_author_current` AuthStats
ON
  A.scholar_id = AuthStats.scholar_id
ORDER BY
  pip_auc_score DESC;