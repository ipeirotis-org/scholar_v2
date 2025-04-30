CREATE OR REPLACE VIEW `scholar-version2.statistics.author_pip_scores` AS
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
    `scholar-version2.statistics.author_pub_stats` ),
  TrapezoidAreas AS (
  SELECT
    scholar_id,
    (num_papers_percentile - prev_num_papers_percentile) * (num_citations_percentile + prev_num_citations_percentile) / 2 AS area
  FROM
    RankedPublications
  WHERE
    prev_num_citations_percentile IS NOT NULL ),
  AUC AS (
  SELECT
    scholar_id,
    ROUND(SUM(area),4) AS pip_auc_score
  FROM
    TrapezoidAreas
  GROUP BY
    scholar_id )
SELECT
  A.scholar_id,
  year_of_first_pub,
  pip_auc_score,
  PERCENT_RANK() OVER (PARTITION BY A.year_of_first_pub ORDER BY pip_auc_score) AS pip_auc_score_percentile
FROM
  AUC
JOIN
  `scholar-version2.statistics.author_stats` A
ON
  AUC.scholar_id = A.scholar_id
ORDER BY
  pip_auc_score DESC;