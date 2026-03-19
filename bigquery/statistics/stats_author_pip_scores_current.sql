CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_author_pip_scores_current` AS
-- Tier 1: Raw PiP-AUC score per author — no percentile column.
-- Computes PiP-AUC score via trapezoidal integration of the PiP chart.
-- The pip_auc_score_percentile is added by ranked_author_pip_scores_current (Tier 3)
-- via dist_pip_auc_scores.
WITH
  RankedPublications AS (
    SELECT
      scholar_id,
      author_pub_id,
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
  )
SELECT
  A.scholar_id,
  AuthStats.year_of_first_pub,
  A.pip_auc_score
FROM AUC A
JOIN `scholar-version2.statistics.stats_author_current` AuthStats
  ON A.scholar_id = AuthStats.scholar_id;
