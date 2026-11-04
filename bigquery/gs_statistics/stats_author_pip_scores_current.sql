CREATE OR REPLACE VIEW `scholar-version2.gs_statistics.stats_author_pip_scores_current` AS
-- Level 4: Raw PiP-AUC score per author — no percentile column.
-- Computes PiP-AUC as the area under the PiP step curve: sum over works k (in percentile order) of
-- citation percentile y_k times (x_k - x_(k-1)), where x_k is the cohort percentile of a k-paper count.
-- Equivalent to the integral over thresholds t of F(number of works with percentile >= t).
-- Depends on: stats_author_publication_pip_inputs_current (L3), stats_author_current (L2).
-- The pip_auc_score_percentile is added by ranked_author_pip_scores_current (L5)
-- via dist_pip_auc_scores (L4).
WITH
  RankedPublications AS (
    SELECT
      scholar_id,
      author_pub_id,
      num_citations_percentile,
      num_papers_percentile,
      -- Width of work k's step: x_k - x_(k-1), x_0 = 0, in rank order (ties in x get width 0)
      COALESCE(LAG(num_papers_percentile) OVER(PARTITION BY scholar_id ORDER BY publication_rank), 0) AS prev_num_papers_percentile
    FROM `scholar-version2.gs_statistics.stats_author_publication_pip_inputs_current`
  ),
  StepAreas AS (
    SELECT
      scholar_id,
      (num_papers_percentile - prev_num_papers_percentile) * num_citations_percentile AS area
    FROM RankedPublications
  ),
  AUC AS (
    SELECT
      scholar_id,
      ROUND(SUM(area), 4) AS pip_auc_score
    FROM StepAreas
    GROUP BY scholar_id
  )
SELECT
  A.scholar_id,
  AuthStats.year_of_first_pub,
  A.pip_auc_score
FROM AUC A
JOIN `scholar-version2.gs_statistics.stats_author_current` AuthStats
  ON A.scholar_id = AuthStats.scholar_id;
