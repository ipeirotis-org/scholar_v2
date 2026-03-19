CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_author_pip_scores_current` AS
-- Tier 3: PiP-AUC scores enriched with percentile.
-- Joins stats_author_pip_scores_current (Tier 1) against dist_pip_auc_scores (Tier 2)
-- using a floor lookup: MAX(percentile WHERE dist_score <= actual_score).
WITH
  score_percentile AS (
    SELECT
      a.scholar_id,
      MAX(d.percentile) AS pip_auc_score_percentile
    FROM `scholar-version2.statistics.stats_author_pip_scores_current` a
    LEFT JOIN `scholar-version2.statistics.dist_pip_auc_scores` d
      ON d.year_of_first_pub = a.year_of_first_pub
     AND d.pip_auc_score <= a.pip_auc_score
    GROUP BY a.scholar_id
  )
SELECT
  a.scholar_id,
  a.year_of_first_pub,
  a.pip_auc_score,
  COALESCE(s.pip_auc_score_percentile, 0.0) AS pip_auc_score_percentile
FROM `scholar-version2.statistics.stats_author_pip_scores_current` a
LEFT JOIN score_percentile s ON s.scholar_id = a.scholar_id
ORDER BY a.pip_auc_score DESC;
