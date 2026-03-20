CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_author_pip_scores_temporal` AS
-- Level 7: Temporal PiP-AUC scores enriched with percentile.
-- Joins stats_author_pip_scores_temporal_view (L5) against
-- dist_pip_auc_scores_temporal (L6) using floor lookups.
WITH
  score_percentile AS (
    SELECT
      a.scholar_id,
      a.state_year,
      MAX(d.percentile) AS pip_auc_score_percentile
    FROM `scholar-version2.statistics.stats_author_pip_scores_temporal_view` a
    LEFT JOIN `scholar-version2.statistics.dist_pip_auc_scores_temporal` d
      ON d.year_of_first_pub = a.year_of_first_pub
     AND d.state_year = a.state_year
     AND d.pip_auc_score <= a.pip_auc_score
    GROUP BY a.scholar_id, a.state_year
  )
SELECT
  a.scholar_id,
  a.state_year,
  a.year_of_first_pub,
  a.pip_auc_score,
  COALESCE(s.pip_auc_score_percentile, 0.0) AS pip_auc_score_percentile
FROM `scholar-version2.statistics.stats_author_pip_scores_temporal_view` a
LEFT JOIN score_percentile s ON s.scholar_id = a.scholar_id AND s.state_year = a.state_year;
