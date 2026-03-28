CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_author_pip_scores_temporal` AS
-- Level 7: Temporal PiP-AUC scores enriched with percentile.
-- Uses scalar subquery against dist_pip_auc_scores_temporal instead of range join.
-- Percentiles are computed against the 'active_authors' benchmark.
SELECT
  a.scholar_id,
  a.state_year,
  a.year_of_first_pub,
  a.pip_auc_score,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_pip_auc_scores_temporal` d
     WHERE d.benchmark = 'active_authors'
       AND d.year_of_first_pub = a.year_of_first_pub AND d.state_year = a.state_year
       AND d.pip_auc_score <= a.pip_auc_score),
    0.0
  ) AS pip_auc_score_percentile
FROM `scholar-version2.statistics.stats_author_pip_scores_temporal_view` a;
