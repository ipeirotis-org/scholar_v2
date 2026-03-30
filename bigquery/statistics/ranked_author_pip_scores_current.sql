CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_author_pip_scores_current` AS
-- Level 5: PiP-AUC scores enriched with percentile.
-- Uses scalar subquery against dist_pip_auc_scores instead of range join.
-- Percentiles are computed against the 'active_authors' benchmark.
-- Reads from stats_author_pip_scores_current_table (materialized in Level 4)
-- to avoid re-executing the PiP-AUC computation.
SELECT
  a.scholar_id,
  a.year_of_first_pub,
  a.pip_auc_score,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_pip_auc_scores` d
     WHERE d.benchmark = 'active_authors'
       AND d.year_of_first_pub = a.year_of_first_pub AND d.pip_auc_score <= a.pip_auc_score),
    0.0
  ) AS pip_auc_score_percentile
FROM `scholar-version2.statistics.stats_author_pip_scores_current_table` a;
