CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_author_pip_scores_current` AS
-- Level 5: PiP-AUC scores enriched with percentile.
-- Uses RANGE_BUCKET + pre-aggregated arrays for O(log n) floor lookup.
-- Percentiles are computed against the 'active_authors' benchmark.
-- Reads from stats_author_pip_scores_current (view). During materialization,
-- the pipeline substitutes _table references for performance.
WITH
  DistArrays AS (
    SELECT
      year_of_first_pub,
      ARRAY_AGG(pip_auc_score ORDER BY pip_auc_score) AS values_arr,
      ARRAY_AGG(percentile ORDER BY pip_auc_score) AS pcts_arr
    FROM `scholar-version2.statistics.dist_pip_auc_scores`
    WHERE benchmark = 'active_authors'
    GROUP BY year_of_first_pub
  )
SELECT
  a.scholar_id,
  a.year_of_first_pub,
  a.pip_auc_score,
  COALESCE(
    da.pcts_arr[SAFE_ORDINAL(RANGE_BUCKET(a.pip_auc_score, da.values_arr))],
    0.0
  ) AS pip_auc_score_percentile
FROM `scholar-version2.statistics.stats_author_pip_scores_current` a
JOIN DistArrays da ON da.year_of_first_pub = a.year_of_first_pub;
