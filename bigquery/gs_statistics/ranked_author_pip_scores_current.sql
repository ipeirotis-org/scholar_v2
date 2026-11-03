CREATE OR REPLACE VIEW `scholar-version2.gs_statistics.ranked_author_pip_scores_current` AS
-- Level 5: PiP-AUC scores enriched with percentile.
-- Uses RANGE_BUCKET + pre-aggregated arrays for O(log n) floor lookup.
-- Percentiles are computed against the 'active_authors' benchmark.
-- Reads from stats_author_pip_scores_current (view). During materialization,
-- the pipeline substitutes _table references for performance.
--
-- Field columns (GS only): the author's field group (author_fields) and PiP-AUC
-- percentile among field peers (dist_pip_auc_scores_field: active authors in the
-- group whose career started near the author's; the window and its peer count
-- say who the comparison is with). They live here, not in a separate view, so
-- the cache layer's author lookup reads stats_author_pip_scores_current once
-- (a separate view added ~3 s per lookup).
-- field_pip_auc_score_percentile is the share of peers with a lower score, over
-- peer_count - 1 (PERCENT_RANK when the author is among the peers; capped at 1.0
-- for authors outside the benchmark who beat every peer). It is NULL when the
-- author has no field or the window holds fewer than 30 peers.
WITH
  DistArrays AS (
    SELECT
      year_of_first_pub,
      ARRAY_AGG(pip_auc_score ORDER BY pip_auc_score, percentile) AS values_arr,
      ARRAY_AGG(percentile ORDER BY pip_auc_score, percentile) AS pcts_arr
    FROM `scholar-version2.gs_statistics.dist_pip_auc_scores`
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
  ) AS pip_auc_score_percentile,
  f.field_group,
  f.field_source,
  df.cohort_start AS field_cohort_start,
  df.cohort_end AS field_cohort_end,
  df.peer_count AS field_peer_count,
  IF(a.pip_auc_score IS NOT NULL AND df.peer_count >= 30,
     LEAST(1.0, (SELECT COUNTIF(s < a.pip_auc_score) FROM UNNEST(df.scores) AS s) / (df.peer_count - 1)),
     NULL) AS field_pip_auc_score_percentile
FROM `scholar-version2.gs_statistics.stats_author_pip_scores_current` a
LEFT JOIN DistArrays da ON da.year_of_first_pub = a.year_of_first_pub
LEFT JOIN `scholar-version2.gs_statistics.author_fields` f ON f.scholar_id = a.scholar_id
LEFT JOIN `scholar-version2.gs_statistics.dist_pip_auc_scores_field` df
  ON df.field_group = f.field_group AND df.year_of_first_pub = a.year_of_first_pub;
