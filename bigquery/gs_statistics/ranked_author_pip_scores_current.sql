CREATE OR REPLACE VIEW `scholar-version2.gs_statistics.ranked_author_pip_scores_current` AS
-- Level 5: PiP-AUC scores enriched with percentile.
-- Uses RANGE_BUCKET + pre-aggregated arrays for O(log n) floor lookup.
-- Percentiles are computed against the 'active_authors' benchmark.
-- Reads from stats_author_pip_scores_current (view). During materialization,
-- the pipeline substitutes _table references for performance.
--
-- Field columns (GS only):
--   field_group, field_source: the author's own field group (author_fields), the
--     results page's default comparison.
--   field_percentiles_json: a JSON array with one object per field group, so the
--     page can switch fields without another lookup:
--     {field_group, cohort_start, cohort_end, peer_count, percentile}. Peers come
--     from dist_pip_auc_scores_field (active authors in that group whose career
--     started near the author's); percentile is the share of peers with a lower
--     PiP-AUC score, over peer_count - 1 (PERCENT_RANK when the author is among
--     the peers; capped at 1.0 for authors outside the benchmark who beat every
--     peer), NULL when the window holds fewer than 30 peers. Every group is
--     listed, whatever the author's own group (or none). NULL without a score.
-- They live here, not in a separate view, so the cache layer's author lookup
-- reads stats_author_pip_scores_current once (a separate view added ~3 s per
-- lookup). FieldDists pre-groups the per-field distributions by career-start
-- year because BigQuery rejects a per-row subquery on another table.
WITH
  DistArrays AS (
    SELECT
      year_of_first_pub,
      ARRAY_AGG(pip_auc_score ORDER BY pip_auc_score, percentile) AS values_arr,
      ARRAY_AGG(percentile ORDER BY pip_auc_score, percentile) AS pcts_arr
    FROM `scholar-version2.gs_statistics.dist_pip_auc_scores`
    WHERE benchmark = 'active_authors'
    GROUP BY year_of_first_pub
  ),
  FieldDists AS (
    SELECT
      year_of_first_pub,
      ARRAY_AGG(STRUCT(field_group, cohort_start, cohort_end, peer_count, scores) ORDER BY field_group) AS fields
    FROM `scholar-version2.gs_statistics.dist_pip_auc_scores_field`
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
  IF(a.pip_auc_score IS NULL, NULL,
     (SELECT TO_JSON_STRING(ARRAY_AGG(STRUCT(
               x.field_group, x.cohort_start, x.cohort_end, x.peer_count,
               IF(x.peer_count >= 30,
                  LEAST(1.0, (SELECT COUNTIF(s < a.pip_auc_score) FROM UNNEST(x.scores) AS s) / (x.peer_count - 1)),
                  NULL) AS percentile)
             ORDER BY x.field_group))
      FROM UNNEST(fd.fields) AS x)) AS field_percentiles_json
FROM `scholar-version2.gs_statistics.stats_author_pip_scores_current` a
LEFT JOIN DistArrays da ON da.year_of_first_pub = a.year_of_first_pub
LEFT JOIN FieldDists fd ON fd.year_of_first_pub = a.year_of_first_pub
LEFT JOIN `scholar-version2.gs_statistics.author_fields` f ON f.scholar_id = a.scholar_id;
