CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_author_metrics_temporal` AS
-- Level 5: Temporal author metrics enriched with 7 percentile columns.
-- Uses RANGE_BUCKET + pre-aggregated arrays for O(log n) floor lookups instead of
-- correlated scalar subqueries (which are slow on 1B+ rows).
-- Percentiles are computed against the 'active_authors' benchmark.
WITH
  -- Pre-aggregate dist breakpoints into sorted arrays per (year_of_first_pub, state_year, metric_name)
  DistArrays AS (
    SELECT
      year_of_first_pub,
      state_year,
      metric_name,
      ARRAY_AGG(metric_value ORDER BY metric_value, percentile) AS values_arr,
      ARRAY_AGG(percentile ORDER BY metric_value, percentile) AS pcts_arr
    FROM `scholar-version2.statistics.dist_author_metrics_temporal`
    WHERE benchmark = 'active_authors'
    GROUP BY year_of_first_pub, state_year, metric_name
  ),
  -- Pivot into one row per (year_of_first_pub, state_year) with arrays for each metric
  DistPivot AS (
    SELECT
      year_of_first_pub,
      state_year,
      ANY_VALUE(IF(metric_name = 'total_publications', values_arr, NULL)) AS tp_vals,
      ANY_VALUE(IF(metric_name = 'total_publications', pcts_arr, NULL)) AS tp_pcts,
      ANY_VALUE(IF(metric_name = 'total_citations', values_arr, NULL)) AS tc_vals,
      ANY_VALUE(IF(metric_name = 'total_citations', pcts_arr, NULL)) AS tc_pcts,
      ANY_VALUE(IF(metric_name = 'total_recent_citations_5y', values_arr, NULL)) AS trc_vals,
      ANY_VALUE(IF(metric_name = 'total_recent_citations_5y', pcts_arr, NULL)) AS trc_pcts,
      ANY_VALUE(IF(metric_name = 'h_index', values_arr, NULL)) AS hi_vals,
      ANY_VALUE(IF(metric_name = 'h_index', pcts_arr, NULL)) AS hi_pcts,
      ANY_VALUE(IF(metric_name = 'h_index_5y', values_arr, NULL)) AS hi5_vals,
      ANY_VALUE(IF(metric_name = 'h_index_5y', pcts_arr, NULL)) AS hi5_pcts,
      ANY_VALUE(IF(metric_name = 'i10_index', values_arr, NULL)) AS i10_vals,
      ANY_VALUE(IF(metric_name = 'i10_index', pcts_arr, NULL)) AS i10_pcts,
      ANY_VALUE(IF(metric_name = 'i10_index_5y', values_arr, NULL)) AS i105_vals,
      ANY_VALUE(IF(metric_name = 'i10_index_5y', pcts_arr, NULL)) AS i105_pcts
    FROM DistArrays
    GROUP BY year_of_first_pub, state_year
  )
SELECT
  b.scholar_id,
  b.state_year,
  b.year_of_first_pub,
  b.total_publications,
  b.total_citations,
  b.total_recent_citations_5y,
  b.h_index,
  b.h_index_5y,
  b.i10_index,
  b.i10_index_5y,
  COALESCE(dp.tp_pcts[SAFE_ORDINAL(RANGE_BUCKET(b.total_publications, dp.tp_vals))], 0.0)
    AS total_publications_percentile,
  COALESCE(dp.tc_pcts[SAFE_ORDINAL(RANGE_BUCKET(b.total_citations, dp.tc_vals))], 0.0)
    AS total_citations_percentile,
  COALESCE(dp.trc_pcts[SAFE_ORDINAL(RANGE_BUCKET(b.total_recent_citations_5y, dp.trc_vals))], 0.0)
    AS total_recent_citations_5y_percentile,
  COALESCE(dp.hi_pcts[SAFE_ORDINAL(RANGE_BUCKET(b.h_index, dp.hi_vals))], 0.0)
    AS h_index_percentile,
  COALESCE(dp.hi5_pcts[SAFE_ORDINAL(RANGE_BUCKET(b.h_index_5y, dp.hi5_vals))], 0.0)
    AS h_index_5y_percentile,
  COALESCE(dp.i10_pcts[SAFE_ORDINAL(RANGE_BUCKET(b.i10_index, dp.i10_vals))], 0.0)
    AS i10_index_percentile,
  COALESCE(dp.i105_pcts[SAFE_ORDINAL(RANGE_BUCKET(b.i10_index_5y, dp.i105_vals))], 0.0)
    AS i10_index_5y_percentile
FROM `scholar-version2.statistics.stats_author_metrics_temporal_view` b
LEFT JOIN DistPivot dp
  ON dp.year_of_first_pub = b.year_of_first_pub
 AND dp.state_year = b.state_year
WHERE b.year_of_first_pub IS NOT NULL;
