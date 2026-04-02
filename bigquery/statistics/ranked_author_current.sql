CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_author_current` AS
-- Level 3: Author stats enriched with 5 metric percentiles.
-- Uses RANGE_BUCKET + pre-aggregated arrays for O(log n) floor lookups instead of
-- correlated scalar subqueries.
--
-- Percentiles are computed against the 'active_authors' benchmark
-- (hindex >= 3 AND total_publications >= 3) for meaningful differentiation.
--
-- hindex5y, citedby5y, i10index5y are not available in S2.
-- NULL compatibility columns + 0.0 percentiles kept for frontend/cache consumers.
WITH
  DistArrays AS (
    SELECT
      year_of_first_pub,
      metric_name,
      ARRAY_AGG(metric_value ORDER BY metric_value, percentile) AS values_arr,
      ARRAY_AGG(percentile ORDER BY metric_value, percentile) AS pcts_arr
    FROM `scholar-version2.statistics.dist_author_metrics`
    WHERE benchmark = 'active_authors'
    GROUP BY year_of_first_pub, metric_name
  ),
  DistPivot AS (
    SELECT
      year_of_first_pub,
      ANY_VALUE(IF(metric_name = 'hindex', values_arr, NULL)) AS hi_vals,
      ANY_VALUE(IF(metric_name = 'hindex', pcts_arr, NULL)) AS hi_pcts,
      ANY_VALUE(IF(metric_name = 'citedby', values_arr, NULL)) AS cb_vals,
      ANY_VALUE(IF(metric_name = 'citedby', pcts_arr, NULL)) AS cb_pcts,
      ANY_VALUE(IF(metric_name = 'i10index', values_arr, NULL)) AS i10_vals,
      ANY_VALUE(IF(metric_name = 'i10index', pcts_arr, NULL)) AS i10_pcts,
      ANY_VALUE(IF(metric_name = 'total_publications', values_arr, NULL)) AS tp_vals,
      ANY_VALUE(IF(metric_name = 'total_publications', pcts_arr, NULL)) AS tp_pcts,
      ANY_VALUE(IF(metric_name = 'total_publications_with_citations', values_arr, NULL)) AS tpwc_vals,
      ANY_VALUE(IF(metric_name = 'total_publications_with_citations', pcts_arr, NULL)) AS tpwc_pcts
    FROM DistArrays
    GROUP BY year_of_first_pub
  )
SELECT
  b.scholar_id,
  b.name,
  b.affiliation,
  b.email_domain,
  b.hindex,
  b.hindex5y,
  b.citedby,
  b.citedby5y,
  b.i10index,
  b.i10index5y,
  b.total_publications,
  b.total_publications_with_citations,
  b.year_of_first_pub,
  b.last_updated,
  COALESCE(dp.hi_pcts[SAFE_ORDINAL(RANGE_BUCKET(b.hindex, dp.hi_vals))], 0.0) AS hindex_percentile,
  0.0 AS hindex5y_percentile,
  COALESCE(dp.cb_pcts[SAFE_ORDINAL(RANGE_BUCKET(b.citedby, dp.cb_vals))], 0.0) AS citedby_percentile,
  0.0 AS citedby5y_percentile,
  COALESCE(dp.i10_pcts[SAFE_ORDINAL(RANGE_BUCKET(b.i10index, dp.i10_vals))], 0.0) AS i10index_percentile,
  0.0 AS i10index5y_percentile,
  COALESCE(dp.tp_pcts[SAFE_ORDINAL(RANGE_BUCKET(b.total_publications, dp.tp_vals))], 0.0) AS total_publications_percentile,
  COALESCE(dp.tpwc_pcts[SAFE_ORDINAL(RANGE_BUCKET(b.total_publications_with_citations, dp.tpwc_vals))], 0.0) AS total_publications_with_citations_percentile
FROM `scholar-version2.statistics.stats_author_current` b
LEFT JOIN DistPivot dp ON dp.year_of_first_pub = b.year_of_first_pub
WHERE b.year_of_first_pub IS NOT NULL;
