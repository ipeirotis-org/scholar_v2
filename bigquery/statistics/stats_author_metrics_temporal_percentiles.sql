WITH TemporalMetrics AS (
  -- Reference the view created in Step 3, which contains the historical metric *values*
  SELECT * FROM `scholar-version2.statistics.stats_author_metrics_temporal`
)

SELECT
  scholar_id,
  state_year,
  year_of_first_pub,

  -- Include the original metric values for reference
  total_publications,
  total_citations,
  total_recent_citations_5y,
  h_index,
  h_index_5y,
  i10_index,
  i10_index_5y,

  -- Calculate Percentiles for each metric using PERCENT_RANK()
  -- Partitioning by year_of_first_pub and state_year compares authors against their peers
  -- (same start year) at the same point in historical time.
  -- Ordering by the metric ASC means higher values get ranks closer to 1.0.

  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY total_publications ASC) AS total_publications_percentile,
  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY total_citations ASC) AS total_citations_percentile,
  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY total_recent_citations_5y ASC) AS total_recent_citations_5y_percentile,
  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY h_index ASC) AS h_index_percentile,
  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY h_index_5y ASC) AS h_index_5y_percentile,
  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY i10_index ASC) AS i10_index_percentile,
  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY i10_index_5y ASC) AS i10_index_5y_percentile

FROM TemporalMetrics

-- No explicit ordering needed in the final view, but can be added if required for specific use cases.
-- ORDER BY scholar_id, state_year;