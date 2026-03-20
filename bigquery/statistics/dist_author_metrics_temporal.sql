-- Level 4: Author metric percentile distribution for temporal data.
--
-- Stores the distinct (year_of_first_pub, state_year, metric_name, metric_value) → percentile
-- mapping for 7 author metrics. Partitioned by (year_of_first_pub, state_year) to compare
-- authors against their peers at the same point in historical time.
--
-- PERCENT_RANK() is computed over ALL rows (preserving frequency), then DISTINCT
-- collapses tied values since they all receive the same rank.
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by ranked_author_metrics_temporal to do fast floor lookups.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_author_metrics_temporal`
CLUSTER BY year_of_first_pub, state_year, metric_name
AS
WITH
  TemporalData AS (
    SELECT
      year_of_first_pub,
      state_year,
      total_publications,
      total_citations,
      total_recent_citations_5y,
      h_index,
      h_index_5y,
      i10_index,
      i10_index_5y
    FROM `scholar-version2.statistics.stats_author_metrics_temporal_view`
    WHERE year_of_first_pub IS NOT NULL
  ),
  WithPercentiles AS (
    SELECT *,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY total_publications ASC)       AS total_publications_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY total_citations ASC)          AS total_citations_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY total_recent_citations_5y ASC) AS total_recent_citations_5y_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY h_index ASC)                  AS h_index_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY h_index_5y ASC)               AS h_index_5y_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY i10_index ASC)                AS i10_index_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY i10_index_5y ASC)             AS i10_index_5y_pct
    FROM TemporalData
  )

SELECT DISTINCT year_of_first_pub, state_year, 'total_publications'       AS metric_name, total_publications       AS metric_value, total_publications_pct       AS percentile FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, state_year, 'total_citations',          total_citations,          total_citations_pct          FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, state_year, 'total_recent_citations_5y', total_recent_citations_5y, total_recent_citations_5y_pct FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, state_year, 'h_index',                  h_index,                  h_index_pct                  FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, state_year, 'h_index_5y',               h_index_5y,               h_index_5y_pct               FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, state_year, 'i10_index',                i10_index,                i10_index_pct                FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, state_year, 'i10_index_5y',             i10_index_5y,             i10_index_5y_pct             FROM WithPercentiles;
