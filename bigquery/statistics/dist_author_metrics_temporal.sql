-- Level 4: Author metric percentile distribution for temporal data (approximate quantiles).
--
-- Stores 1000 quantile breakpoints per (benchmark, year_of_first_pub, state_year, metric_name).
-- Each row maps a metric_value to its percentile (0.000 to 1.000).
--
-- At query time, ranked_author_metrics_temporal does a floor lookup:
--   MAX(percentile) WHERE metric_value <= author's_value
--
-- Benchmarks:
--   'active_authors' — authors with hindex >= 3 AND total_publications >= 3 (current state)
--
-- Uses APPROX_QUANTILES for fast computation. Much smaller output than exact
-- PERCENT_RANK (hundreds of thousands of rows vs tens of millions).
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by ranked_author_metrics_temporal for fast floor lookups.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_author_metrics_temporal`
CLUSTER BY benchmark, year_of_first_pub, state_year, metric_name
AS
WITH
  TemporalData AS (
    SELECT
      scholar_id,
      year_of_first_pub,
      state_year,
      total_publications,
      total_citations,
      total_recent_citations_5y,
      h_index,
      h_index_5y,
      i10_index,
      i10_index_5y
    FROM `scholar-version2.statistics.stats_author_metrics_temporal_table`
    WHERE year_of_first_pub IS NOT NULL
  ),
  ActiveAuthors AS (
    SELECT a.authorid AS scholar_id
    FROM `scholar-version2.s2_data.authors` a
    JOIN `scholar-version2.s2_data.author_paper_stats` ps ON a.authorid = ps.authorid
    WHERE a.hindex >= 3 AND COALESCE(ps.total_publications, 0) >= 3
  ),
  ActiveData AS (
    SELECT t.* FROM TemporalData t
    WHERE t.scholar_id IN (SELECT scholar_id FROM ActiveAuthors)
  )
-- Generate quantile breakpoints for each (year_of_first_pub, state_year, metric)
-- Only active_authors benchmark for temporal (all_authors is too expensive and not used).
SELECT DISTINCT * FROM (
  SELECT 'active_authors' AS benchmark, year_of_first_pub, state_year, metric_name,
         value AS metric_value, offset / 1000.0 AS percentile
  FROM (
    SELECT year_of_first_pub, state_year, 'total_publications' AS metric_name,
           APPROX_QUANTILES(total_publications, 1000) AS quantiles
    FROM ActiveData GROUP BY year_of_first_pub, state_year
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
  UNION ALL
  SELECT 'active_authors', year_of_first_pub, state_year, 'total_citations',
         value, offset / 1000.0
  FROM (
    SELECT year_of_first_pub, state_year, APPROX_QUANTILES(total_citations, 1000) AS quantiles
    FROM ActiveData GROUP BY year_of_first_pub, state_year
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
  UNION ALL
  SELECT 'active_authors', year_of_first_pub, state_year, 'total_recent_citations_5y',
         value, offset / 1000.0
  FROM (
    SELECT year_of_first_pub, state_year, APPROX_QUANTILES(total_recent_citations_5y, 1000) AS quantiles
    FROM ActiveData GROUP BY year_of_first_pub, state_year
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
  UNION ALL
  SELECT 'active_authors', year_of_first_pub, state_year, 'h_index',
         value, offset / 1000.0
  FROM (
    SELECT year_of_first_pub, state_year, APPROX_QUANTILES(h_index, 1000) AS quantiles
    FROM ActiveData GROUP BY year_of_first_pub, state_year
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
  UNION ALL
  SELECT 'active_authors', year_of_first_pub, state_year, 'h_index_5y',
         value, offset / 1000.0
  FROM (
    SELECT year_of_first_pub, state_year, APPROX_QUANTILES(h_index_5y, 1000) AS quantiles
    FROM ActiveData GROUP BY year_of_first_pub, state_year
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
  UNION ALL
  SELECT 'active_authors', year_of_first_pub, state_year, 'i10_index',
         value, offset / 1000.0
  FROM (
    SELECT year_of_first_pub, state_year, APPROX_QUANTILES(i10_index, 1000) AS quantiles
    FROM ActiveData GROUP BY year_of_first_pub, state_year
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
  UNION ALL
  SELECT 'active_authors', year_of_first_pub, state_year, 'i10_index_5y',
         value, offset / 1000.0
  FROM (
    SELECT year_of_first_pub, state_year, APPROX_QUANTILES(i10_index_5y, 1000) AS quantiles
    FROM ActiveData GROUP BY year_of_first_pub, state_year
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
);
