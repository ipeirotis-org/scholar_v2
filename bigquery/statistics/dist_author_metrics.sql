-- Author metric percentile distribution table (approximate quantiles).
--
-- Source: s2_data.authors + s2_data.author_paper_stats.
-- Stores 1000 quantile breakpoints per (benchmark, year_of_first_pub, metric_name).
-- Each row maps a metric_value to its percentile (0.000 to 1.000).
--
-- At query time, ranked_author_current does a floor lookup:
--   MAX(percentile) WHERE metric_value <= author's_value
-- This gives an approximate percentile within 0.1% accuracy.
--
-- Benchmark:
--   'active_authors' — authors with hindex >= 3 AND total_publications >= 3
--
-- Much faster to compute than exact PERCENT_RANK() and produces a tiny table
-- (~250K rows vs millions). Refreshed quarterly.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_author_metrics`
CLUSTER BY benchmark, year_of_first_pub, metric_name
AS
WITH
  CombinedData AS (
    SELECT
      a.hindex,
      a.citationcount AS citedby,
      COALESCE(ps.i10_index, 0) AS i10index,
      COALESCE(ps.total_publications, 0) AS total_publications,
      COALESCE(ps.total_publications_with_citations, 0) AS total_publications_with_citations,
      ps.year_of_first_pub
    FROM `scholar-version2.s2_data.authors` a
    JOIN `scholar-version2.s2_data.author_paper_stats` ps ON a.authorid = ps.authorid
    WHERE a.authorid IS NOT NULL
      AND ps.year_of_first_pub IS NOT NULL
      -- active_authors benchmark: hindex >= 3 AND total_publications >= 3
      AND a.hindex >= 3
      AND COALESCE(ps.total_publications, 0) >= 3
  )
-- Generate quantile breakpoints for each (cohort, metric).
-- DISTINCT collapses duplicate breakpoints
-- (many quantiles map to the same value, e.g. hindex=3 for p0-p30).
SELECT DISTINCT * FROM (
  SELECT 'active_authors' AS benchmark, year_of_first_pub, metric_name,
         value AS metric_value, offset / 1000.0 AS percentile
  FROM (
    SELECT year_of_first_pub, 'hindex' AS metric_name,
           APPROX_QUANTILES(hindex, 1000) AS quantiles FROM CombinedData GROUP BY year_of_first_pub
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
  UNION ALL
  SELECT 'active_authors', year_of_first_pub, 'citedby',
         value, offset / 1000.0
  FROM (
    SELECT year_of_first_pub, APPROX_QUANTILES(citedby, 1000) AS quantiles FROM CombinedData GROUP BY year_of_first_pub
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
  UNION ALL
  SELECT 'active_authors', year_of_first_pub, 'i10index',
         value, offset / 1000.0
  FROM (
    SELECT year_of_first_pub, APPROX_QUANTILES(i10index, 1000) AS quantiles FROM CombinedData GROUP BY year_of_first_pub
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
  UNION ALL
  SELECT 'active_authors', year_of_first_pub, 'total_publications',
         value, offset / 1000.0
  FROM (
    SELECT year_of_first_pub, APPROX_QUANTILES(total_publications, 1000) AS quantiles FROM CombinedData GROUP BY year_of_first_pub
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
  UNION ALL
  SELECT 'active_authors', year_of_first_pub, 'total_publications_with_citations',
         value, offset / 1000.0
  FROM (
    SELECT year_of_first_pub, APPROX_QUANTILES(total_publications_with_citations, 1000) AS quantiles FROM CombinedData GROUP BY year_of_first_pub
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
);
