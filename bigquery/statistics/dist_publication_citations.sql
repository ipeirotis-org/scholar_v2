-- Publication citation percentile distribution table (approximate quantiles).
--
-- Stores 1000 quantile breakpoints per pub_year for citation counts.
-- Source: s2_data.papers (one row per paper, not per author).
-- Uses APPROX_QUANTILES instead of exact PERCENT_RANK for efficiency.
--
-- At query time, ranked_publication_current does a floor lookup:
--   MAX(num_citations_percentile) WHERE num_citations <= paper's_value
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by ranked_publication_current for fast floor lookups.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_publication_citations`
CLUSTER BY pub_year
AS
SELECT DISTINCT
  pub_year,
  value AS num_citations,
  offset / 1000.0 AS num_citations_percentile
FROM (
  SELECT
    year AS pub_year,
    APPROX_QUANTILES(citationcount, 1000) AS quantiles
  FROM `scholar-version2.s2_data.papers`
  WHERE year IS NOT NULL
    AND year > 1950
    AND year <= EXTRACT(YEAR FROM CURRENT_DATE())
    AND citationcount > 0
    -- Only include papers from authors with >= 6 total publications
    -- for percentile calculations.
    AND corpusid IN (
      SELECT corpusid FROM `scholar-version2.s2_data.qualifying_papers`
    )
  GROUP BY year
), UNNEST(quantiles) AS value WITH OFFSET AS offset;
