-- Publication citation percentile distribution table.
--
-- Stores the distinct (pub_year, num_citations) → percentile mapping.
-- Source: s2_data.papers (one row per paper, not per author).
-- PERCENT_RANK() is computed over ALL papers (preserving frequency), then DISTINCT
-- collapses tied values since they all receive the same rank.
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by ranked_publication_current to do a fast JOIN lookup instead of
-- recomputing PERCENT_RANK() on every query.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_publication_citations`
CLUSTER BY pub_year
AS
SELECT DISTINCT
  year AS pub_year,
  citationcount AS num_citations,
  PERCENT_RANK() OVER(PARTITION BY year ORDER BY citationcount ASC) AS num_citations_percentile
FROM `scholar-version2.s2_data.papers`
WHERE year IS NOT NULL
  AND year > 1950
  AND year <= EXTRACT(YEAR FROM CURRENT_DATE())
  AND citationcount > 0
  -- Only include papers from authors with >= 6 total publications
  -- for percentile calculations.
  AND corpusid IN (
    SELECT corpusid FROM `scholar-version2.s2_data.qualifying_papers`
  );
