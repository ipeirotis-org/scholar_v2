-- Publication citation percentile distribution table.
--
-- Stores the distinct (pub_year, num_citations) → percentile mapping.
-- PERCENT_RANK() is computed over ALL rows (preserving frequency), then DISTINCT
-- collapses tied values since they all receive the same rank.
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by stats_publication_current to do a fast JOIN lookup instead of
-- recomputing PERCENT_RANK() on every query.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_publication_citations`
CLUSTER BY pub_year
AS
SELECT DISTINCT
  pub_year,
  num_citations,
  PERCENT_RANK() OVER(PARTITION BY pub_year ORDER BY num_citations ASC) AS num_citations_percentile
FROM `scholar-version2.scholar_raw_data.pub_latest_table`
WHERE pub_year > 1950
  AND pub_year <= EXTRACT(YEAR FROM CURRENT_DATE())
  AND num_citations > 0;
