-- Publication citation percentile distribution table.
--
-- Stores the distinct (pub_year, num_citations) → percentile mapping.
-- PERCENT_RANK() is computed over ALL rows (preserving frequency), then DISTINCT
-- collapses tied values since they all receive the same rank.
--
-- Refreshed daily by bigquery-materialize.yml.
-- Used by stats_publication_current to do a fast JOIN lookup instead of
-- recomputing PERCENT_RANK() on every query.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_publication_citations`
CLUSTER BY pub_year
AS
SELECT DISTINCT
  pub_year,
  num_citations,
  PERCENT_RANK() OVER(PARTITION BY pub_year ORDER BY num_citations ASC) AS num_citations_percentile
FROM (
  SELECT
    CAST(JSON_EXTRACT_SCALAR(data, '$.data.bib.pub_year') AS INT64) AS pub_year,
    CAST(JSON_EXTRACT_SCALAR(data, '$.data.num_citations') AS INT64) AS num_citations
  FROM `scholar-version2.firestore_export.scholar_raw_pub_raw_latest`
)
WHERE pub_year > 1950
  AND pub_year <= EXTRACT(YEAR FROM CURRENT_DATE())
  AND num_citations > 0;
