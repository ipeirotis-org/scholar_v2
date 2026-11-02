-- Publication citation percentile distribution (approximate quantiles), GS version.
-- 1000 breakpoints per pub_year over cited papers on profiled authors' lists.
-- ranked_publication_current does a floor lookup against this table.
-- Rebuilt by gs_crawler (CRAWL_MODE=materialize).

CREATE OR REPLACE TABLE `scholar-version2.gs_statistics.dist_publication_citations`
CLUSTER BY pub_year
AS
SELECT DISTINCT
  pub_year,
  value AS num_citations,
  offset / 1000.0 AS num_citations_percentile
FROM (
  SELECT pub_year, APPROX_QUANTILES(num_citations, 1000) AS quantiles
  FROM `scholar-version2.gs_statistics.stats_publication_current`
  GROUP BY pub_year
), UNNEST(quantiles) AS value WITH OFFSET AS offset;
