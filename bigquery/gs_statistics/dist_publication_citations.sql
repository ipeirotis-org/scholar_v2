-- Publication citation percentile distribution, GS version: exact, one row per (pub_year, citation count).
-- num_citations_percentile = share of cited papers on profiled authors' lists from the same year with at most
-- this many citations (the cumulative distribution), so distinct citation counts get distinct percentiles.
-- ranked_publication_current does a floor lookup against this table (MAX percentile where count <= the paper's).
-- Exact counts replaced APPROX_QUANTILES(..., 1000) on 2026-11-04: the 1000 breakpoints rounded percentiles to
-- 0.001 steps, so same-year papers with different citation counts could tie and be ranked in either order. GS has
-- ~75K distinct (pub_year, citations) pairs, fewer rows than the 1000-quantile table had.
-- Rebuilt by gs_crawler (CRAWL_MODE=materialize).
CREATE OR REPLACE TABLE `scholar-version2.gs_statistics.dist_publication_citations`
CLUSTER BY pub_year
AS
SELECT
  pub_year,
  num_citations,
  SUM(n) OVER (PARTITION BY pub_year ORDER BY num_citations) / SUM(n) OVER (PARTITION BY pub_year)
    AS num_citations_percentile
FROM (
  SELECT pub_year, num_citations, COUNT(*) AS n
  FROM `scholar-version2.gs_statistics.stats_publication_current`
  GROUP BY pub_year, num_citations
);
