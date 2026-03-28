CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_author_current` AS
-- Level 3: Author stats enriched with 5 metric percentiles.
-- Uses scalar subqueries against the small dist_author_metrics table instead of
-- range joins, which are orders of magnitude faster for per-author queries.
--
-- Dropped vs Google Scholar version: hindex5y, citedby5y, i10index5y percentiles
-- (those metrics are not available in S2).
SELECT
  b.scholar_id,
  b.name,
  b.affiliation,
  b.hindex,
  b.citedby,
  b.i10index,
  b.total_publications,
  b.total_publications_with_citations,
  b.year_of_first_pub,
  b.last_updated,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_author_metrics` d
     WHERE d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'hindex' AND d.metric_value <= b.hindex),
    0.0) AS hindex_percentile,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_author_metrics` d
     WHERE d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'citedby' AND d.metric_value <= b.citedby),
    0.0) AS citedby_percentile,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_author_metrics` d
     WHERE d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'i10index' AND d.metric_value <= b.i10index),
    0.0) AS i10index_percentile,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_author_metrics` d
     WHERE d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'total_publications' AND d.metric_value <= b.total_publications),
    0.0) AS total_publications_percentile,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_author_metrics` d
     WHERE d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'total_publications_with_citations' AND d.metric_value <= b.total_publications_with_citations),
    0.0) AS total_publications_with_citations_percentile
FROM `scholar-version2.statistics.stats_author_current` b
WHERE b.year_of_first_pub IS NOT NULL;
