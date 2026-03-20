CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_author_metrics_temporal` AS
-- Level 5: Temporal author metrics enriched with 7 percentile columns.
-- Uses scalar subqueries against dist_author_metrics_temporal instead of
-- range joins, which are orders of magnitude faster for per-author queries.
SELECT
  b.scholar_id,
  b.state_year,
  b.year_of_first_pub,
  b.total_publications,
  b.total_citations,
  b.total_recent_citations_5y,
  b.h_index,
  b.h_index_5y,
  b.i10_index,
  b.i10_index_5y,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
     WHERE d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
       AND d.metric_name = 'total_publications' AND d.metric_value <= b.total_publications),
    0.0) AS total_publications_percentile,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
     WHERE d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
       AND d.metric_name = 'total_citations' AND d.metric_value <= b.total_citations),
    0.0) AS total_citations_percentile,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
     WHERE d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
       AND d.metric_name = 'total_recent_citations_5y' AND d.metric_value <= b.total_recent_citations_5y),
    0.0) AS total_recent_citations_5y_percentile,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
     WHERE d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
       AND d.metric_name = 'h_index' AND d.metric_value <= b.h_index),
    0.0) AS h_index_percentile,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
     WHERE d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
       AND d.metric_name = 'h_index_5y' AND d.metric_value <= b.h_index_5y),
    0.0) AS h_index_5y_percentile,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
     WHERE d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
       AND d.metric_name = 'i10_index' AND d.metric_value <= b.i10_index),
    0.0) AS i10_index_percentile,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
     WHERE d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
       AND d.metric_name = 'i10_index_5y' AND d.metric_value <= b.i10_index_5y),
    0.0) AS i10_index_5y_percentile
FROM `scholar-version2.statistics.stats_author_metrics_temporal_view` b
WHERE b.year_of_first_pub IS NOT NULL;
