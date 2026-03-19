CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_author_metrics_temporal` AS
-- Tier 3: Temporal author metrics enriched with 7 percentile columns.
-- Joins stats_author_metrics_temporal_view (Tier 1) against
-- dist_author_metrics_temporal (Tier 2) using floor lookups.
WITH
  base AS (
    SELECT * FROM `scholar-version2.statistics.stats_author_metrics_temporal_view`
    WHERE year_of_first_pub IS NOT NULL
  ),
  p_total_publications AS (
    SELECT b.scholar_id, b.state_year, MAX(d.percentile) AS total_publications_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics_temporal` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
     AND d.metric_name = 'total_publications' AND d.metric_value <= b.total_publications
    GROUP BY b.scholar_id, b.state_year
  ),
  p_total_citations AS (
    SELECT b.scholar_id, b.state_year, MAX(d.percentile) AS total_citations_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics_temporal` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
     AND d.metric_name = 'total_citations' AND d.metric_value <= b.total_citations
    GROUP BY b.scholar_id, b.state_year
  ),
  p_total_recent_citations_5y AS (
    SELECT b.scholar_id, b.state_year, MAX(d.percentile) AS total_recent_citations_5y_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics_temporal` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
     AND d.metric_name = 'total_recent_citations_5y' AND d.metric_value <= b.total_recent_citations_5y
    GROUP BY b.scholar_id, b.state_year
  ),
  p_h_index AS (
    SELECT b.scholar_id, b.state_year, MAX(d.percentile) AS h_index_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics_temporal` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
     AND d.metric_name = 'h_index' AND d.metric_value <= b.h_index
    GROUP BY b.scholar_id, b.state_year
  ),
  p_h_index_5y AS (
    SELECT b.scholar_id, b.state_year, MAX(d.percentile) AS h_index_5y_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics_temporal` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
     AND d.metric_name = 'h_index_5y' AND d.metric_value <= b.h_index_5y
    GROUP BY b.scholar_id, b.state_year
  ),
  p_i10_index AS (
    SELECT b.scholar_id, b.state_year, MAX(d.percentile) AS i10_index_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics_temporal` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
     AND d.metric_name = 'i10_index' AND d.metric_value <= b.i10_index
    GROUP BY b.scholar_id, b.state_year
  ),
  p_i10_index_5y AS (
    SELECT b.scholar_id, b.state_year, MAX(d.percentile) AS i10_index_5y_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics_temporal` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.state_year = b.state_year
     AND d.metric_name = 'i10_index_5y' AND d.metric_value <= b.i10_index_5y
    GROUP BY b.scholar_id, b.state_year
  )
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
  COALESCE(p1.total_publications_percentile, 0.0)       AS total_publications_percentile,
  COALESCE(p2.total_citations_percentile, 0.0)           AS total_citations_percentile,
  COALESCE(p3.total_recent_citations_5y_percentile, 0.0) AS total_recent_citations_5y_percentile,
  COALESCE(p4.h_index_percentile, 0.0)                  AS h_index_percentile,
  COALESCE(p5.h_index_5y_percentile, 0.0)               AS h_index_5y_percentile,
  COALESCE(p6.i10_index_percentile, 0.0)                AS i10_index_percentile,
  COALESCE(p7.i10_index_5y_percentile, 0.0)             AS i10_index_5y_percentile
FROM base b
LEFT JOIN p_total_publications       p1 ON p1.scholar_id = b.scholar_id AND p1.state_year = b.state_year
LEFT JOIN p_total_citations          p2 ON p2.scholar_id = b.scholar_id AND p2.state_year = b.state_year
LEFT JOIN p_total_recent_citations_5y p3 ON p3.scholar_id = b.scholar_id AND p3.state_year = b.state_year
LEFT JOIN p_h_index                  p4 ON p4.scholar_id = b.scholar_id AND p4.state_year = b.state_year
LEFT JOIN p_h_index_5y              p5 ON p5.scholar_id = b.scholar_id AND p5.state_year = b.state_year
LEFT JOIN p_i10_index               p6 ON p6.scholar_id = b.scholar_id AND p6.state_year = b.state_year
LEFT JOIN p_i10_index_5y            p7 ON p7.scholar_id = b.scholar_id AND p7.state_year = b.state_year;
