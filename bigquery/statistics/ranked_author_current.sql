CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_author_current` AS
-- Level 3: Author stats enriched with 8 metric percentiles.
-- Joins stats_author_current (L2) against dist_author_metrics (L1)
-- using floor lookups: MAX(percentile WHERE dist_value <= actual_value).
WITH
  base AS (
    SELECT * FROM `scholar-version2.statistics.stats_author_current`
    WHERE year_of_first_pub IS NOT NULL
  ),
  -- One subquery per metric to do the floor lookup
  p_hindex AS (
    SELECT b.scholar_id, MAX(d.percentile) AS hindex_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'hindex' AND d.metric_value <= b.hindex
    GROUP BY b.scholar_id
  ),
  p_hindex5y AS (
    SELECT b.scholar_id, MAX(d.percentile) AS hindex5y_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'hindex5y' AND d.metric_value <= b.hindex5y
    GROUP BY b.scholar_id
  ),
  p_citedby AS (
    SELECT b.scholar_id, MAX(d.percentile) AS citedby_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'citedby' AND d.metric_value <= b.citedby
    GROUP BY b.scholar_id
  ),
  p_citedby5y AS (
    SELECT b.scholar_id, MAX(d.percentile) AS citedby5y_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'citedby5y' AND d.metric_value <= b.citedby5y
    GROUP BY b.scholar_id
  ),
  p_i10index AS (
    SELECT b.scholar_id, MAX(d.percentile) AS i10index_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'i10index' AND d.metric_value <= b.i10index
    GROUP BY b.scholar_id
  ),
  p_i10index5y AS (
    SELECT b.scholar_id, MAX(d.percentile) AS i10index5y_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'i10index5y' AND d.metric_value <= b.i10index5y
    GROUP BY b.scholar_id
  ),
  p_total_publications AS (
    SELECT b.scholar_id, MAX(d.percentile) AS total_publications_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'total_publications' AND d.metric_value <= b.total_publications
    GROUP BY b.scholar_id
  ),
  p_total_publications_with_citations AS (
    SELECT b.scholar_id, MAX(d.percentile) AS total_publications_with_citations_percentile
    FROM base b JOIN `scholar-version2.statistics.dist_author_metrics` d
      ON d.year_of_first_pub = b.year_of_first_pub AND d.metric_name = 'total_publications_with_citations' AND d.metric_value <= b.total_publications_with_citations
    GROUP BY b.scholar_id
  )
SELECT
  b.scholar_id,
  b.name,
  b.affiliation,
  b.email_domain,
  b.hindex,
  b.hindex5y,
  b.citedby,
  b.citedby5y,
  b.i10index,
  b.i10index5y,
  b.total_publications,
  b.total_publications_with_citations,
  b.year_of_first_pub,
  b.last_updated,
  COALESCE(p1.hindex_percentile, 0.0)                            AS hindex_percentile,
  COALESCE(p2.hindex5y_percentile, 0.0)                          AS hindex5y_percentile,
  COALESCE(p3.citedby_percentile, 0.0)                           AS citedby_percentile,
  COALESCE(p4.citedby5y_percentile, 0.0)                         AS citedby5y_percentile,
  COALESCE(p5.i10index_percentile, 0.0)                          AS i10index_percentile,
  COALESCE(p6.i10index5y_percentile, 0.0)                        AS i10index5y_percentile,
  COALESCE(p7.total_publications_percentile, 0.0)                AS total_publications_percentile,
  COALESCE(p8.total_publications_with_citations_percentile, 0.0) AS total_publications_with_citations_percentile
FROM base b
LEFT JOIN p_hindex                            p1 ON p1.scholar_id = b.scholar_id
LEFT JOIN p_hindex5y                          p2 ON p2.scholar_id = b.scholar_id
LEFT JOIN p_citedby                           p3 ON p3.scholar_id = b.scholar_id
LEFT JOIN p_citedby5y                         p4 ON p4.scholar_id = b.scholar_id
LEFT JOIN p_i10index                          p5 ON p5.scholar_id = b.scholar_id
LEFT JOIN p_i10index5y                        p6 ON p6.scholar_id = b.scholar_id
LEFT JOIN p_total_publications                p7 ON p7.scholar_id = b.scholar_id
LEFT JOIN p_total_publications_with_citations p8 ON p8.scholar_id = b.scholar_id;
