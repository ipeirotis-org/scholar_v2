-- Level 4: Author metric percentile distribution for temporal data.
--
-- Stores the distinct (benchmark, year_of_first_pub, state_year, metric_name, metric_value) → percentile
-- mapping for 7 author metrics. Partitioned by (benchmark, year_of_first_pub, state_year) to compare
-- authors against their benchmark peers at the same point in historical time.
--
-- Benchmarks:
--   'all_authors'    — full S2 population
--   'active_authors' — authors with hindex >= 3 AND total_publications >= 3 (current state)
--
-- PERCENT_RANK() is computed over ALL rows (preserving frequency), then DISTINCT
-- collapses tied values since they all receive the same rank.
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by ranked_author_metrics_temporal to do fast floor lookups.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_author_metrics_temporal`
CLUSTER BY benchmark, year_of_first_pub, state_year, metric_name
AS
WITH
  TemporalData AS (
    SELECT
      scholar_id,
      year_of_first_pub,
      state_year,
      total_publications,
      total_citations,
      total_recent_citations_5y,
      h_index,
      h_index_5y,
      i10_index,
      i10_index_5y
    FROM `scholar-version2.statistics.stats_author_metrics_temporal_view`
    WHERE year_of_first_pub IS NOT NULL
  ),
  -- Define active authors based on current metrics
  ActiveAuthors AS (
    SELECT a.authorid AS scholar_id
    FROM `scholar-version2.s2_data.authors` a
    JOIN `scholar-version2.s2_data.author_paper_stats` ps ON a.authorid = ps.authorid
    WHERE a.hindex >= 3 AND COALESCE(ps.total_publications, 0) >= 3
  ),
  -- all_authors benchmark
  AllPercentiles AS (
    SELECT
      'all_authors' AS benchmark, year_of_first_pub, state_year,
      total_publications, total_citations, total_recent_citations_5y,
      h_index, h_index_5y, i10_index, i10_index_5y,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY total_publications ASC)       AS total_publications_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY total_citations ASC)          AS total_citations_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY total_recent_citations_5y ASC) AS total_recent_citations_5y_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY h_index ASC)                  AS h_index_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY h_index_5y ASC)               AS h_index_5y_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY i10_index ASC)                AS i10_index_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY i10_index_5y ASC)             AS i10_index_5y_pct
    FROM TemporalData
  ),
  -- active_authors benchmark (filtered by current hindex >= 3, total_publications >= 3)
  ActivePercentiles AS (
    SELECT
      'active_authors' AS benchmark, t.year_of_first_pub, t.state_year,
      t.total_publications, t.total_citations, t.total_recent_citations_5y,
      t.h_index, t.h_index_5y, t.i10_index, t.i10_index_5y,
      PERCENT_RANK() OVER(PARTITION BY t.year_of_first_pub, t.state_year ORDER BY t.total_publications ASC)       AS total_publications_pct,
      PERCENT_RANK() OVER(PARTITION BY t.year_of_first_pub, t.state_year ORDER BY t.total_citations ASC)          AS total_citations_pct,
      PERCENT_RANK() OVER(PARTITION BY t.year_of_first_pub, t.state_year ORDER BY t.total_recent_citations_5y ASC) AS total_recent_citations_5y_pct,
      PERCENT_RANK() OVER(PARTITION BY t.year_of_first_pub, t.state_year ORDER BY t.h_index ASC)                  AS h_index_pct,
      PERCENT_RANK() OVER(PARTITION BY t.year_of_first_pub, t.state_year ORDER BY t.h_index_5y ASC)               AS h_index_5y_pct,
      PERCENT_RANK() OVER(PARTITION BY t.year_of_first_pub, t.state_year ORDER BY t.i10_index ASC)                AS i10_index_pct,
      PERCENT_RANK() OVER(PARTITION BY t.year_of_first_pub, t.state_year ORDER BY t.i10_index_5y ASC)             AS i10_index_5y_pct
    FROM TemporalData t
    WHERE t.scholar_id IN (SELECT scholar_id FROM ActiveAuthors)
  ),
  Combined AS (
    SELECT * FROM AllPercentiles
    UNION ALL
    SELECT * FROM ActivePercentiles
  )

SELECT DISTINCT benchmark, year_of_first_pub, state_year, 'total_publications'       AS metric_name, total_publications       AS metric_value, total_publications_pct       AS percentile FROM Combined
UNION ALL
SELECT DISTINCT benchmark, year_of_first_pub, state_year, 'total_citations',          total_citations,          total_citations_pct          FROM Combined
UNION ALL
SELECT DISTINCT benchmark, year_of_first_pub, state_year, 'total_recent_citations_5y', total_recent_citations_5y, total_recent_citations_5y_pct FROM Combined
UNION ALL
SELECT DISTINCT benchmark, year_of_first_pub, state_year, 'h_index',                  h_index,                  h_index_pct                  FROM Combined
UNION ALL
SELECT DISTINCT benchmark, year_of_first_pub, state_year, 'h_index_5y',               h_index_5y,               h_index_5y_pct               FROM Combined
UNION ALL
SELECT DISTINCT benchmark, year_of_first_pub, state_year, 'i10_index',                i10_index,                i10_index_pct                FROM Combined
UNION ALL
SELECT DISTINCT benchmark, year_of_first_pub, state_year, 'i10_index_5y',             i10_index_5y,             i10_index_5y_pct             FROM Combined;
