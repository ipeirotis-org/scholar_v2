-- Author metric percentile distribution table.
--
-- Source: s2_data.authors + s2_data.author_paper_stats.
-- Stores distinct (year_of_first_pub, metric_name, metric_value) → percentile rows
-- for 5 author metrics (hindex5y, citedby5y, i10index5y dropped — not in S2).
-- PERCENT_RANK() is computed over all authors per cohort,
-- then DISTINCT collapses tied values.
--
-- Including total_publications_with_citations here also serves as the
-- num_papers distribution needed by stats_author_publication_pip_inputs_current,
-- so that view can read directly from this small table instead of scanning
-- all of stats_author_current.
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by ranked_author_current for fast lookups instead of live PERCENT_RANK().

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_author_metrics`
CLUSTER BY year_of_first_pub, metric_name
AS
WITH
  CombinedData AS (
    -- Join S2 authors with computed paper stats.
    -- Only include authors who have year_of_first_pub (at least one dated paper).
    SELECT
      a.authorid AS scholar_id,
      a.hindex,
      a.citationcount AS citedby,
      COALESCE(ps.i10_index, 0) AS i10index,
      COALESCE(ps.total_publications, 0) AS total_publications,
      COALESCE(ps.total_publications_with_citations, 0) AS total_publications_with_citations,
      ps.year_of_first_pub
    FROM `scholar-version2.s2_data.authors` a
    JOIN `scholar-version2.s2_data.author_paper_stats` ps ON a.authorid = ps.authorid
    WHERE a.authorid IS NOT NULL
      AND ps.year_of_first_pub IS NOT NULL
  ),
  WithPercentiles AS (
    -- Compute PERCENT_RANK for all 5 metrics in a single pass over CombinedData
    SELECT *,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY hindex ASC)                            AS hindex_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY citedby ASC)                           AS citedby_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY i10index ASC)                          AS i10index_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY total_publications ASC)                AS total_publications_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY total_publications_with_citations ASC) AS total_publications_with_citations_pct
    FROM CombinedData
  )
-- Pivot to normalized (year_of_first_pub, metric_name, metric_value, percentile) format.
-- DISTINCT collapses tied values that share the same PERCENT_RANK.
SELECT DISTINCT year_of_first_pub, 'hindex'                           AS metric_name, hindex                           AS metric_value, hindex_pct                           AS percentile FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, 'citedby',                           citedby,                           citedby_pct                           FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, 'i10index',                          i10index,                          i10index_pct                          FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, 'total_publications',                total_publications,                total_publications_pct                FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, 'total_publications_with_citations', total_publications_with_citations, total_publications_with_citations_pct FROM WithPercentiles;
