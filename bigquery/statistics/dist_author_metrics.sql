-- Author metric percentile distribution table.
--
-- Stores distinct (year_of_first_pub, metric_name, metric_value) → percentile rows
-- for all 8 author metrics. PERCENT_RANK() is computed over all authors per cohort,
-- then DISTINCT collapses tied values.
--
-- Including total_publications_with_citations here also serves as the
-- num_papers distribution needed by stats_author_publication_pip_inputs_current,
-- so that view can read directly from this small table instead of scanning
-- all of stats_author_current.
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by stats_author_current for fast lookups instead of live PERCENT_RANK().

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_author_metrics`
CLUSTER BY year_of_first_pub, metric_name
AS
WITH
  AuthorPubs AS (
    -- Explode publication list from author JSON (same logic as base_author_publications)
    SELECT
      JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') AS scholar_id,
      JSON_EXTRACT_SCALAR(pub, '$.author_pub_id') AS author_pub_id,
      CAST(JSON_EXTRACT_SCALAR(pub, '$.bib.pub_year') AS INT64) AS pub_year
    FROM `scholar-version2.scholar_raw_data.author_latest`,
         UNNEST(JSON_EXTRACT_ARRAY(DATA, '$.data.publications')) AS pub
    WHERE JSON_EXTRACT_SCALAR(pub, '$.author_pub_id') IS NOT NULL
      AND JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') IS NOT NULL
      AND CAST(JSON_EXTRACT_SCALAR(pub, '$.bib.pub_year') AS INT64) IS NOT NULL
  ),
  PubCitations AS (
    -- Citation counts per publication (for filtering cited vs uncited pubs)
    SELECT
      JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id') AS author_pub_id,
      CAST(JSON_EXTRACT_SCALAR(data, '$.data.num_citations') AS INT64) AS num_citations
    FROM `scholar-version2.scholar_raw_data.pub_latest`
  ),
  AuthorPubCounts AS (
    -- Compute total publications, cited publications, and year of first cited pub
    SELECT
      ap.scholar_id,
      COUNT(ap.author_pub_id) AS total_publications,
      COUNT(IF(pc.num_citations > 0, ap.author_pub_id, NULL)) AS total_publications_with_citations,
      MIN(IF(pc.num_citations > 0, ap.pub_year, NULL)) AS year_of_first_pub
    FROM AuthorPubs ap
    LEFT JOIN PubCitations pc ON ap.author_pub_id = pc.author_pub_id
    WHERE ap.pub_year > 1950 AND ap.pub_year <= EXTRACT(YEAR FROM CURRENT_DATE())
    GROUP BY ap.scholar_id
  ),
  ScholarData AS (
    SELECT
      JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') AS scholar_id,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.hindex') AS INT64) AS hindex,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.hindex5y') AS INT64) AS hindex5y,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.citedby') AS INT64) AS citedby,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.citedby5y') AS INT64) AS citedby5y,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.i10index') AS INT64) AS i10index,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.i10index5y') AS INT64) AS i10index5y
    FROM `scholar-version2.scholar_raw_data.author_latest`
    WHERE JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') IS NOT NULL
  ),
  CombinedData AS (
    -- Combine author metrics with publication counts and first-pub year
    -- Only include authors who have at least one cited publication (year_of_first_pub IS NOT NULL)
    SELECT
      s.scholar_id,
      COALESCE(pc.total_publications, 0) AS total_publications,
      COALESCE(pc.total_publications_with_citations, 0) AS total_publications_with_citations,
      pc.year_of_first_pub,
      s.hindex,
      s.hindex5y,
      s.citedby,
      s.citedby5y,
      s.i10index,
      s.i10index5y
    FROM ScholarData s
    LEFT JOIN AuthorPubCounts pc ON s.scholar_id = pc.scholar_id
    WHERE pc.year_of_first_pub IS NOT NULL
  ),
  WithPercentiles AS (
    -- Compute PERCENT_RANK for all 8 metrics in a single pass over CombinedData
    SELECT *,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY hindex ASC)                         AS hindex_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY hindex5y ASC)                       AS hindex5y_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY citedby ASC)                        AS citedby_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY citedby5y ASC)                      AS citedby5y_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY i10index ASC)                       AS i10index_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY i10index5y ASC)                     AS i10index5y_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY total_publications ASC)             AS total_publications_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY total_publications_with_citations ASC) AS total_publications_with_citations_pct
    FROM CombinedData
  )
-- Pivot to normalized (year_of_first_pub, metric_name, metric_value, percentile) format.
-- DISTINCT collapses tied values that share the same PERCENT_RANK.
SELECT DISTINCT year_of_first_pub, 'hindex'                         AS metric_name, hindex                         AS metric_value, hindex_pct                         AS percentile FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, 'hindex5y',                        hindex5y,                        hindex5y_pct                        FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, 'citedby',                         citedby,                         citedby_pct                         FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, 'citedby5y',                       citedby5y,                       citedby5y_pct                       FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, 'i10index',                        i10index,                        i10index_pct                        FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, 'i10index5y',                      i10index5y,                      i10index5y_pct                      FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, 'total_publications',              total_publications,              total_publications_pct              FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, 'total_publications_with_citations', total_publications_with_citations, total_publications_with_citations_pct FROM WithPercentiles;
