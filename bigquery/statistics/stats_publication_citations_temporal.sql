CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_publication_citations_temporal` AS
-- Tier 1: Raw temporal citation statistics — no percentiles, no PERCENT_RANK.
-- Computes yearly and cumulative citation counts per publication per year.
-- Percentiles are added by ranked_publication_citations_temporal (Tier 3)
-- via dist_publication_citations_temporal.
WITH
  ExtractedData AS (
    SELECT
      SPLIT(JSON_EXTRACT_SCALAR(DATA, '$.data.author_pub_id'), ':')[SAFE_OFFSET(0)] AS scholar_id,
      JSON_EXTRACT_SCALAR(DATA, '$.data.author_pub_id') AS author_pub_id,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.bib.pub_year') AS INT64) AS pub_year,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.num_citations') AS INT64) AS total_citations,
      JSON_QUERY(DATA, '$.data.cites_per_year') AS cites_per_year
    FROM `scholar-version2.scholar_raw_data.pub_latest`
  ),
  ExplodedData AS (
    SELECT
      ed.scholar_id,
      ed.author_pub_id,
      CAST(ed.pub_year AS INT64) AS pub_year,
      ed.total_citations,
      SAFE_CAST(json_key AS INT64) AS citation_year,
      SAFE_CAST(json_key AS INT64) - CAST(ed.pub_year AS INT64) AS age,
      CAST(JSON_VALUE(PARSE_JSON(ed.cites_per_year)[json_key]) AS INT64) AS yearly_citations
    FROM ExtractedData ed
    CROSS JOIN UNNEST(JSON_KEYS(PARSE_JSON(ed.cites_per_year))) AS json_key
    WHERE ed.pub_year IS NOT NULL
      AND SAFE_CAST(json_key AS INT64) IS NOT NULL
      AND SAFE.PARSE_JSON(ed.cites_per_year) IS NOT NULL
  ),
  YearSeries AS (
    SELECT
      scholar_id,
      author_pub_id,
      pub_year,
      GENERATE_ARRAY(pub_year, EXTRACT(YEAR FROM CURRENT_DATE())) AS year_series
    FROM ExtractedData
  ),
  ExplodedYearSeries AS (
    SELECT scholar_id, author_pub_id, pub_year, year
    FROM YearSeries
    CROSS JOIN UNNEST(year_series) AS year
  )
SELECT
  eys.scholar_id,
  eys.author_pub_id,
  eys.pub_year,
  eys.year - eys.pub_year AS age,
  eys.year AS citation_year,
  COALESCE(ed.yearly_citations, 0) AS yearly_citations,
  SUM(COALESCE(ed.yearly_citations, 0)) OVER (
    PARTITION BY eys.author_pub_id ORDER BY eys.year
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_citations
FROM ExplodedYearSeries eys
LEFT JOIN ExplodedData ed
  ON eys.scholar_id = ed.scholar_id
  AND eys.author_pub_id = ed.author_pub_id
  AND eys.year = ed.citation_year;
