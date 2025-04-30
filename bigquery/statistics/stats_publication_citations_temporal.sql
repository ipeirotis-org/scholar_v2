CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_publication_citations_temporal` 

    AS
WITH
  ExtractedData AS (
  SELECT
    SPLIT(JSON_EXTRACT_SCALAR(DATA, '$.data.author_pub_id'), ':')[SAFE_OFFSET(0)] AS scholar_id,
    JSON_EXTRACT_SCALAR(DATA, '$.data.author_pub_id') AS author_pub_id,
    CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.bib.pub_year') AS INT64) AS pub_year,
    CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.num_citations') AS INT64) AS total_citations,
    JSON_QUERY(DATA, '$.data.cites_per_year') AS cites_per_year
  FROM
    `scholar-version2.firestore_export.scholar_raw_pub_raw_latest`),
ExplodedData AS (
  SELECT
    ed.scholar_id,
    ed.author_pub_id,
    CAST(ed.pub_year AS INT64) AS pub_year,
    ed.total_citations,
    -- Attempt to cast the extracted JSON key (year) to INT64
    SAFE_CAST(json_key AS INT64) AS citation_year,
    SAFE_CAST(json_key AS INT64) - CAST(ed.pub_year AS INT64) AS age,
    -- 1. Parse the string to JSON type
    -- 2. Access the value using the dynamic key `json_key` via the [] operator
    -- 3. Use JSON_VALUE() to extract the scalar value as a STRING from the resulting JSON
    -- 4. Cast the resulting STRING to INT64
    CAST(JSON_VALUE( PARSE_JSON(ed.cites_per_year)[json_key] ) AS INT64) AS yearly_citations
  FROM
    ExtractedData ed
  CROSS JOIN
    -- Use JSON_KEYS with PARSE_JSON to get the keys
    UNNEST(JSON_KEYS(PARSE_JSON(ed.cites_per_year))) AS json_key
  WHERE
    ed.pub_year IS NOT NULL
    -- Ensure the key is actually a number representing a year
    AND SAFE_CAST(json_key AS INT64) IS NOT NULL
    -- Optional but recommended: Handle potential invalid JSON strings
    AND SAFE.PARSE_JSON(ed.cites_per_year) IS NOT NULL
),
  YearSeries AS (
  SELECT
    scholar_id,
    author_pub_id,
    pub_year,
    GENERATE_ARRAY(pub_year, 2025) AS year_series
  FROM
    ExtractedData ),
  ExplodedYearSeries AS (
  SELECT
    scholar_id,
    author_pub_id,
    pub_year,
    year
  FROM
    YearSeries
  CROSS JOIN
    UNNEST(year_series) AS year ),
  JoinedData AS (
  SELECT
    eys.scholar_id,
    eys.author_pub_id,
    eys.pub_year,
    eys.year - eys.pub_year AS age,
    eys.year AS citation_year,
    COALESCE(ed.yearly_citations, 0) AS yearly_citations,
    SUM(COALESCE(ed.yearly_citations, 0)) OVER (PARTITION BY eys.author_pub_id ORDER BY eys.year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_citations
  FROM
    ExplodedYearSeries eys
  LEFT JOIN
    ExplodedData ed
  ON
    eys.scholar_id = ed.scholar_id
    AND eys.author_pub_id = ed.author_pub_id
    AND eys.year = ed.citation_year ),
  PercentileCalculations AS (
  SELECT
    *,
    PERCENT_RANK() OVER(PARTITION BY pub_year, citation_year ORDER BY yearly_citations ASC) AS perc_pub_year_yearly_citations,
    PERCENT_RANK() OVER(PARTITION BY pub_year, citation_year ORDER BY cumulative_citations ASC) AS perc_pub_year_cumulative_citations,
    PERCENT_RANK() OVER(PARTITION BY age ORDER BY yearly_citations ASC) AS perc_age_yearly_citations,
    PERCENT_RANK() OVER(PARTITION BY age ORDER BY cumulative_citations ASC) AS perc_age_cumulative_citations,
  FROM
    JoinedData )
SELECT
  *
FROM
  PercentileCalculations