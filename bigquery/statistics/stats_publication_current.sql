CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_publication_current` AS

-- Selects core publication details and calculates the current citation percentile
SELECT
  scholar_id,
  author_pub_id,
  title,
  author,
  pub_year,
  num_citations,
  -- Calculate percentile rank based on citations within the same publication year
  PERCENT_RANK() OVER(PARTITION BY pub_year ORDER BY num_citations ASC) AS num_citations_percentile,
  timestamp AS last_updated -- Timestamp from the raw publication document

FROM (
  -- Extract details from the raw Firestore publication export
  SELECT
    -- Extract scholar_id from the author_pub_id string
    SPLIT(JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id'), ':')[SAFE_OFFSET(0)] AS scholar_id,
    JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id') AS author_pub_id,
    CAST(JSON_EXTRACT_SCALAR(data, '$.data.bib.pub_year') AS INT64) AS pub_year,
    JSON_EXTRACT_SCALAR(data, '$.data.bib.title') AS title,
    JSON_EXTRACT_SCALAR(data, '$.data.bib.author') AS author,
    CAST(JSON_EXTRACT_SCALAR(data, '$.data.num_citations') AS INT64) AS num_citations,
    timestamp
  FROM
    `scholar-version2.firestore_export.scholar_raw_pub_raw_latest`
)
WHERE
  pub_year > 1950 AND pub_year <= EXTRACT(year FROM CURRENT_DATE()) AND num_citations > 0;
