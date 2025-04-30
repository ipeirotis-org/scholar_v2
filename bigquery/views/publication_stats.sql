CREATE OR REPLACE VIEW `scholar-version2.statistics.publication_stats` AS

SELECT
scholar_id,
  author_pub_id,
  title,
  author,
  pub_year,
  num_citations,
  PERCENT_RANK() OVER(PARTITION BY pub_year ORDER BY num_citations ASC) AS num_citations_percentile,
  timestamp AS last_updated

FROM (
  SELECT
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
  pub_year > 1950 AND pub_year <= EXTRACT(year FROM CURRENT_DATE()) AND num_citations > 0

ORDER BY
  pub_year DESC;
