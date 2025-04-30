CREATE VIEW statistics.base_author_publications AS
SELECT
  JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') AS scholar_id,
  JSON_EXTRACT_SCALAR(pub, '$.author_pub_id') AS author_pub_id,
  CAST(JSON_EXTRACT_SCALAR(pub, '$.bib.pub_year') AS INT64) AS pub_year
FROM
  `scholar-version2.firestore_export.scholar_raw_author_raw_latest`,
  UNNEST(JSON_EXTRACT_ARRAY(DATA, '$.data.publications')) AS pub
WHERE
  JSON_EXTRACT_SCALAR(pub, '$.author_pub_id') IS NOT NULL
  AND JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') IS NOT NULL
  AND CAST(JSON_EXTRACT_SCALAR(pub, '$.bib.pub_year') AS INT64) IS NOT NULL;