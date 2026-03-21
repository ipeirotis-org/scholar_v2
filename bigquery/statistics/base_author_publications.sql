CREATE OR REPLACE VIEW `scholar-version2.statistics.base_author_publications` AS
SELECT
  scholar_id,
  JSON_EXTRACT_SCALAR(pub, '$.author_pub_id') AS author_pub_id,
  CAST(JSON_EXTRACT_SCALAR(pub, '$.bib.pub_year') AS INT64) AS pub_year
FROM
  `scholar-version2.scholar_raw_data.author_latest_table`,
  UNNEST(publications) AS pub
WHERE
  JSON_EXTRACT_SCALAR(pub, '$.author_pub_id') IS NOT NULL
  AND scholar_id IS NOT NULL
  AND CAST(JSON_EXTRACT_SCALAR(pub, '$.bib.pub_year') AS INT64) IS NOT NULL;
