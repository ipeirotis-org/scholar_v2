CREATE OR REPLACE VIEW `scholar-version2.statistics.coauthors_to_add` AS
SELECT
  coauthor_scholar_id,
  coauthor_name,
  coauthor_affiliation,
  primary_email_domain,
  COUNT(*) AS cnt
FROM
  `scholar-version2.statistics.coauthor_network`
WHERE
  coauthor_scholar_id NOT IN (
  SELECT
    JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') AS primary_scholar_id
  FROM
    `scholar-version2.firestore_export.scholar_raw_author_raw_latest`)
GROUP BY
  coauthor_scholar_id,
  coauthor_name,
  coauthor_affiliation,
  primary_email_domain
ORDER BY
  cnt DESC