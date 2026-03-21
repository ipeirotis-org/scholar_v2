CREATE OR REPLACE VIEW `scholar-version2.statistics.coauthor_network` AS
SELECT
  scholar_id AS primary_scholar_id,
  name AS primary_name,
  affiliation AS primary_affiliation,
  email_domain AS primary_email_domain,
  JSON_EXTRACT_SCALAR(coauthor, '$.scholar_id') AS coauthor_scholar_id,
  JSON_EXTRACT_SCALAR(coauthor, '$.name') AS coauthor_name,
  JSON_EXTRACT_SCALAR(coauthor, '$.affiliation') AS coauthor_affiliation
FROM
  `scholar-version2.scholar_raw_data.author_latest_table`,
  UNNEST(coauthors) AS coauthor
