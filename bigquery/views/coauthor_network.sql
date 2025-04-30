SELECT
  JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') AS primary_scholar_id,
  JSON_EXTRACT_SCALAR(DATA, '$.data.name') AS primary_name,
  JSON_EXTRACT_SCALAR(DATA, '$.data.affiliation') AS primary_affiliation,
  JSON_EXTRACT_SCALAR(DATA, '$.data.email_domain') AS primary_email_domain,
  JSON_EXTRACT_SCALAR(coauthor, '$.scholar_id') AS coauthor_scholar_id,
  JSON_EXTRACT_SCALAR(coauthor, '$.name') AS coauthor_name,
  JSON_EXTRACT_SCALAR(coauthor, '$.affiliation') AS coauthor_affiliation
FROM
  `scholar-version2.firestore_export.scholar_raw_author_raw_latest`,
  UNNEST(JSON_EXTRACT_ARRAY(DATA, '$.data.coauthors')) AS coauthor
