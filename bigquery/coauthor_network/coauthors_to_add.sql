CREATE OR REPLACE VIEW `scholar-version2.statistics.coauthors_to_add` AS
-- Coauthors that appear in papers but don't have an entry in the authors table.
-- With S2 bulk data, most authors should exist, but this catches any gaps.
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
  SELECT authorid
  FROM `scholar-version2.s2_data.authors`
  WHERE authorid IS NOT NULL)
GROUP BY
  coauthor_scholar_id,
  coauthor_name,
  coauthor_affiliation,
  primary_email_domain
ORDER BY
  cnt DESC;
