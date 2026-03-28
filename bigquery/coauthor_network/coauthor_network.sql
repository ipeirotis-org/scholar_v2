CREATE OR REPLACE VIEW `scholar-version2.statistics.coauthor_network` AS
-- Derive coauthor relationships from S2 papers' authors arrays.
-- Two authors are coauthors if they appear on the same paper.
-- Uses != (not <) so every author appears as both primary and coauthor,
-- ensuring coauthors_to_add can find missing authors regardless of ID ordering.
-- Note: email_domain is not available in S2, so primary_email_domain is NULL.
WITH paper_authors AS (
  SELECT
    p.corpusid,
    LAX_STRING(a.authorId) AS authorid,
    LAX_STRING(a.name) AS name
  FROM `scholar-version2.s2_data.papers` p,
       UNNEST(JSON_QUERY_ARRAY(p.authors)) AS a
  WHERE LAX_STRING(a.authorId) IS NOT NULL
)
SELECT
  a1.authorid AS primary_scholar_id,
  auth.name AS primary_name,
  LAX_STRING(JSON_QUERY_ARRAY(auth.affiliations)[SAFE_OFFSET(0)]) AS primary_affiliation,
  CAST(NULL AS STRING) AS primary_email_domain,
  a2.authorid AS coauthor_scholar_id,
  a2.name AS coauthor_name,
  CAST(NULL AS STRING) AS coauthor_affiliation
FROM paper_authors a1
JOIN paper_authors a2
  ON a1.corpusid = a2.corpusid AND a1.authorid != a2.authorid
JOIN `scholar-version2.s2_data.authors` auth
  ON a1.authorid = auth.authorid;
