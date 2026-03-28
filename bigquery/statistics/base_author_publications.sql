CREATE OR REPLACE VIEW `scholar-version2.statistics.base_author_publications` AS
-- Level 1: Explode S2 papers' authors array into (author, paper, year) rows.
-- Source: s2_data.papers (authors JSON array contains [{authorId, name}, ...]).
-- Each paper produces one row per author.
SELECT
  LAX_STRING(a.authorId) AS scholar_id,
  CAST(p.corpusid AS STRING) AS author_pub_id,
  p.year AS pub_year
FROM
  `scholar-version2.s2_data.papers` p,
  UNNEST(JSON_QUERY_ARRAY(p.authors)) AS a
WHERE
  LAX_STRING(a.authorId) IS NOT NULL
  AND p.corpusid IS NOT NULL
  AND p.year IS NOT NULL
  AND p.year > 1900
  AND p.year <= EXTRACT(YEAR FROM CURRENT_DATE());
