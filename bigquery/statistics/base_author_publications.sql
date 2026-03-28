CREATE OR REPLACE VIEW `scholar-version2.statistics.base_author_publications` AS
-- Level 1: Author-paper bridge — one row per (author, paper, year).
-- Source: s2_data.author_paper_bridge (materialized during dataset ingestion).
-- Using the pre-materialized bridge table instead of UNNEST(papers.authors)
-- enables BigQuery to push scholar_id predicates down efficiently.
-- Without this, every per-author query would scan all 200M papers.
SELECT
  authorid AS scholar_id,
  CAST(corpusid AS STRING) AS author_pub_id,
  pub_year
FROM
  `scholar-version2.s2_data.author_paper_bridge`
WHERE
  authorid IS NOT NULL
  AND pub_year IS NOT NULL
  AND pub_year > 1900
  AND pub_year <= EXTRACT(YEAR FROM CURRENT_DATE());
