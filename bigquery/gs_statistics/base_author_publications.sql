CREATE OR REPLACE VIEW `scholar-version2.gs_statistics.base_author_publications` AS
-- Level 1: one row per publication on an author's latest Google Scholar profile.
-- Source: gs_data.publications_latest (profile crawls; legacy scholarly snapshots
-- until each author is re-crawled). author_pub_id is the GS citation_id
-- "SCHOLARID:PUBID", unique per (author, paper).
SELECT
  scholar_id,
  author_pub_id,
  pub_year,
  num_citations
FROM `scholar-version2.gs_data.publications_latest`
WHERE author_pub_id IS NOT NULL
  AND pub_year IS NOT NULL
  AND pub_year > 1900
  AND pub_year <= EXTRACT(YEAR FROM CURRENT_DATE());
