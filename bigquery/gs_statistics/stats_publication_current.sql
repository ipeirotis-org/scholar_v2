CREATE OR REPLACE VIEW `scholar-version2.gs_statistics.stats_publication_current` AS
-- Level 1: raw publication statistics (no percentiles), cited papers only.
-- Source: gs_data.publications_latest. Unlike the S2 version this keeps
-- scholar_id: GS papers are identified per author ("SCHOLARID:PUBID"), so a paper
-- co-authored by two profiled authors appears once per author (as before S2).
-- Percentiles are added by ranked_publication_current via dist_publication_citations.
SELECT
  scholar_id,
  author_pub_id,
  pub_year,
  title,
  authors AS author,
  venue,
  num_citations,
  fetched_at AS last_updated
FROM `scholar-version2.gs_data.publications_latest`
WHERE pub_year IS NOT NULL
  AND pub_year > 1950
  AND pub_year <= EXTRACT(YEAR FROM CURRENT_DATE())
  AND num_citations > 0;
