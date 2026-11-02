CREATE OR REPLACE VIEW `scholar-version2.gs_statistics.stats_author_current` AS
-- Level 2: raw author statistics (no percentiles).
-- Source: gs_data.authors_latest (GS profile metrics, including i10 and the
-- "since YYYY" columns, which fill the *5y compatibility columns) plus
-- publication counts from base_author_publications / stats_publication_current.
--
-- year_of_first_pub is the earliest *cited* paper with a plausible year: GS
-- profiles often carry stray uncited items with wrong early dates, which would
-- otherwise put an author into a much older cohort.
-- Percentiles are added by ranked_author_current via dist_author_metrics.
WITH
  PubCounts AS (
    SELECT scholar_id, COUNT(*) AS total_publications
    FROM `scholar-version2.gs_statistics.base_author_publications`
    GROUP BY scholar_id
  ),
  CitedPubs AS (
    SELECT
      scholar_id,
      COUNT(*) AS total_publications_with_citations,
      MIN(pub_year) AS year_of_first_pub
    FROM `scholar-version2.gs_statistics.stats_publication_current`
    GROUP BY scholar_id
  )
SELECT
  a.scholar_id,
  a.name,
  a.affiliation,
  a.email_domain,
  a.hindex,
  a.hindex_since AS hindex5y,
  a.citedby,
  a.citedby_since AS citedby5y,
  a.i10index,
  a.i10index_since AS i10index5y,
  COALESCE(pc.total_publications, 0) AS total_publications,
  COALESCE(cp.total_publications_with_citations, 0) AS total_publications_with_citations,
  cp.year_of_first_pub,
  a.fetched_at AS last_updated
FROM `scholar-version2.gs_data.authors_latest` a
LEFT JOIN PubCounts pc USING (scholar_id)
LEFT JOIN CitedPubs cp USING (scholar_id);
