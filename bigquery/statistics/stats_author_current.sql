CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_author_current` AS
-- Level 2: Raw author statistics — no percentiles, no PERCENT_RANK.
-- Source: s2_data.authors + s2_data.author_paper_stats (derived table).
-- Computes author metrics (h-index, citations, i10-index, publication counts).
-- Depends on: base_author_publications (L1), stats_publication_current (L1).
-- Percentiles are added by ranked_author_current (L3) via dist_author_metrics.
--
-- Dropped vs Google Scholar version:
--   hindex5y, citedby5y, i10index5y (not available in S2; NULL compatibility columns kept)
--   email_domain (not available in S2; NULL compatibility column kept)
WITH
  AuthorData AS (
    SELECT
      authorid AS scholar_id,
      name,
      -- Extract first affiliation from JSON array
      LAX_STRING(JSON_QUERY_ARRAY(affiliations)[SAFE_OFFSET(0)]) AS affiliation,
      hindex,
      citationcount AS citedby
    FROM `scholar-version2.s2_data.authors`
    WHERE authorid IS NOT NULL
  ),
  PaperStats AS (
    SELECT
      authorid AS scholar_id,
      total_publications,
      total_publications_with_citations,
      i10_index AS i10index,
      year_of_first_pub
    FROM `scholar-version2.s2_data.author_paper_stats`
    -- Only include authors with >= 6 total publications.
    WHERE total_publications >= 6
  )
SELECT
  ad.scholar_id,
  ad.name,
  ad.affiliation,
  CAST(NULL AS STRING) AS email_domain,
  ad.hindex,
  CAST(NULL AS INT64) AS hindex5y,
  ad.citedby,
  CAST(NULL AS INT64) AS citedby5y,
  ps.i10index,
  CAST(NULL AS INT64) AS i10index5y,
  ps.total_publications,
  ps.total_publications_with_citations,
  ps.year_of_first_pub,
  CURRENT_TIMESTAMP() AS last_updated
FROM AuthorData ad
INNER JOIN PaperStats ps ON ad.scholar_id = ps.scholar_id;
