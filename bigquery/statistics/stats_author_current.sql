CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_author_current` AS
-- Level 2: Raw author statistics — no percentiles, no PERCENT_RANK.
-- Computes author metrics (h-index, citations, i10-index, publication counts)
-- from the latest deduplicated author and publication data.
-- Depends on: base_author_publications (L1), stats_publication_current (L1).
-- Percentiles are added by ranked_author_current (L3) via dist_author_metrics.
WITH
  ScholarData AS (
    SELECT
      scholar_id,
      name,
      affiliation,
      email_domain,
      hindex,
      hindex5y,
      citedby,
      citedby5y,
      i10index,
      i10index5y,
      timestamp
    FROM `scholar-version2.scholar_raw_data.author_latest_table`
    WHERE scholar_id IS NOT NULL
  ),
  AuthorPubsData AS (
    SELECT scholar_id, author_pub_id, pub_year
    FROM `scholar-version2.statistics.base_author_publications`
    WHERE pub_year > 1950 AND pub_year <= EXTRACT(YEAR FROM CURRENT_DATE())
  ),
  PublicationCounts AS (
    SELECT
      apd.scholar_id,
      COUNT(apd.author_pub_id) AS total_publications_calculated,
      COUNT(IF(ps.num_citations > 0, apd.author_pub_id, NULL)) AS total_publications_with_citations_calculated
    FROM AuthorPubsData apd
    LEFT JOIN `scholar-version2.statistics.stats_publication_current` ps
      ON apd.author_pub_id = ps.author_pub_id
    GROUP BY apd.scholar_id
  ),
  FirstPubYear AS (
    SELECT apd.scholar_id, MIN(apd.pub_year) AS year_of_first_pub
    FROM AuthorPubsData apd
    JOIN `scholar-version2.statistics.stats_publication_current` ps
      ON apd.author_pub_id = ps.author_pub_id
    WHERE ps.num_citations > 0
    GROUP BY apd.scholar_id
  )
SELECT
  sd.scholar_id,
  sd.name,
  sd.affiliation,
  sd.email_domain,
  sd.hindex,
  sd.hindex5y,
  sd.citedby,
  sd.citedby5y,
  sd.i10index,
  sd.i10index5y,
  COALESCE(pc.total_publications_calculated, 0) AS total_publications,
  COALESCE(pc.total_publications_with_citations_calculated, 0) AS total_publications_with_citations,
  fpy.year_of_first_pub,
  sd.timestamp AS last_updated
FROM ScholarData sd
LEFT JOIN FirstPubYear fpy ON sd.scholar_id = fpy.scholar_id
LEFT JOIN PublicationCounts pc ON sd.scholar_id = pc.scholar_id;
