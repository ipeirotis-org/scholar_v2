CREATE OR REPLACE VIEW `scholar-version2.gs_statistics.stats_author_current` AS
-- Level 2: raw author statistics (no percentiles).
-- Source: gs_data.authors_latest (GS profile metrics, including i10 and the
-- "since YYYY" columns, which fill the *5y compatibility columns) plus
-- publication counts from base_author_publications / stats_publication_current.
--
-- year_of_first_pub sets the career-stage cohort. GS profiles carry stray items:
-- misdated entries and namesakes' papers, often decades before the real career
-- (e.g. Pietro Perona: one untitled 1963 item, then 1988 onward; Anindya Ghose:
-- a 1975 Lancaster library report, then 2002 onward). In order of precedence:
--   1. a manual row in gs_data.start_year_overrides (latest set_at wins);
--   2. the first cited paper after the last gap of more than 5 years whose
--      earlier cited papers are few: at most 2, or 5% of the author's cited
--      papers (2026-11-03: moves 1,689 of 16,207 profiles, 985 by 10+ years);
--   3. the earliest cited paper with a plausible year (uncited items never count).
-- Percentiles are added by ranked_author_current via dist_author_metrics.
WITH
  PubCounts AS (
    SELECT scholar_id, COUNT(*) AS total_publications
    FROM `scholar-version2.gs_statistics.base_author_publications`
    GROUP BY scholar_id
  ),
  CitedYears AS (
    SELECT
      scholar_id,
      pub_year,
      pub_year - LAG(pub_year) OVER w AS gap_years,
      ROW_NUMBER() OVER w - 1 AS cited_before,
      COUNT(*) OVER (PARTITION BY scholar_id) AS cited_total
    FROM `scholar-version2.gs_statistics.stats_publication_current`
    WINDOW w AS (PARTITION BY scholar_id ORDER BY pub_year)
  ),
  CitedPubs AS (
    SELECT
      scholar_id,
      COUNT(*) AS total_publications_with_citations,
      MIN(pub_year) AS earliest_cited_year,
      -- Only the first paper of a year has a nonzero gap, and its cited_before
      -- counts exactly the papers of earlier years.
      MAX(IF(gap_years > 5 AND cited_before <= GREATEST(2, 0.05 * cited_total),
             pub_year, NULL)) AS start_after_strays
    FROM CitedYears
    GROUP BY scholar_id
  ),
  Overrides AS (
    SELECT scholar_id,
           ARRAY_AGG(year_of_first_pub ORDER BY set_at DESC LIMIT 1)[OFFSET(0)] AS year_of_first_pub
    FROM `scholar-version2.gs_data.start_year_overrides`
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
  COALESCE(ov.year_of_first_pub, cp.start_after_strays, cp.earliest_cited_year) AS year_of_first_pub,
  a.fetched_at AS last_updated
FROM `scholar-version2.gs_data.authors_latest` a
LEFT JOIN PubCounts pc USING (scholar_id)
LEFT JOIN CitedPubs cp USING (scholar_id)
LEFT JOIN Overrides ov USING (scholar_id);
