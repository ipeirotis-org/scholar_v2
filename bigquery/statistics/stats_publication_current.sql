CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_publication_current` AS
-- Level 1: Raw publication statistics — no percentiles, no PERCENT_RANK.
-- Source: s2_data.papers. One row per paper (not per author).
-- Note: scholar_id is NOT included here because corpusid uniquely identifies a paper.
-- The author dimension is provided by base_author_publications via JOINs in downstream views.
-- Percentiles are added by ranked_publication_current (Level 2) via dist_publication_citations.
SELECT
  CAST(corpusid AS STRING) AS author_pub_id,
  year AS pub_year,
  title,
  (SELECT STRING_AGG(LAX_STRING(aa.name), ', ')
   FROM UNNEST(JSON_QUERY_ARRAY(authors)) aa) AS author,
  citationcount AS num_citations
FROM `scholar-version2.s2_data.papers`
WHERE year IS NOT NULL
  AND year > 1950
  AND year <= EXTRACT(YEAR FROM CURRENT_DATE())
  AND citationcount > 0;
