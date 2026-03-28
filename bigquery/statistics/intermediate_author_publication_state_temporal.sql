CREATE OR REPLACE VIEW `scholar-version2.statistics.intermediate_author_publication_state_temporal` AS
-- Level 2: Per-publication per-year citation state for each author.
-- Joins base_author_publications (L1) with stats_publication_citations_temporal (L1).
-- Note: join is on author_pub_id only (corpusid uniquely identifies a paper in S2).

-- Combine author's publications with their yearly citation state
SELECT
  ap.scholar_id,
  CAST(ap.author_pub_id AS STRING) AS author_pub_id,
  ap.pub_year,
  ypc.citation_year AS state_year,
  ypc.cumulative_citations AS cumulative_citations_at_state_year,
  ypc.yearly_citations AS yearly_citations_at_state_year
FROM
  `scholar-version2.statistics.base_author_publications` ap
JOIN
  `scholar-version2.statistics.stats_publication_citations_temporal` ypc
    ON ap.author_pub_id = ypc.author_pub_id;
