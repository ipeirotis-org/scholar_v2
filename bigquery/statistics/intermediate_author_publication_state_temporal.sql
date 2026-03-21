CREATE OR REPLACE VIEW `scholar-version2.statistics.intermediate_author_publication_state_temporal` AS
-- Level 2: Per-publication per-year citation state for each author.
-- Reuses base_author_publications (L1) instead of re-parsing author_latest JSON.
-- Depends on: base_author_publications (L1), stats_publication_citations_temporal (L1).

-- Combine author's publications with their yearly citation state
SELECT
  ap.scholar_id,
  ap.author_pub_id,
  ap.pub_year,
  ypc.citation_year AS state_year,
  ypc.cumulative_citations AS cumulative_citations_at_state_year,
  ypc.yearly_citations AS yearly_citations_at_state_year
FROM
  `scholar-version2.statistics.base_author_publications` ap
JOIN
  `scholar-version2.statistics.stats_publication_citations_temporal` ypc
    ON ap.author_pub_id = ypc.author_pub_id AND ap.scholar_id = ypc.scholar_id;
