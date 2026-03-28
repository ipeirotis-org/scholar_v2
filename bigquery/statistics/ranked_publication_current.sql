CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_publication_current` AS
-- Level 2: Publication stats enriched with citation percentile.
-- Source: stats_publication_current (L1) + dist_publication_citations (L1).
-- Note: scholar_id is NOT included (S2 papers are identified by corpusid alone).
-- The author dimension is added downstream by joining with base_author_publications.
-- Scalar subquery against the small dist table is orders of magnitude faster
-- than a range join (d.num_citations <= p.num_citations) across all publications.
SELECT
  p.author_pub_id,
  p.title,
  p.author,
  p.pub_year,
  p.num_citations,
  COALESCE(
    (SELECT MAX(d.num_citations_percentile)
     FROM `scholar-version2.statistics.dist_publication_citations` d
     WHERE d.pub_year = p.pub_year AND d.num_citations <= p.num_citations),
    0.0
  ) AS num_citations_percentile
FROM `scholar-version2.statistics.stats_publication_current` p;
