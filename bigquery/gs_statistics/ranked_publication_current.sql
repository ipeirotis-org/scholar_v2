CREATE OR REPLACE VIEW `scholar-version2.gs_statistics.ranked_publication_current` AS
-- Level 2: Publication stats enriched with citation percentile.
-- Source: stats_publication_current (L1) + dist_publication_citations (L1).
-- GS version: keeps scholar_id and venue (GS papers are identified per author).
-- Scalar subquery against the small dist table is orders of magnitude faster
-- than a range join (d.num_citations <= p.num_citations) across all publications.
SELECT
  p.scholar_id,
  p.author_pub_id,
  p.title,
  p.author,
  p.venue,
  p.pub_year,
  p.num_citations,
  COALESCE(
    (SELECT MAX(d.num_citations_percentile)
     FROM `scholar-version2.gs_statistics.dist_publication_citations` d
     WHERE d.pub_year = p.pub_year AND d.num_citations <= p.num_citations),
    0.0
  ) AS num_citations_percentile
FROM `scholar-version2.gs_statistics.stats_publication_current` p;
