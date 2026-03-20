CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_publication_current` AS
-- Level 2: Publication stats enriched with citation percentile.
-- Joins stats_publication_current (L1) against dist_publication_citations (L1)
-- using a floor lookup: MAX(percentile WHERE dist_citations <= actual_citations).
WITH percentile_lookup AS (
  SELECT
    p.author_pub_id,
    MAX(d.num_citations_percentile) AS num_citations_percentile
  FROM `scholar-version2.statistics.stats_publication_current` p
  JOIN `scholar-version2.statistics.dist_publication_citations` d
    ON d.pub_year = p.pub_year
   AND d.num_citations <= p.num_citations
  GROUP BY p.author_pub_id
)
SELECT
  p.scholar_id,
  p.author_pub_id,
  p.title,
  p.author,
  p.pub_year,
  p.num_citations,
  COALESCE(l.num_citations_percentile, 0.0) AS num_citations_percentile,
  p.last_updated
FROM `scholar-version2.statistics.stats_publication_current` p
LEFT JOIN percentile_lookup l ON l.author_pub_id = p.author_pub_id;
