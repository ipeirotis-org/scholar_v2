CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_publication_citations_temporal` AS
-- Level 3: Temporal publication citation stats enriched with 4 percentile columns.
-- Uses scalar subqueries against dist_publication_citations_temporal instead of
-- range joins, which are orders of magnitude faster for per-paper queries.
-- Note: scholar_id not included (temporal citation data is per-paper in S2).
SELECT
  b.author_pub_id,
  b.pub_year,
  b.age,
  b.citation_year,
  b.yearly_citations,
  b.cumulative_citations,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_publication_citations_temporal` d
     WHERE d.metric_name = 'pub_year_yearly_citations'
       AND d.pub_year = b.pub_year AND d.citation_year = b.citation_year
       AND d.metric_value <= b.yearly_citations),
    0.0) AS perc_pub_year_yearly_citations,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_publication_citations_temporal` d
     WHERE d.metric_name = 'pub_year_cumulative_citations'
       AND d.pub_year = b.pub_year AND d.citation_year = b.citation_year
       AND d.metric_value <= b.cumulative_citations),
    0.0) AS perc_pub_year_cumulative_citations,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_publication_citations_temporal` d
     WHERE d.metric_name = 'age_yearly_citations'
       AND d.age = b.age
       AND d.metric_value <= b.yearly_citations),
    0.0) AS perc_age_yearly_citations,
  COALESCE(
    (SELECT MAX(d.percentile) FROM `scholar-version2.statistics.dist_publication_citations_temporal` d
     WHERE d.metric_name = 'age_cumulative_citations'
       AND d.age = b.age
       AND d.metric_value <= b.cumulative_citations),
    0.0) AS perc_age_cumulative_citations
FROM `scholar-version2.statistics.stats_publication_citations_temporal` b;
