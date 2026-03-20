CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_publication_citations_temporal` AS
-- Level 3: Temporal publication citation stats enriched with 4 percentile columns.
-- Joins stats_publication_citations_temporal (L1) against
-- dist_publication_citations_temporal (L2) using floor lookups.
WITH
  base AS (
    SELECT * FROM `scholar-version2.statistics.stats_publication_citations_temporal`
  ),
  -- Floor lookup for yearly_citations percentile partitioned by (pub_year, citation_year)
  perc_pub_year_yearly AS (
    SELECT b.scholar_id, b.author_pub_id, b.citation_year,
      MAX(d.percentile) AS perc_pub_year_yearly_citations
    FROM base b
    JOIN `scholar-version2.statistics.dist_publication_citations_temporal` d
      ON d.metric_name = 'pub_year_yearly_citations'
     AND d.pub_year = b.pub_year
     AND d.citation_year = b.citation_year
     AND d.metric_value <= b.yearly_citations
    GROUP BY b.scholar_id, b.author_pub_id, b.citation_year
  ),
  -- Floor lookup for cumulative_citations percentile partitioned by (pub_year, citation_year)
  perc_pub_year_cumul AS (
    SELECT b.scholar_id, b.author_pub_id, b.citation_year,
      MAX(d.percentile) AS perc_pub_year_cumulative_citations
    FROM base b
    JOIN `scholar-version2.statistics.dist_publication_citations_temporal` d
      ON d.metric_name = 'pub_year_cumulative_citations'
     AND d.pub_year = b.pub_year
     AND d.citation_year = b.citation_year
     AND d.metric_value <= b.cumulative_citations
    GROUP BY b.scholar_id, b.author_pub_id, b.citation_year
  ),
  -- Floor lookup for yearly_citations percentile partitioned by (age)
  perc_age_yearly AS (
    SELECT b.scholar_id, b.author_pub_id, b.citation_year,
      MAX(d.percentile) AS perc_age_yearly_citations
    FROM base b
    JOIN `scholar-version2.statistics.dist_publication_citations_temporal` d
      ON d.metric_name = 'age_yearly_citations'
     AND d.age = b.age
     AND d.metric_value <= b.yearly_citations
    GROUP BY b.scholar_id, b.author_pub_id, b.citation_year
  ),
  -- Floor lookup for cumulative_citations percentile partitioned by (age)
  perc_age_cumul AS (
    SELECT b.scholar_id, b.author_pub_id, b.citation_year,
      MAX(d.percentile) AS perc_age_cumulative_citations
    FROM base b
    JOIN `scholar-version2.statistics.dist_publication_citations_temporal` d
      ON d.metric_name = 'age_cumulative_citations'
     AND d.age = b.age
     AND d.metric_value <= b.cumulative_citations
    GROUP BY b.scholar_id, b.author_pub_id, b.citation_year
  )
SELECT
  b.scholar_id,
  b.author_pub_id,
  b.pub_year,
  b.age,
  b.citation_year,
  b.yearly_citations,
  b.cumulative_citations,
  COALESCE(pyy.perc_pub_year_yearly_citations, 0.0)     AS perc_pub_year_yearly_citations,
  COALESCE(pyc.perc_pub_year_cumulative_citations, 0.0)  AS perc_pub_year_cumulative_citations,
  COALESCE(pay.perc_age_yearly_citations, 0.0)           AS perc_age_yearly_citations,
  COALESCE(pac.perc_age_cumulative_citations, 0.0)       AS perc_age_cumulative_citations
FROM base b
LEFT JOIN perc_pub_year_yearly pyy ON pyy.scholar_id = b.scholar_id AND pyy.author_pub_id = b.author_pub_id AND pyy.citation_year = b.citation_year
LEFT JOIN perc_pub_year_cumul  pyc ON pyc.scholar_id = b.scholar_id AND pyc.author_pub_id = b.author_pub_id AND pyc.citation_year = b.citation_year
LEFT JOIN perc_age_yearly      pay ON pay.scholar_id = b.scholar_id AND pay.author_pub_id = b.author_pub_id AND pay.citation_year = b.citation_year
LEFT JOIN perc_age_cumul       pac ON pac.scholar_id = b.scholar_id AND pac.author_pub_id = b.author_pub_id AND pac.citation_year = b.citation_year;
