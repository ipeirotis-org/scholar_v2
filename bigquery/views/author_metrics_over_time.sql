WITH FirstPublishingYear AS (
  SELECT
    scholar_id,
    MIN(pub_year) AS first_pub_year
  FROM
    `scholar-version2.statistics.publication_citations`
  GROUP BY
    scholar_id
),
TotalCitations AS (
  SELECT
    pc.scholar_id,
    pc.citation_year,
    COUNT(DISTINCT pc.author_pub_id) AS yearly_publications,
    SUM(pc.yearly_citations) AS total_yearly_citations,
    SUM(pc.cumulative_citations) AS total_cumulative_citations
  FROM
    `scholar-version2.statistics.publication_citations` pc
  JOIN
    FirstPublishingYear fpy ON pc.scholar_id = fpy.scholar_id
  GROUP BY
    pc.scholar_id,
    pc.citation_year
),
PublicationsAndCitations AS (
  SELECT
    tc.*,
    SUM(tc.yearly_publications) OVER (PARTITION BY tc.scholar_id ORDER BY tc.citation_year) AS total_publications,
  FROM
    TotalCitations tc
),
I10Index AS (
  SELECT
    scholar_id,
    citation_year,
    SUM(CASE WHEN cumulative_citations >= 10 THEN 1 ELSE 0 END) AS i10_index
  FROM
    `scholar-version2.statistics.publication_citations`
  GROUP BY
    scholar_id, citation_year
)

SELECT
  pac.scholar_id,
  pac.citation_year,
  pac.total_yearly_citations,
  pac.total_cumulative_citations,
  pac.total_publications,
  pac.yearly_publications,
  i10.i10_index,
  PERCENT_RANK() OVER (PARTITION BY pac.citation_year, fpy.first_pub_year ORDER BY pac.total_cumulative_citations) AS total_citations_percentile_rank,
  PERCENT_RANK() OVER (PARTITION BY pac.citation_year, fpy.first_pub_year ORDER BY pac.total_yearly_citations) AS yearly_citations_percentile_rank,
  PERCENT_RANK() OVER (PARTITION BY pac.citation_year, fpy.first_pub_year ORDER BY pac.total_publications) AS total_publications_percentile_rank,
  PERCENT_RANK() OVER (PARTITION BY pac.citation_year, fpy.first_pub_year ORDER BY pac.yearly_publications) AS yearly_publications_percentile_rank,
  PERCENT_RANK() OVER (PARTITION BY pac.citation_year, fpy.first_pub_year ORDER BY i10.i10_index) AS i10_index_percentile_rank,
FROM
  PublicationsAndCitations pac
JOIN
  FirstPublishingYear fpy ON pac.scholar_id = fpy.scholar_id
LEFT JOIN
  I10Index i10 ON pac.scholar_id = i10.scholar_id AND pac.citation_year = i10.citation_year
