CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_publication_citations_temporal` AS
-- Level 1: Raw temporal citation statistics — no percentiles, no PERCENT_RANK.
-- Source: s2_data.papers + s2_data.paper_citations_by_year.
-- Computes yearly and cumulative citation counts per publication per year.
-- Generates a row for every year from pub_year to current year (including 0-citation years)
-- to support correct cumulative sums and temporal percentile lookups.
-- Note: scholar_id is NOT included — paper temporal data is author-independent.
-- The author dimension is added by intermediate_author_publication_state_temporal.
-- Percentiles are added by ranked_publication_citations_temporal (Level 3)
-- via dist_publication_citations_temporal.
WITH
  PaperDetails AS (
    SELECT
      CAST(corpusid AS STRING) AS author_pub_id,
      year AS pub_year
    FROM `scholar-version2.s2_data.papers`
    WHERE year IS NOT NULL
      AND year > 1950
      AND year <= EXTRACT(YEAR FROM CURRENT_DATE())
      AND citationcount > 0
      -- Only include papers from authors with >= 6 total publications.
      AND corpusid IN (
        SELECT corpusid FROM `scholar-version2.s2_data.qualifying_papers`
      )
  ),
  CitationData AS (
    SELECT
      CAST(citedcorpusid AS STRING) AS author_pub_id,
      citing_year AS citation_year,
      citation_count AS yearly_citations
    FROM `scholar-version2.s2_data.paper_citations_by_year`
  ),
  YearSeries AS (
    SELECT
      author_pub_id,
      pub_year,
      year
    FROM PaperDetails
    CROSS JOIN UNNEST(GENERATE_ARRAY(pub_year, EXTRACT(YEAR FROM CURRENT_DATE()))) AS year
  )
SELECT
  ys.author_pub_id,
  ys.pub_year,
  ys.year - ys.pub_year AS age,
  ys.year AS citation_year,
  COALESCE(cd.yearly_citations, 0) AS yearly_citations,
  SUM(COALESCE(cd.yearly_citations, 0)) OVER (
    PARTITION BY ys.author_pub_id ORDER BY ys.year
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_citations
FROM YearSeries ys
LEFT JOIN CitationData cd
  ON ys.author_pub_id = cd.author_pub_id
  AND ys.year = cd.citation_year;
