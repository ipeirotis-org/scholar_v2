-- 01: One row per S2 paper with cumulative citations by age.
--
-- Age t = citations received through calendar year pub_year + t (age 0 = publication year, plus any
-- earlier-dated citations from preprints). c{t} is NULL when pub_year + t is after the last complete
-- citing year (2025), so every non-NULL value is fully observed.
--
-- Keeps uncited papers (all zeros) and uses only publication-time attributes for filtering:
--   fields     distinct S2 fields of study (whole counting; empty when S2 has none)
--   pub_types  S2 publication types (empty for about half of all records)
--   is_article typed as a research article (see paper/README.md), not editorial/news/letter
--
-- Source snapshot: S2 release 2026-10-27 (papers + citations loaded 2026-11-01).
CREATE SCHEMA IF NOT EXISTS `scholar-version2.paper_rp`
  OPTIONS (location = 'US', description = 'Analysis tables for the rank percentile / PiP-AUC paper (paper/ in scholar_v2)');

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.paper_citations_by_age`
PARTITION BY RANGE_BUCKET(pub_year, GENERATE_ARRAY(1950, 2026, 1))
CLUSTER BY is_article
OPTIONS (description = 'Cumulative citations at ages 0-30 for every S2 paper 1950-2025; NULL when not yet observable. Built by paper/sql/01_paper_citations_by_age.sql')
AS
WITH
  papers AS (
    SELECT
      corpusid,
      year AS pub_year,
      ARRAY(
        SELECT DISTINCT JSON_VALUE(f, '$.category')
        FROM UNNEST(JSON_QUERY_ARRAY(s2fieldsofstudy)) AS f
        WHERE JSON_VALUE(f, '$.category') IS NOT NULL
      ) AS fields,
      ARRAY(
        SELECT DISTINCT JSON_VALUE(t)
        FROM UNNEST(JSON_QUERY_ARRAY(publicationtypes)) AS t
        WHERE JSON_VALUE(t) IS NOT NULL
      ) AS pub_types,
      NULLIF(TRIM(venue), '') IS NOT NULL AS has_venue,
      referencecount
    FROM `scholar-version2.s2_data.papers`
    WHERE year BETWEEN 1950 AND 2025
  ),
  cites AS (
    SELECT
      p.corpusid,
    SUM(IF(c.citing_year <= p.pub_year + 0, c.citation_count, 0)) AS c00,
    SUM(IF(c.citing_year <= p.pub_year + 1, c.citation_count, 0)) AS c01,
    SUM(IF(c.citing_year <= p.pub_year + 2, c.citation_count, 0)) AS c02,
    SUM(IF(c.citing_year <= p.pub_year + 3, c.citation_count, 0)) AS c03,
    SUM(IF(c.citing_year <= p.pub_year + 4, c.citation_count, 0)) AS c04,
    SUM(IF(c.citing_year <= p.pub_year + 5, c.citation_count, 0)) AS c05,
    SUM(IF(c.citing_year <= p.pub_year + 6, c.citation_count, 0)) AS c06,
    SUM(IF(c.citing_year <= p.pub_year + 7, c.citation_count, 0)) AS c07,
    SUM(IF(c.citing_year <= p.pub_year + 8, c.citation_count, 0)) AS c08,
    SUM(IF(c.citing_year <= p.pub_year + 9, c.citation_count, 0)) AS c09,
    SUM(IF(c.citing_year <= p.pub_year + 10, c.citation_count, 0)) AS c10,
    SUM(IF(c.citing_year <= p.pub_year + 11, c.citation_count, 0)) AS c11,
    SUM(IF(c.citing_year <= p.pub_year + 12, c.citation_count, 0)) AS c12,
    SUM(IF(c.citing_year <= p.pub_year + 13, c.citation_count, 0)) AS c13,
    SUM(IF(c.citing_year <= p.pub_year + 14, c.citation_count, 0)) AS c14,
    SUM(IF(c.citing_year <= p.pub_year + 15, c.citation_count, 0)) AS c15,
    SUM(IF(c.citing_year <= p.pub_year + 16, c.citation_count, 0)) AS c16,
    SUM(IF(c.citing_year <= p.pub_year + 17, c.citation_count, 0)) AS c17,
    SUM(IF(c.citing_year <= p.pub_year + 18, c.citation_count, 0)) AS c18,
    SUM(IF(c.citing_year <= p.pub_year + 19, c.citation_count, 0)) AS c19,
    SUM(IF(c.citing_year <= p.pub_year + 20, c.citation_count, 0)) AS c20,
    SUM(IF(c.citing_year <= p.pub_year + 21, c.citation_count, 0)) AS c21,
    SUM(IF(c.citing_year <= p.pub_year + 22, c.citation_count, 0)) AS c22,
    SUM(IF(c.citing_year <= p.pub_year + 23, c.citation_count, 0)) AS c23,
    SUM(IF(c.citing_year <= p.pub_year + 24, c.citation_count, 0)) AS c24,
    SUM(IF(c.citing_year <= p.pub_year + 25, c.citation_count, 0)) AS c25,
    SUM(IF(c.citing_year <= p.pub_year + 26, c.citation_count, 0)) AS c26,
    SUM(IF(c.citing_year <= p.pub_year + 27, c.citation_count, 0)) AS c27,
    SUM(IF(c.citing_year <= p.pub_year + 28, c.citation_count, 0)) AS c28,
    SUM(IF(c.citing_year <= p.pub_year + 29, c.citation_count, 0)) AS c29,
    SUM(IF(c.citing_year <= p.pub_year + 30, c.citation_count, 0)) AS c30,
    SUM(c.citation_count) AS c_total
    FROM papers AS p
    JOIN `scholar-version2.s2_data.paper_citations_by_year` AS c
      ON c.citedcorpusid = p.corpusid
    WHERE c.citing_year <= 2025
    GROUP BY p.corpusid
  )
SELECT
  p.corpusid,
  p.pub_year,
  p.fields,
  p.pub_types,
  p.has_venue,
  p.referencecount,
  EXISTS (SELECT 1 FROM UNNEST(p.pub_types) AS t
          WHERE t IN ('JournalArticle', 'Conference', 'Review', 'Study', 'ClinicalTrial', 'MetaAnalysis', 'CaseReport'))
    AND NOT EXISTS (SELECT 1 FROM UNNEST(p.pub_types) AS t
                    WHERE t IN ('Editorial', 'News', 'LettersAndComments')) AS is_article,
  IFNULL(c.c_total, 0) AS c_through_last_year,
  IF(p.pub_year + 0 <= 2025, IFNULL(c.c00, 0), NULL) AS c00,
  IF(p.pub_year + 1 <= 2025, IFNULL(c.c01, 0), NULL) AS c01,
  IF(p.pub_year + 2 <= 2025, IFNULL(c.c02, 0), NULL) AS c02,
  IF(p.pub_year + 3 <= 2025, IFNULL(c.c03, 0), NULL) AS c03,
  IF(p.pub_year + 4 <= 2025, IFNULL(c.c04, 0), NULL) AS c04,
  IF(p.pub_year + 5 <= 2025, IFNULL(c.c05, 0), NULL) AS c05,
  IF(p.pub_year + 6 <= 2025, IFNULL(c.c06, 0), NULL) AS c06,
  IF(p.pub_year + 7 <= 2025, IFNULL(c.c07, 0), NULL) AS c07,
  IF(p.pub_year + 8 <= 2025, IFNULL(c.c08, 0), NULL) AS c08,
  IF(p.pub_year + 9 <= 2025, IFNULL(c.c09, 0), NULL) AS c09,
  IF(p.pub_year + 10 <= 2025, IFNULL(c.c10, 0), NULL) AS c10,
  IF(p.pub_year + 11 <= 2025, IFNULL(c.c11, 0), NULL) AS c11,
  IF(p.pub_year + 12 <= 2025, IFNULL(c.c12, 0), NULL) AS c12,
  IF(p.pub_year + 13 <= 2025, IFNULL(c.c13, 0), NULL) AS c13,
  IF(p.pub_year + 14 <= 2025, IFNULL(c.c14, 0), NULL) AS c14,
  IF(p.pub_year + 15 <= 2025, IFNULL(c.c15, 0), NULL) AS c15,
  IF(p.pub_year + 16 <= 2025, IFNULL(c.c16, 0), NULL) AS c16,
  IF(p.pub_year + 17 <= 2025, IFNULL(c.c17, 0), NULL) AS c17,
  IF(p.pub_year + 18 <= 2025, IFNULL(c.c18, 0), NULL) AS c18,
  IF(p.pub_year + 19 <= 2025, IFNULL(c.c19, 0), NULL) AS c19,
  IF(p.pub_year + 20 <= 2025, IFNULL(c.c20, 0), NULL) AS c20,
  IF(p.pub_year + 21 <= 2025, IFNULL(c.c21, 0), NULL) AS c21,
  IF(p.pub_year + 22 <= 2025, IFNULL(c.c22, 0), NULL) AS c22,
  IF(p.pub_year + 23 <= 2025, IFNULL(c.c23, 0), NULL) AS c23,
  IF(p.pub_year + 24 <= 2025, IFNULL(c.c24, 0), NULL) AS c24,
  IF(p.pub_year + 25 <= 2025, IFNULL(c.c25, 0), NULL) AS c25,
  IF(p.pub_year + 26 <= 2025, IFNULL(c.c26, 0), NULL) AS c26,
  IF(p.pub_year + 27 <= 2025, IFNULL(c.c27, 0), NULL) AS c27,
  IF(p.pub_year + 28 <= 2025, IFNULL(c.c28, 0), NULL) AS c28,
  IF(p.pub_year + 29 <= 2025, IFNULL(c.c29, 0), NULL) AS c29,
  IF(p.pub_year + 30 <= 2025, IFNULL(c.c30, 0), NULL) AS c30
FROM papers AS p
LEFT JOIN cites AS c USING (corpusid);
