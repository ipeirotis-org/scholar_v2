-- 05: Author -> article links with the author's career start, plus the article's percentile arrays from 04.
--
-- start_year = year of the author's first article in S2. It never depends on information after any evaluation year
-- Y >= start_year (the first article is always in the past). Only articles count (see paper/README.md), both for
-- the start year and for paper counts, so untyped S2 records do not inflate productivity.
-- author_paper_bridge can repeat a link; DISTINCT removes repeats.
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.author_articles`
PARTITION BY RANGE_BUCKET(start_year, GENERATE_ARRAY(1950, 2026, 1))
CLUSTER BY authorid
OPTIONS (description = 'S2 author-article links with career start (first article year) and per-age article percentiles. Built by paper/sql/05_author_articles.sql')
AS
WITH
  links AS (
    SELECT DISTINCT b.authorid, b.corpusid
    FROM `scholar-version2.s2_data.author_paper_bridge` AS b
    WHERE b.authorid IS NOT NULL
  )
SELECT
  l.authorid,
  MIN(a.pub_year) OVER (PARTITION BY l.authorid) AS start_year,
  a.corpusid,
  a.pub_year,
  a.cites,
  a.below,
  a.hazen
FROM links AS l
JOIN `scholar-version2.paper_rp.article_percentiles` AS a
  ON a.corpusid = l.corpusid;
