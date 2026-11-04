-- 00 (self-citation robustness): per-paper citations by citing year, without self-citations.
--
-- Same construction as s2_data.paper_citations_by_year (dataset_ingestion/loader.py: distinct citing/cited pairs,
-- citing year from the citing paper, years 1901..current), minus self-citations: a citation is a self-citation when
-- the citing and the cited paper share at least one S2 author (author_paper_bridge). `self_count` keeps the removed
-- self-citations for reporting.
-- run_noself.sh then reruns the analysis steps against this table in the dataset paper_rp_noself.
CREATE SCHEMA IF NOT EXISTS `scholar-version2.paper_rp_noself`
  OPTIONS (location = 'US', description = 'paper_rp analysis rerun without self-citations (paper/sql/run_noself.sh)');

CREATE OR REPLACE TABLE `scholar-version2.paper_rp_noself.paper_citations_by_year`
CLUSTER BY citedcorpusid
OPTIONS (description = 'Citations by cited paper and citing year, excluding self-citations (shared S2 author). Built by paper/sql/00_noself_citations.sql')
AS
WITH
  deduped AS (
    SELECT DISTINCT citingcorpusid, citedcorpusid
    FROM `scholar-version2.s2_data.citations`
    WHERE citedcorpusid IS NOT NULL AND citingcorpusid IS NOT NULL
  ),
  self_pairs AS (
    SELECT DISTINCT d.citingcorpusid, d.citedcorpusid
    FROM deduped AS d
    JOIN `scholar-version2.s2_data.author_paper_bridge` AS a ON a.corpusid = d.citingcorpusid
    JOIN `scholar-version2.s2_data.author_paper_bridge` AS b
      ON b.corpusid = d.citedcorpusid AND b.authorid = a.authorid
  )
SELECT
  d.citedcorpusid,
  p.year AS citing_year,
  COUNTIF(s.citingcorpusid IS NULL) AS citation_count,
  COUNTIF(s.citingcorpusid IS NOT NULL) AS self_count
FROM deduped AS d
JOIN `scholar-version2.s2_data.papers` AS p ON p.corpusid = d.citingcorpusid
LEFT JOIN self_pairs AS s ON s.citingcorpusid = d.citingcorpusid AND s.citedcorpusid = d.citedcorpusid
WHERE p.year IS NOT NULL AND p.year > 1900 AND p.year <= EXTRACT(YEAR FROM CURRENT_DATE())
GROUP BY d.citedcorpusid, p.year;
