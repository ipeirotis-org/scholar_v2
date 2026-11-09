-- 04: Every article's citation percentile at each observable age, as arrays indexed by age.
--
-- For article j published in year p, element t (t = 0 .. min(30, 2025 - p)) of each array describes j in calendar
-- year p + t, compared with all articles published in year p (reference set 'article', all fields pooled):
--   cites[t]  cumulative citations through p + t
--   below[t]  share of same-year articles with strictly fewer citations (uncited articles score 0); this is the
--             paper's percentile y in PiP-AUC
--   hazen[t]  Hazen percentile with average ranks for ties, (n_less + n_equal / 2) / n_total
-- So the percentile an evaluator would have seen in year Y is below[OFFSET(Y - p)], with no information after Y.
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.article_percentiles`
PARTITION BY RANGE_BUCKET(pub_year, GENERATE_ARRAY(1950, 2026, 1))
CLUSTER BY corpusid
OPTIONS (description = 'Article citation percentiles within publication year at ages 0-30 (arrays indexed by age). Built by paper/sql/04_article_percentiles.sql')
AS
WITH
  long_form AS (
    SELECT
      p.corpusid,
      p.pub_year,
      p.fields,
      age,
      c AS cites
    FROM `scholar-version2.paper_rp.paper_citations_by_age` AS p,
      UNNEST([p.c00, p.c01, p.c02, p.c03, p.c04, p.c05, p.c06, p.c07, p.c08, p.c09, p.c10,
              p.c11, p.c12, p.c13, p.c14, p.c15, p.c16, p.c17, p.c18, p.c19, p.c20,
              p.c21, p.c22, p.c23, p.c24, p.c25, p.c26, p.c27, p.c28, p.c29, p.c30]) AS c WITH OFFSET AS age
    WHERE p.is_article AND age <= LEAST(30, 2025 - p.pub_year)
  )
SELECT
  lf.corpusid,
  lf.pub_year,
  ANY_VALUE(lf.fields) AS fields,
  ARRAY_AGG(lf.cites ORDER BY lf.age) AS cites,
  ARRAY_AGG(l.n_less / l.n_total ORDER BY lf.age) AS below,
  ARRAY_AGG(l.hazen ORDER BY lf.age) AS hazen
FROM long_form AS lf
JOIN `scholar-version2.paper_rp.citation_percentile_lookup` AS l
  ON l.ref_set = 'article' AND l.field = '*' AND l.pub_year = lf.pub_year AND l.age = lf.age AND l.citations = lf.cites
GROUP BY lf.corpusid, lf.pub_year;
