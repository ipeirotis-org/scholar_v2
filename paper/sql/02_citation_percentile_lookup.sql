-- 02: Citation percentile lookup by (reference set, field, publication year, age, citations).
--
-- For every observed value of cumulative citations at age t among papers published in the same year:
--   n_equal  papers in the reference set with exactly this many citations
--   n_less   papers with fewer citations
--   n_total  all papers in the reference set observable at this age
-- so any percentile convention can be derived without recomputing:
--   Hazen with average ranks (as in v3): (n_less + 0.5 * n_equal) / n_total  -> column `hazen`
--   share strictly below:               n_less / n_total
--   share at or below:                  (n_less + n_equal) / n_total
-- A paper outside the reference set (e.g. an untyped paper scored against `article`) takes the row with the
-- largest `citations` <= its own value: n_less + n_equal of that row is its strictly-below count when the
-- values differ.
--
-- field = '*' pools all fields; otherwise one row set per S2 field of study (whole counting: a paper with two
-- fields counts in both). Reference sets: 'all' = every S2 paper, 'article' = is_article (see 01).
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.citation_percentile_lookup`
CLUSTER BY ref_set, field, pub_year, age
OPTIONS (description = 'Counts for citation percentiles within (ref_set, field, pub_year, age). Built by paper/sql/02_citation_percentile_lookup.sql')
AS
WITH
  long_form AS (
    SELECT
      ref_set,
      field,
      p.pub_year,
      age,
      citations
    FROM `scholar-version2.paper_rp.paper_citations_by_age` AS p,
      UNNEST(IF(p.is_article, ['all', 'article'], ['all'])) AS ref_set,
      UNNEST(ARRAY_CONCAT(['*'], p.fields)) AS field,
      UNNEST([IFNULL(c00, -1), IFNULL(c01, -1), IFNULL(c02, -1), IFNULL(c03, -1), IFNULL(c04, -1), IFNULL(c05, -1), IFNULL(c06, -1), IFNULL(c07, -1), IFNULL(c08, -1), IFNULL(c09, -1), IFNULL(c10, -1), IFNULL(c11, -1), IFNULL(c12, -1), IFNULL(c13, -1), IFNULL(c14, -1), IFNULL(c15, -1), IFNULL(c16, -1), IFNULL(c17, -1), IFNULL(c18, -1), IFNULL(c19, -1), IFNULL(c20, -1), IFNULL(c21, -1), IFNULL(c22, -1), IFNULL(c23, -1), IFNULL(c24, -1), IFNULL(c25, -1), IFNULL(c26, -1), IFNULL(c27, -1), IFNULL(c28, -1), IFNULL(c29, -1), IFNULL(c30, -1)]) AS citations WITH OFFSET AS age
    WHERE citations >= 0
  ),
  counts AS (
    SELECT ref_set, field, pub_year, age, citations, COUNT(*) AS n_equal
    FROM long_form
    GROUP BY ref_set, field, pub_year, age, citations
  )
SELECT
  ref_set,
  field,
  pub_year,
  age,
  citations,
  n_equal,
  IFNULL(SUM(n_equal) OVER (PARTITION BY ref_set, field, pub_year, age ORDER BY citations
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS n_less,
  SUM(n_equal) OVER (PARTITION BY ref_set, field, pub_year, age) AS n_total,
  (IFNULL(SUM(n_equal) OVER (PARTITION BY ref_set, field, pub_year, age ORDER BY citations
                             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) + 0.5 * n_equal)
    / SUM(n_equal) OVER (PARTITION BY ref_set, field, pub_year, age) AS hazen
FROM counts;
