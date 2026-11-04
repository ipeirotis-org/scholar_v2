-- 03: Stability of paper citation percentiles with age (S2 version of v3 Figs. 4-6).
--
-- For every article, its Hazen percentile within (field, publication year) at ages [1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 25, 30],
-- then for each pair of ages t1 < t2: number of papers observed at t2, Pearson correlation of the two percentiles
-- (equal to the Spearman correlation of the raw citation counts, since the percentiles are mid-ranks), the OLS slope
-- of P(t2) on P(t1), and the mean absolute change in percentile.
-- field = '*' pools all fields; otherwise S2 field of study (whole counting). Reference set: 'article'.
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.paper_percentile_stability`
OPTIONS (description = 'Correlation of paper percentiles between ages, by field and publication year. Built by paper/sql/03_paper_percentile_stability.sql')
AS
WITH
  long_form AS (
    SELECT p.corpusid, p.pub_year, f AS field, a AS age, l.hazen
    FROM `scholar-version2.paper_rp.paper_citations_by_age` AS p
    CROSS JOIN UNNEST(ARRAY_CONCAT(['*'], p.fields)) AS f
    CROSS JOIN UNNEST([1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 25, 30]) AS a
    JOIN `scholar-version2.paper_rp.citation_percentile_lookup` AS l
      ON l.ref_set = 'article' AND l.field = f AND l.pub_year = p.pub_year AND l.age = a
     AND l.citations = CASE a WHEN 1 THEN p.c01 WHEN 2 THEN p.c02 WHEN 3 THEN p.c03 WHEN 4 THEN p.c04 WHEN 5 THEN p.c05 WHEN 6 THEN p.c06 WHEN 8 THEN p.c08 WHEN 10 THEN p.c10 WHEN 12 THEN p.c12 WHEN 15 THEN p.c15 WHEN 20 THEN p.c20 WHEN 25 THEN p.c25 WHEN 30 THEN p.c30 END
    WHERE p.is_article AND p.pub_year BETWEEN 1960 AND 2024
  ),
  wide AS (
    SELECT corpusid, pub_year, field,
      MAX(IF(age = 1, hazen, NULL)) AS h01,
      MAX(IF(age = 2, hazen, NULL)) AS h02,
      MAX(IF(age = 3, hazen, NULL)) AS h03,
      MAX(IF(age = 4, hazen, NULL)) AS h04,
      MAX(IF(age = 5, hazen, NULL)) AS h05,
      MAX(IF(age = 6, hazen, NULL)) AS h06,
      MAX(IF(age = 8, hazen, NULL)) AS h08,
      MAX(IF(age = 10, hazen, NULL)) AS h10,
      MAX(IF(age = 12, hazen, NULL)) AS h12,
      MAX(IF(age = 15, hazen, NULL)) AS h15,
      MAX(IF(age = 20, hazen, NULL)) AS h20,
      MAX(IF(age = 25, hazen, NULL)) AS h25,
      MAX(IF(age = 30, hazen, NULL)) AS h30
    FROM long_form
    GROUP BY corpusid, pub_year, field
  ),
  stats AS (
    SELECT pub_year, field,
      [
        STRUCT(1 AS t1, 2 AS t2, COUNT(h02) AS n, CORR(h01, h02) AS pearson, SAFE_DIVIDE(COVAR_POP(h01, h02), VAR_POP(h01)) AS slope, AVG(IF(h02 IS NOT NULL, ABS(h02 - h01), NULL)) AS mean_abs_change),
        STRUCT(1 AS t1, 3 AS t2, COUNT(h03) AS n, CORR(h01, h03) AS pearson, SAFE_DIVIDE(COVAR_POP(h01, h03), VAR_POP(h01)) AS slope, AVG(IF(h03 IS NOT NULL, ABS(h03 - h01), NULL)) AS mean_abs_change),
        STRUCT(1 AS t1, 4 AS t2, COUNT(h04) AS n, CORR(h01, h04) AS pearson, SAFE_DIVIDE(COVAR_POP(h01, h04), VAR_POP(h01)) AS slope, AVG(IF(h04 IS NOT NULL, ABS(h04 - h01), NULL)) AS mean_abs_change),
        STRUCT(1 AS t1, 5 AS t2, COUNT(h05) AS n, CORR(h01, h05) AS pearson, SAFE_DIVIDE(COVAR_POP(h01, h05), VAR_POP(h01)) AS slope, AVG(IF(h05 IS NOT NULL, ABS(h05 - h01), NULL)) AS mean_abs_change),
        STRUCT(1 AS t1, 6 AS t2, COUNT(h06) AS n, CORR(h01, h06) AS pearson, SAFE_DIVIDE(COVAR_POP(h01, h06), VAR_POP(h01)) AS slope, AVG(IF(h06 IS NOT NULL, ABS(h06 - h01), NULL)) AS mean_abs_change),
        STRUCT(1 AS t1, 8 AS t2, COUNT(h08) AS n, CORR(h01, h08) AS pearson, SAFE_DIVIDE(COVAR_POP(h01, h08), VAR_POP(h01)) AS slope, AVG(IF(h08 IS NOT NULL, ABS(h08 - h01), NULL)) AS mean_abs_change),
        STRUCT(1 AS t1, 10 AS t2, COUNT(h10) AS n, CORR(h01, h10) AS pearson, SAFE_DIVIDE(COVAR_POP(h01, h10), VAR_POP(h01)) AS slope, AVG(IF(h10 IS NOT NULL, ABS(h10 - h01), NULL)) AS mean_abs_change),
        STRUCT(1 AS t1, 12 AS t2, COUNT(h12) AS n, CORR(h01, h12) AS pearson, SAFE_DIVIDE(COVAR_POP(h01, h12), VAR_POP(h01)) AS slope, AVG(IF(h12 IS NOT NULL, ABS(h12 - h01), NULL)) AS mean_abs_change),
        STRUCT(1 AS t1, 15 AS t2, COUNT(h15) AS n, CORR(h01, h15) AS pearson, SAFE_DIVIDE(COVAR_POP(h01, h15), VAR_POP(h01)) AS slope, AVG(IF(h15 IS NOT NULL, ABS(h15 - h01), NULL)) AS mean_abs_change),
        STRUCT(1 AS t1, 20 AS t2, COUNT(h20) AS n, CORR(h01, h20) AS pearson, SAFE_DIVIDE(COVAR_POP(h01, h20), VAR_POP(h01)) AS slope, AVG(IF(h20 IS NOT NULL, ABS(h20 - h01), NULL)) AS mean_abs_change),
        STRUCT(1 AS t1, 25 AS t2, COUNT(h25) AS n, CORR(h01, h25) AS pearson, SAFE_DIVIDE(COVAR_POP(h01, h25), VAR_POP(h01)) AS slope, AVG(IF(h25 IS NOT NULL, ABS(h25 - h01), NULL)) AS mean_abs_change),
        STRUCT(1 AS t1, 30 AS t2, COUNT(h30) AS n, CORR(h01, h30) AS pearson, SAFE_DIVIDE(COVAR_POP(h01, h30), VAR_POP(h01)) AS slope, AVG(IF(h30 IS NOT NULL, ABS(h30 - h01), NULL)) AS mean_abs_change),
        STRUCT(2 AS t1, 3 AS t2, COUNT(h03) AS n, CORR(h02, h03) AS pearson, SAFE_DIVIDE(COVAR_POP(h02, h03), VAR_POP(h02)) AS slope, AVG(IF(h03 IS NOT NULL, ABS(h03 - h02), NULL)) AS mean_abs_change),
        STRUCT(2 AS t1, 4 AS t2, COUNT(h04) AS n, CORR(h02, h04) AS pearson, SAFE_DIVIDE(COVAR_POP(h02, h04), VAR_POP(h02)) AS slope, AVG(IF(h04 IS NOT NULL, ABS(h04 - h02), NULL)) AS mean_abs_change),
        STRUCT(2 AS t1, 5 AS t2, COUNT(h05) AS n, CORR(h02, h05) AS pearson, SAFE_DIVIDE(COVAR_POP(h02, h05), VAR_POP(h02)) AS slope, AVG(IF(h05 IS NOT NULL, ABS(h05 - h02), NULL)) AS mean_abs_change),
        STRUCT(2 AS t1, 6 AS t2, COUNT(h06) AS n, CORR(h02, h06) AS pearson, SAFE_DIVIDE(COVAR_POP(h02, h06), VAR_POP(h02)) AS slope, AVG(IF(h06 IS NOT NULL, ABS(h06 - h02), NULL)) AS mean_abs_change),
        STRUCT(2 AS t1, 8 AS t2, COUNT(h08) AS n, CORR(h02, h08) AS pearson, SAFE_DIVIDE(COVAR_POP(h02, h08), VAR_POP(h02)) AS slope, AVG(IF(h08 IS NOT NULL, ABS(h08 - h02), NULL)) AS mean_abs_change),
        STRUCT(2 AS t1, 10 AS t2, COUNT(h10) AS n, CORR(h02, h10) AS pearson, SAFE_DIVIDE(COVAR_POP(h02, h10), VAR_POP(h02)) AS slope, AVG(IF(h10 IS NOT NULL, ABS(h10 - h02), NULL)) AS mean_abs_change),
        STRUCT(2 AS t1, 12 AS t2, COUNT(h12) AS n, CORR(h02, h12) AS pearson, SAFE_DIVIDE(COVAR_POP(h02, h12), VAR_POP(h02)) AS slope, AVG(IF(h12 IS NOT NULL, ABS(h12 - h02), NULL)) AS mean_abs_change),
        STRUCT(2 AS t1, 15 AS t2, COUNT(h15) AS n, CORR(h02, h15) AS pearson, SAFE_DIVIDE(COVAR_POP(h02, h15), VAR_POP(h02)) AS slope, AVG(IF(h15 IS NOT NULL, ABS(h15 - h02), NULL)) AS mean_abs_change),
        STRUCT(2 AS t1, 20 AS t2, COUNT(h20) AS n, CORR(h02, h20) AS pearson, SAFE_DIVIDE(COVAR_POP(h02, h20), VAR_POP(h02)) AS slope, AVG(IF(h20 IS NOT NULL, ABS(h20 - h02), NULL)) AS mean_abs_change),
        STRUCT(2 AS t1, 25 AS t2, COUNT(h25) AS n, CORR(h02, h25) AS pearson, SAFE_DIVIDE(COVAR_POP(h02, h25), VAR_POP(h02)) AS slope, AVG(IF(h25 IS NOT NULL, ABS(h25 - h02), NULL)) AS mean_abs_change),
        STRUCT(2 AS t1, 30 AS t2, COUNT(h30) AS n, CORR(h02, h30) AS pearson, SAFE_DIVIDE(COVAR_POP(h02, h30), VAR_POP(h02)) AS slope, AVG(IF(h30 IS NOT NULL, ABS(h30 - h02), NULL)) AS mean_abs_change),
        STRUCT(3 AS t1, 4 AS t2, COUNT(h04) AS n, CORR(h03, h04) AS pearson, SAFE_DIVIDE(COVAR_POP(h03, h04), VAR_POP(h03)) AS slope, AVG(IF(h04 IS NOT NULL, ABS(h04 - h03), NULL)) AS mean_abs_change),
        STRUCT(3 AS t1, 5 AS t2, COUNT(h05) AS n, CORR(h03, h05) AS pearson, SAFE_DIVIDE(COVAR_POP(h03, h05), VAR_POP(h03)) AS slope, AVG(IF(h05 IS NOT NULL, ABS(h05 - h03), NULL)) AS mean_abs_change),
        STRUCT(3 AS t1, 6 AS t2, COUNT(h06) AS n, CORR(h03, h06) AS pearson, SAFE_DIVIDE(COVAR_POP(h03, h06), VAR_POP(h03)) AS slope, AVG(IF(h06 IS NOT NULL, ABS(h06 - h03), NULL)) AS mean_abs_change),
        STRUCT(3 AS t1, 8 AS t2, COUNT(h08) AS n, CORR(h03, h08) AS pearson, SAFE_DIVIDE(COVAR_POP(h03, h08), VAR_POP(h03)) AS slope, AVG(IF(h08 IS NOT NULL, ABS(h08 - h03), NULL)) AS mean_abs_change),
        STRUCT(3 AS t1, 10 AS t2, COUNT(h10) AS n, CORR(h03, h10) AS pearson, SAFE_DIVIDE(COVAR_POP(h03, h10), VAR_POP(h03)) AS slope, AVG(IF(h10 IS NOT NULL, ABS(h10 - h03), NULL)) AS mean_abs_change),
        STRUCT(3 AS t1, 12 AS t2, COUNT(h12) AS n, CORR(h03, h12) AS pearson, SAFE_DIVIDE(COVAR_POP(h03, h12), VAR_POP(h03)) AS slope, AVG(IF(h12 IS NOT NULL, ABS(h12 - h03), NULL)) AS mean_abs_change),
        STRUCT(3 AS t1, 15 AS t2, COUNT(h15) AS n, CORR(h03, h15) AS pearson, SAFE_DIVIDE(COVAR_POP(h03, h15), VAR_POP(h03)) AS slope, AVG(IF(h15 IS NOT NULL, ABS(h15 - h03), NULL)) AS mean_abs_change),
        STRUCT(3 AS t1, 20 AS t2, COUNT(h20) AS n, CORR(h03, h20) AS pearson, SAFE_DIVIDE(COVAR_POP(h03, h20), VAR_POP(h03)) AS slope, AVG(IF(h20 IS NOT NULL, ABS(h20 - h03), NULL)) AS mean_abs_change),
        STRUCT(3 AS t1, 25 AS t2, COUNT(h25) AS n, CORR(h03, h25) AS pearson, SAFE_DIVIDE(COVAR_POP(h03, h25), VAR_POP(h03)) AS slope, AVG(IF(h25 IS NOT NULL, ABS(h25 - h03), NULL)) AS mean_abs_change),
        STRUCT(3 AS t1, 30 AS t2, COUNT(h30) AS n, CORR(h03, h30) AS pearson, SAFE_DIVIDE(COVAR_POP(h03, h30), VAR_POP(h03)) AS slope, AVG(IF(h30 IS NOT NULL, ABS(h30 - h03), NULL)) AS mean_abs_change),
        STRUCT(4 AS t1, 5 AS t2, COUNT(h05) AS n, CORR(h04, h05) AS pearson, SAFE_DIVIDE(COVAR_POP(h04, h05), VAR_POP(h04)) AS slope, AVG(IF(h05 IS NOT NULL, ABS(h05 - h04), NULL)) AS mean_abs_change),
        STRUCT(4 AS t1, 6 AS t2, COUNT(h06) AS n, CORR(h04, h06) AS pearson, SAFE_DIVIDE(COVAR_POP(h04, h06), VAR_POP(h04)) AS slope, AVG(IF(h06 IS NOT NULL, ABS(h06 - h04), NULL)) AS mean_abs_change),
        STRUCT(4 AS t1, 8 AS t2, COUNT(h08) AS n, CORR(h04, h08) AS pearson, SAFE_DIVIDE(COVAR_POP(h04, h08), VAR_POP(h04)) AS slope, AVG(IF(h08 IS NOT NULL, ABS(h08 - h04), NULL)) AS mean_abs_change),
        STRUCT(4 AS t1, 10 AS t2, COUNT(h10) AS n, CORR(h04, h10) AS pearson, SAFE_DIVIDE(COVAR_POP(h04, h10), VAR_POP(h04)) AS slope, AVG(IF(h10 IS NOT NULL, ABS(h10 - h04), NULL)) AS mean_abs_change),
        STRUCT(4 AS t1, 12 AS t2, COUNT(h12) AS n, CORR(h04, h12) AS pearson, SAFE_DIVIDE(COVAR_POP(h04, h12), VAR_POP(h04)) AS slope, AVG(IF(h12 IS NOT NULL, ABS(h12 - h04), NULL)) AS mean_abs_change),
        STRUCT(4 AS t1, 15 AS t2, COUNT(h15) AS n, CORR(h04, h15) AS pearson, SAFE_DIVIDE(COVAR_POP(h04, h15), VAR_POP(h04)) AS slope, AVG(IF(h15 IS NOT NULL, ABS(h15 - h04), NULL)) AS mean_abs_change),
        STRUCT(4 AS t1, 20 AS t2, COUNT(h20) AS n, CORR(h04, h20) AS pearson, SAFE_DIVIDE(COVAR_POP(h04, h20), VAR_POP(h04)) AS slope, AVG(IF(h20 IS NOT NULL, ABS(h20 - h04), NULL)) AS mean_abs_change),
        STRUCT(4 AS t1, 25 AS t2, COUNT(h25) AS n, CORR(h04, h25) AS pearson, SAFE_DIVIDE(COVAR_POP(h04, h25), VAR_POP(h04)) AS slope, AVG(IF(h25 IS NOT NULL, ABS(h25 - h04), NULL)) AS mean_abs_change),
        STRUCT(4 AS t1, 30 AS t2, COUNT(h30) AS n, CORR(h04, h30) AS pearson, SAFE_DIVIDE(COVAR_POP(h04, h30), VAR_POP(h04)) AS slope, AVG(IF(h30 IS NOT NULL, ABS(h30 - h04), NULL)) AS mean_abs_change),
        STRUCT(5 AS t1, 6 AS t2, COUNT(h06) AS n, CORR(h05, h06) AS pearson, SAFE_DIVIDE(COVAR_POP(h05, h06), VAR_POP(h05)) AS slope, AVG(IF(h06 IS NOT NULL, ABS(h06 - h05), NULL)) AS mean_abs_change),
        STRUCT(5 AS t1, 8 AS t2, COUNT(h08) AS n, CORR(h05, h08) AS pearson, SAFE_DIVIDE(COVAR_POP(h05, h08), VAR_POP(h05)) AS slope, AVG(IF(h08 IS NOT NULL, ABS(h08 - h05), NULL)) AS mean_abs_change),
        STRUCT(5 AS t1, 10 AS t2, COUNT(h10) AS n, CORR(h05, h10) AS pearson, SAFE_DIVIDE(COVAR_POP(h05, h10), VAR_POP(h05)) AS slope, AVG(IF(h10 IS NOT NULL, ABS(h10 - h05), NULL)) AS mean_abs_change),
        STRUCT(5 AS t1, 12 AS t2, COUNT(h12) AS n, CORR(h05, h12) AS pearson, SAFE_DIVIDE(COVAR_POP(h05, h12), VAR_POP(h05)) AS slope, AVG(IF(h12 IS NOT NULL, ABS(h12 - h05), NULL)) AS mean_abs_change),
        STRUCT(5 AS t1, 15 AS t2, COUNT(h15) AS n, CORR(h05, h15) AS pearson, SAFE_DIVIDE(COVAR_POP(h05, h15), VAR_POP(h05)) AS slope, AVG(IF(h15 IS NOT NULL, ABS(h15 - h05), NULL)) AS mean_abs_change),
        STRUCT(5 AS t1, 20 AS t2, COUNT(h20) AS n, CORR(h05, h20) AS pearson, SAFE_DIVIDE(COVAR_POP(h05, h20), VAR_POP(h05)) AS slope, AVG(IF(h20 IS NOT NULL, ABS(h20 - h05), NULL)) AS mean_abs_change),
        STRUCT(5 AS t1, 25 AS t2, COUNT(h25) AS n, CORR(h05, h25) AS pearson, SAFE_DIVIDE(COVAR_POP(h05, h25), VAR_POP(h05)) AS slope, AVG(IF(h25 IS NOT NULL, ABS(h25 - h05), NULL)) AS mean_abs_change),
        STRUCT(5 AS t1, 30 AS t2, COUNT(h30) AS n, CORR(h05, h30) AS pearson, SAFE_DIVIDE(COVAR_POP(h05, h30), VAR_POP(h05)) AS slope, AVG(IF(h30 IS NOT NULL, ABS(h30 - h05), NULL)) AS mean_abs_change),
        STRUCT(6 AS t1, 8 AS t2, COUNT(h08) AS n, CORR(h06, h08) AS pearson, SAFE_DIVIDE(COVAR_POP(h06, h08), VAR_POP(h06)) AS slope, AVG(IF(h08 IS NOT NULL, ABS(h08 - h06), NULL)) AS mean_abs_change),
        STRUCT(6 AS t1, 10 AS t2, COUNT(h10) AS n, CORR(h06, h10) AS pearson, SAFE_DIVIDE(COVAR_POP(h06, h10), VAR_POP(h06)) AS slope, AVG(IF(h10 IS NOT NULL, ABS(h10 - h06), NULL)) AS mean_abs_change),
        STRUCT(6 AS t1, 12 AS t2, COUNT(h12) AS n, CORR(h06, h12) AS pearson, SAFE_DIVIDE(COVAR_POP(h06, h12), VAR_POP(h06)) AS slope, AVG(IF(h12 IS NOT NULL, ABS(h12 - h06), NULL)) AS mean_abs_change),
        STRUCT(6 AS t1, 15 AS t2, COUNT(h15) AS n, CORR(h06, h15) AS pearson, SAFE_DIVIDE(COVAR_POP(h06, h15), VAR_POP(h06)) AS slope, AVG(IF(h15 IS NOT NULL, ABS(h15 - h06), NULL)) AS mean_abs_change),
        STRUCT(6 AS t1, 20 AS t2, COUNT(h20) AS n, CORR(h06, h20) AS pearson, SAFE_DIVIDE(COVAR_POP(h06, h20), VAR_POP(h06)) AS slope, AVG(IF(h20 IS NOT NULL, ABS(h20 - h06), NULL)) AS mean_abs_change),
        STRUCT(6 AS t1, 25 AS t2, COUNT(h25) AS n, CORR(h06, h25) AS pearson, SAFE_DIVIDE(COVAR_POP(h06, h25), VAR_POP(h06)) AS slope, AVG(IF(h25 IS NOT NULL, ABS(h25 - h06), NULL)) AS mean_abs_change),
        STRUCT(6 AS t1, 30 AS t2, COUNT(h30) AS n, CORR(h06, h30) AS pearson, SAFE_DIVIDE(COVAR_POP(h06, h30), VAR_POP(h06)) AS slope, AVG(IF(h30 IS NOT NULL, ABS(h30 - h06), NULL)) AS mean_abs_change),
        STRUCT(8 AS t1, 10 AS t2, COUNT(h10) AS n, CORR(h08, h10) AS pearson, SAFE_DIVIDE(COVAR_POP(h08, h10), VAR_POP(h08)) AS slope, AVG(IF(h10 IS NOT NULL, ABS(h10 - h08), NULL)) AS mean_abs_change),
        STRUCT(8 AS t1, 12 AS t2, COUNT(h12) AS n, CORR(h08, h12) AS pearson, SAFE_DIVIDE(COVAR_POP(h08, h12), VAR_POP(h08)) AS slope, AVG(IF(h12 IS NOT NULL, ABS(h12 - h08), NULL)) AS mean_abs_change),
        STRUCT(8 AS t1, 15 AS t2, COUNT(h15) AS n, CORR(h08, h15) AS pearson, SAFE_DIVIDE(COVAR_POP(h08, h15), VAR_POP(h08)) AS slope, AVG(IF(h15 IS NOT NULL, ABS(h15 - h08), NULL)) AS mean_abs_change),
        STRUCT(8 AS t1, 20 AS t2, COUNT(h20) AS n, CORR(h08, h20) AS pearson, SAFE_DIVIDE(COVAR_POP(h08, h20), VAR_POP(h08)) AS slope, AVG(IF(h20 IS NOT NULL, ABS(h20 - h08), NULL)) AS mean_abs_change),
        STRUCT(8 AS t1, 25 AS t2, COUNT(h25) AS n, CORR(h08, h25) AS pearson, SAFE_DIVIDE(COVAR_POP(h08, h25), VAR_POP(h08)) AS slope, AVG(IF(h25 IS NOT NULL, ABS(h25 - h08), NULL)) AS mean_abs_change),
        STRUCT(8 AS t1, 30 AS t2, COUNT(h30) AS n, CORR(h08, h30) AS pearson, SAFE_DIVIDE(COVAR_POP(h08, h30), VAR_POP(h08)) AS slope, AVG(IF(h30 IS NOT NULL, ABS(h30 - h08), NULL)) AS mean_abs_change),
        STRUCT(10 AS t1, 12 AS t2, COUNT(h12) AS n, CORR(h10, h12) AS pearson, SAFE_DIVIDE(COVAR_POP(h10, h12), VAR_POP(h10)) AS slope, AVG(IF(h12 IS NOT NULL, ABS(h12 - h10), NULL)) AS mean_abs_change),
        STRUCT(10 AS t1, 15 AS t2, COUNT(h15) AS n, CORR(h10, h15) AS pearson, SAFE_DIVIDE(COVAR_POP(h10, h15), VAR_POP(h10)) AS slope, AVG(IF(h15 IS NOT NULL, ABS(h15 - h10), NULL)) AS mean_abs_change),
        STRUCT(10 AS t1, 20 AS t2, COUNT(h20) AS n, CORR(h10, h20) AS pearson, SAFE_DIVIDE(COVAR_POP(h10, h20), VAR_POP(h10)) AS slope, AVG(IF(h20 IS NOT NULL, ABS(h20 - h10), NULL)) AS mean_abs_change),
        STRUCT(10 AS t1, 25 AS t2, COUNT(h25) AS n, CORR(h10, h25) AS pearson, SAFE_DIVIDE(COVAR_POP(h10, h25), VAR_POP(h10)) AS slope, AVG(IF(h25 IS NOT NULL, ABS(h25 - h10), NULL)) AS mean_abs_change),
        STRUCT(10 AS t1, 30 AS t2, COUNT(h30) AS n, CORR(h10, h30) AS pearson, SAFE_DIVIDE(COVAR_POP(h10, h30), VAR_POP(h10)) AS slope, AVG(IF(h30 IS NOT NULL, ABS(h30 - h10), NULL)) AS mean_abs_change),
        STRUCT(12 AS t1, 15 AS t2, COUNT(h15) AS n, CORR(h12, h15) AS pearson, SAFE_DIVIDE(COVAR_POP(h12, h15), VAR_POP(h12)) AS slope, AVG(IF(h15 IS NOT NULL, ABS(h15 - h12), NULL)) AS mean_abs_change),
        STRUCT(12 AS t1, 20 AS t2, COUNT(h20) AS n, CORR(h12, h20) AS pearson, SAFE_DIVIDE(COVAR_POP(h12, h20), VAR_POP(h12)) AS slope, AVG(IF(h20 IS NOT NULL, ABS(h20 - h12), NULL)) AS mean_abs_change),
        STRUCT(12 AS t1, 25 AS t2, COUNT(h25) AS n, CORR(h12, h25) AS pearson, SAFE_DIVIDE(COVAR_POP(h12, h25), VAR_POP(h12)) AS slope, AVG(IF(h25 IS NOT NULL, ABS(h25 - h12), NULL)) AS mean_abs_change),
        STRUCT(12 AS t1, 30 AS t2, COUNT(h30) AS n, CORR(h12, h30) AS pearson, SAFE_DIVIDE(COVAR_POP(h12, h30), VAR_POP(h12)) AS slope, AVG(IF(h30 IS NOT NULL, ABS(h30 - h12), NULL)) AS mean_abs_change),
        STRUCT(15 AS t1, 20 AS t2, COUNT(h20) AS n, CORR(h15, h20) AS pearson, SAFE_DIVIDE(COVAR_POP(h15, h20), VAR_POP(h15)) AS slope, AVG(IF(h20 IS NOT NULL, ABS(h20 - h15), NULL)) AS mean_abs_change),
        STRUCT(15 AS t1, 25 AS t2, COUNT(h25) AS n, CORR(h15, h25) AS pearson, SAFE_DIVIDE(COVAR_POP(h15, h25), VAR_POP(h15)) AS slope, AVG(IF(h25 IS NOT NULL, ABS(h25 - h15), NULL)) AS mean_abs_change),
        STRUCT(15 AS t1, 30 AS t2, COUNT(h30) AS n, CORR(h15, h30) AS pearson, SAFE_DIVIDE(COVAR_POP(h15, h30), VAR_POP(h15)) AS slope, AVG(IF(h30 IS NOT NULL, ABS(h30 - h15), NULL)) AS mean_abs_change),
        STRUCT(20 AS t1, 25 AS t2, COUNT(h25) AS n, CORR(h20, h25) AS pearson, SAFE_DIVIDE(COVAR_POP(h20, h25), VAR_POP(h20)) AS slope, AVG(IF(h25 IS NOT NULL, ABS(h25 - h20), NULL)) AS mean_abs_change),
        STRUCT(20 AS t1, 30 AS t2, COUNT(h30) AS n, CORR(h20, h30) AS pearson, SAFE_DIVIDE(COVAR_POP(h20, h30), VAR_POP(h20)) AS slope, AVG(IF(h30 IS NOT NULL, ABS(h30 - h20), NULL)) AS mean_abs_change),
        STRUCT(25 AS t1, 30 AS t2, COUNT(h30) AS n, CORR(h25, h30) AS pearson, SAFE_DIVIDE(COVAR_POP(h25, h30), VAR_POP(h25)) AS slope, AVG(IF(h30 IS NOT NULL, ABS(h30 - h25), NULL)) AS mean_abs_change)
      ] AS s
    FROM wide
    GROUP BY pub_year, field
  )
SELECT 'article' AS ref_set, field, pub_year, s.*
FROM stats, UNNEST(s) AS s
WHERE s.n > 0;
