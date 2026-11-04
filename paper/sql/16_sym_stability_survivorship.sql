-- 16: The symmetric PiP (sym_tau50, and sym with tau = 0) through the scholar-level tests of 08, 09 and section 5.
--   sym_pct_by_age         within-cohort Hazen percentiles of sym_tau50 and sym at every career age
--   sym_stability          as 08 (correlation of percentiles between career ages), metrics 'sym50' and 'sym'
--   sym_survivorship_auc   as 09: AUC of the age-5 percentile for publishing at career ages 11-15 and 16-20
--   sym_decomposition      variance of log sym_tau50 split into log F(n) (productivity) and log Q (quality multiplier),
--                          active authors with sym_tau50 > 0, start years 1975-2005, at ages 5, 10 and 20
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.sym_pct_by_age`
PARTITION BY RANGE_BUCKET(start_year, GENERATE_ARRAY(1950, 2026, 1))
CLUSTER BY age, authorid
OPTIONS (description = 'Within-cohort Hazen percentiles of symmetric PiP by career age. Built by paper/sql/16_sym_stability_survivorship.sql')
AS
SELECT authorid, start_year, age, sym_tau50, sym,
  (COUNT(*) OVER w + RANK() OVER (w ORDER BY sym_tau50) - RANK() OVER (w ORDER BY sym_tau50 DESC)) / (2 * COUNT(*) OVER w) AS pct_sym50,
  (COUNT(*) OVER w + RANK() OVER (w ORDER BY sym) - RANK() OVER (w ORDER BY sym DESC)) / (2 * COUNT(*) OVER w) AS pct_sym
FROM `scholar-version2.paper_rp.sym_by_age`
WINDOW w AS (PARTITION BY start_year, age);

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.sym_stability`
OPTIONS (description = 'Correlation of symmetric-PiP percentiles between career ages. Built by paper/sql/16_sym_stability_survivorship.sql')
AS
WITH
  wide AS (
    SELECT start_year, authorid,
      MAX(IF(age = 1, pct_sym50, NULL)) AS sym50_01,
      MAX(IF(age = 2, pct_sym50, NULL)) AS sym50_02,
      MAX(IF(age = 3, pct_sym50, NULL)) AS sym50_03,
      MAX(IF(age = 4, pct_sym50, NULL)) AS sym50_04,
      MAX(IF(age = 5, pct_sym50, NULL)) AS sym50_05,
      MAX(IF(age = 6, pct_sym50, NULL)) AS sym50_06,
      MAX(IF(age = 8, pct_sym50, NULL)) AS sym50_08,
      MAX(IF(age = 10, pct_sym50, NULL)) AS sym50_10,
      MAX(IF(age = 12, pct_sym50, NULL)) AS sym50_12,
      MAX(IF(age = 15, pct_sym50, NULL)) AS sym50_15,
      MAX(IF(age = 20, pct_sym50, NULL)) AS sym50_20,
      MAX(IF(age = 25, pct_sym50, NULL)) AS sym50_25,
      MAX(IF(age = 30, pct_sym50, NULL)) AS sym50_30,
      MAX(IF(age = 1, pct_sym, NULL)) AS sym_01,
      MAX(IF(age = 2, pct_sym, NULL)) AS sym_02,
      MAX(IF(age = 3, pct_sym, NULL)) AS sym_03,
      MAX(IF(age = 4, pct_sym, NULL)) AS sym_04,
      MAX(IF(age = 5, pct_sym, NULL)) AS sym_05,
      MAX(IF(age = 6, pct_sym, NULL)) AS sym_06,
      MAX(IF(age = 8, pct_sym, NULL)) AS sym_08,
      MAX(IF(age = 10, pct_sym, NULL)) AS sym_10,
      MAX(IF(age = 12, pct_sym, NULL)) AS sym_12,
      MAX(IF(age = 15, pct_sym, NULL)) AS sym_15,
      MAX(IF(age = 20, pct_sym, NULL)) AS sym_20,
      MAX(IF(age = 25, pct_sym, NULL)) AS sym_25,
      MAX(IF(age = 30, pct_sym, NULL)) AS sym_30
    FROM `scholar-version2.paper_rp.sym_pct_by_age`
    WHERE age IN UNNEST([1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 25, 30])
    GROUP BY start_year, authorid
  ),
  stats AS (
    SELECT start_year,
      [
        STRUCT('sym50' AS metric, 1 AS t1, 2 AS t2, COUNTIF(sym50_01 IS NOT NULL AND sym50_02 IS NOT NULL) AS n, CORR(sym50_01, sym50_02) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_01, sym50_02), VAR_POP(sym50_01)) AS slope, AVG(IF(sym50_02 IS NOT NULL, ABS(sym50_02 - sym50_01), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 1 AS t1, 3 AS t2, COUNTIF(sym50_01 IS NOT NULL AND sym50_03 IS NOT NULL) AS n, CORR(sym50_01, sym50_03) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_01, sym50_03), VAR_POP(sym50_01)) AS slope, AVG(IF(sym50_03 IS NOT NULL, ABS(sym50_03 - sym50_01), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 1 AS t1, 4 AS t2, COUNTIF(sym50_01 IS NOT NULL AND sym50_04 IS NOT NULL) AS n, CORR(sym50_01, sym50_04) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_01, sym50_04), VAR_POP(sym50_01)) AS slope, AVG(IF(sym50_04 IS NOT NULL, ABS(sym50_04 - sym50_01), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 1 AS t1, 5 AS t2, COUNTIF(sym50_01 IS NOT NULL AND sym50_05 IS NOT NULL) AS n, CORR(sym50_01, sym50_05) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_01, sym50_05), VAR_POP(sym50_01)) AS slope, AVG(IF(sym50_05 IS NOT NULL, ABS(sym50_05 - sym50_01), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 1 AS t1, 6 AS t2, COUNTIF(sym50_01 IS NOT NULL AND sym50_06 IS NOT NULL) AS n, CORR(sym50_01, sym50_06) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_01, sym50_06), VAR_POP(sym50_01)) AS slope, AVG(IF(sym50_06 IS NOT NULL, ABS(sym50_06 - sym50_01), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 1 AS t1, 8 AS t2, COUNTIF(sym50_01 IS NOT NULL AND sym50_08 IS NOT NULL) AS n, CORR(sym50_01, sym50_08) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_01, sym50_08), VAR_POP(sym50_01)) AS slope, AVG(IF(sym50_08 IS NOT NULL, ABS(sym50_08 - sym50_01), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 1 AS t1, 10 AS t2, COUNTIF(sym50_01 IS NOT NULL AND sym50_10 IS NOT NULL) AS n, CORR(sym50_01, sym50_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_01, sym50_10), VAR_POP(sym50_01)) AS slope, AVG(IF(sym50_10 IS NOT NULL, ABS(sym50_10 - sym50_01), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 1 AS t1, 12 AS t2, COUNTIF(sym50_01 IS NOT NULL AND sym50_12 IS NOT NULL) AS n, CORR(sym50_01, sym50_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_01, sym50_12), VAR_POP(sym50_01)) AS slope, AVG(IF(sym50_12 IS NOT NULL, ABS(sym50_12 - sym50_01), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 1 AS t1, 15 AS t2, COUNTIF(sym50_01 IS NOT NULL AND sym50_15 IS NOT NULL) AS n, CORR(sym50_01, sym50_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_01, sym50_15), VAR_POP(sym50_01)) AS slope, AVG(IF(sym50_15 IS NOT NULL, ABS(sym50_15 - sym50_01), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 1 AS t1, 20 AS t2, COUNTIF(sym50_01 IS NOT NULL AND sym50_20 IS NOT NULL) AS n, CORR(sym50_01, sym50_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_01, sym50_20), VAR_POP(sym50_01)) AS slope, AVG(IF(sym50_20 IS NOT NULL, ABS(sym50_20 - sym50_01), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 1 AS t1, 25 AS t2, COUNTIF(sym50_01 IS NOT NULL AND sym50_25 IS NOT NULL) AS n, CORR(sym50_01, sym50_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_01, sym50_25), VAR_POP(sym50_01)) AS slope, AVG(IF(sym50_25 IS NOT NULL, ABS(sym50_25 - sym50_01), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 1 AS t1, 30 AS t2, COUNTIF(sym50_01 IS NOT NULL AND sym50_30 IS NOT NULL) AS n, CORR(sym50_01, sym50_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_01, sym50_30), VAR_POP(sym50_01)) AS slope, AVG(IF(sym50_30 IS NOT NULL, ABS(sym50_30 - sym50_01), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 2 AS t1, 3 AS t2, COUNTIF(sym50_02 IS NOT NULL AND sym50_03 IS NOT NULL) AS n, CORR(sym50_02, sym50_03) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_02, sym50_03), VAR_POP(sym50_02)) AS slope, AVG(IF(sym50_03 IS NOT NULL, ABS(sym50_03 - sym50_02), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 2 AS t1, 4 AS t2, COUNTIF(sym50_02 IS NOT NULL AND sym50_04 IS NOT NULL) AS n, CORR(sym50_02, sym50_04) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_02, sym50_04), VAR_POP(sym50_02)) AS slope, AVG(IF(sym50_04 IS NOT NULL, ABS(sym50_04 - sym50_02), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 2 AS t1, 5 AS t2, COUNTIF(sym50_02 IS NOT NULL AND sym50_05 IS NOT NULL) AS n, CORR(sym50_02, sym50_05) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_02, sym50_05), VAR_POP(sym50_02)) AS slope, AVG(IF(sym50_05 IS NOT NULL, ABS(sym50_05 - sym50_02), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 2 AS t1, 6 AS t2, COUNTIF(sym50_02 IS NOT NULL AND sym50_06 IS NOT NULL) AS n, CORR(sym50_02, sym50_06) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_02, sym50_06), VAR_POP(sym50_02)) AS slope, AVG(IF(sym50_06 IS NOT NULL, ABS(sym50_06 - sym50_02), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 2 AS t1, 8 AS t2, COUNTIF(sym50_02 IS NOT NULL AND sym50_08 IS NOT NULL) AS n, CORR(sym50_02, sym50_08) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_02, sym50_08), VAR_POP(sym50_02)) AS slope, AVG(IF(sym50_08 IS NOT NULL, ABS(sym50_08 - sym50_02), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 2 AS t1, 10 AS t2, COUNTIF(sym50_02 IS NOT NULL AND sym50_10 IS NOT NULL) AS n, CORR(sym50_02, sym50_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_02, sym50_10), VAR_POP(sym50_02)) AS slope, AVG(IF(sym50_10 IS NOT NULL, ABS(sym50_10 - sym50_02), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 2 AS t1, 12 AS t2, COUNTIF(sym50_02 IS NOT NULL AND sym50_12 IS NOT NULL) AS n, CORR(sym50_02, sym50_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_02, sym50_12), VAR_POP(sym50_02)) AS slope, AVG(IF(sym50_12 IS NOT NULL, ABS(sym50_12 - sym50_02), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 2 AS t1, 15 AS t2, COUNTIF(sym50_02 IS NOT NULL AND sym50_15 IS NOT NULL) AS n, CORR(sym50_02, sym50_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_02, sym50_15), VAR_POP(sym50_02)) AS slope, AVG(IF(sym50_15 IS NOT NULL, ABS(sym50_15 - sym50_02), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 2 AS t1, 20 AS t2, COUNTIF(sym50_02 IS NOT NULL AND sym50_20 IS NOT NULL) AS n, CORR(sym50_02, sym50_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_02, sym50_20), VAR_POP(sym50_02)) AS slope, AVG(IF(sym50_20 IS NOT NULL, ABS(sym50_20 - sym50_02), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 2 AS t1, 25 AS t2, COUNTIF(sym50_02 IS NOT NULL AND sym50_25 IS NOT NULL) AS n, CORR(sym50_02, sym50_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_02, sym50_25), VAR_POP(sym50_02)) AS slope, AVG(IF(sym50_25 IS NOT NULL, ABS(sym50_25 - sym50_02), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 2 AS t1, 30 AS t2, COUNTIF(sym50_02 IS NOT NULL AND sym50_30 IS NOT NULL) AS n, CORR(sym50_02, sym50_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_02, sym50_30), VAR_POP(sym50_02)) AS slope, AVG(IF(sym50_30 IS NOT NULL, ABS(sym50_30 - sym50_02), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 3 AS t1, 4 AS t2, COUNTIF(sym50_03 IS NOT NULL AND sym50_04 IS NOT NULL) AS n, CORR(sym50_03, sym50_04) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_03, sym50_04), VAR_POP(sym50_03)) AS slope, AVG(IF(sym50_04 IS NOT NULL, ABS(sym50_04 - sym50_03), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 3 AS t1, 5 AS t2, COUNTIF(sym50_03 IS NOT NULL AND sym50_05 IS NOT NULL) AS n, CORR(sym50_03, sym50_05) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_03, sym50_05), VAR_POP(sym50_03)) AS slope, AVG(IF(sym50_05 IS NOT NULL, ABS(sym50_05 - sym50_03), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 3 AS t1, 6 AS t2, COUNTIF(sym50_03 IS NOT NULL AND sym50_06 IS NOT NULL) AS n, CORR(sym50_03, sym50_06) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_03, sym50_06), VAR_POP(sym50_03)) AS slope, AVG(IF(sym50_06 IS NOT NULL, ABS(sym50_06 - sym50_03), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 3 AS t1, 8 AS t2, COUNTIF(sym50_03 IS NOT NULL AND sym50_08 IS NOT NULL) AS n, CORR(sym50_03, sym50_08) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_03, sym50_08), VAR_POP(sym50_03)) AS slope, AVG(IF(sym50_08 IS NOT NULL, ABS(sym50_08 - sym50_03), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 3 AS t1, 10 AS t2, COUNTIF(sym50_03 IS NOT NULL AND sym50_10 IS NOT NULL) AS n, CORR(sym50_03, sym50_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_03, sym50_10), VAR_POP(sym50_03)) AS slope, AVG(IF(sym50_10 IS NOT NULL, ABS(sym50_10 - sym50_03), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 3 AS t1, 12 AS t2, COUNTIF(sym50_03 IS NOT NULL AND sym50_12 IS NOT NULL) AS n, CORR(sym50_03, sym50_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_03, sym50_12), VAR_POP(sym50_03)) AS slope, AVG(IF(sym50_12 IS NOT NULL, ABS(sym50_12 - sym50_03), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 3 AS t1, 15 AS t2, COUNTIF(sym50_03 IS NOT NULL AND sym50_15 IS NOT NULL) AS n, CORR(sym50_03, sym50_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_03, sym50_15), VAR_POP(sym50_03)) AS slope, AVG(IF(sym50_15 IS NOT NULL, ABS(sym50_15 - sym50_03), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 3 AS t1, 20 AS t2, COUNTIF(sym50_03 IS NOT NULL AND sym50_20 IS NOT NULL) AS n, CORR(sym50_03, sym50_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_03, sym50_20), VAR_POP(sym50_03)) AS slope, AVG(IF(sym50_20 IS NOT NULL, ABS(sym50_20 - sym50_03), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 3 AS t1, 25 AS t2, COUNTIF(sym50_03 IS NOT NULL AND sym50_25 IS NOT NULL) AS n, CORR(sym50_03, sym50_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_03, sym50_25), VAR_POP(sym50_03)) AS slope, AVG(IF(sym50_25 IS NOT NULL, ABS(sym50_25 - sym50_03), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 3 AS t1, 30 AS t2, COUNTIF(sym50_03 IS NOT NULL AND sym50_30 IS NOT NULL) AS n, CORR(sym50_03, sym50_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_03, sym50_30), VAR_POP(sym50_03)) AS slope, AVG(IF(sym50_30 IS NOT NULL, ABS(sym50_30 - sym50_03), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 4 AS t1, 5 AS t2, COUNTIF(sym50_04 IS NOT NULL AND sym50_05 IS NOT NULL) AS n, CORR(sym50_04, sym50_05) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_04, sym50_05), VAR_POP(sym50_04)) AS slope, AVG(IF(sym50_05 IS NOT NULL, ABS(sym50_05 - sym50_04), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 4 AS t1, 6 AS t2, COUNTIF(sym50_04 IS NOT NULL AND sym50_06 IS NOT NULL) AS n, CORR(sym50_04, sym50_06) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_04, sym50_06), VAR_POP(sym50_04)) AS slope, AVG(IF(sym50_06 IS NOT NULL, ABS(sym50_06 - sym50_04), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 4 AS t1, 8 AS t2, COUNTIF(sym50_04 IS NOT NULL AND sym50_08 IS NOT NULL) AS n, CORR(sym50_04, sym50_08) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_04, sym50_08), VAR_POP(sym50_04)) AS slope, AVG(IF(sym50_08 IS NOT NULL, ABS(sym50_08 - sym50_04), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 4 AS t1, 10 AS t2, COUNTIF(sym50_04 IS NOT NULL AND sym50_10 IS NOT NULL) AS n, CORR(sym50_04, sym50_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_04, sym50_10), VAR_POP(sym50_04)) AS slope, AVG(IF(sym50_10 IS NOT NULL, ABS(sym50_10 - sym50_04), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 4 AS t1, 12 AS t2, COUNTIF(sym50_04 IS NOT NULL AND sym50_12 IS NOT NULL) AS n, CORR(sym50_04, sym50_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_04, sym50_12), VAR_POP(sym50_04)) AS slope, AVG(IF(sym50_12 IS NOT NULL, ABS(sym50_12 - sym50_04), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 4 AS t1, 15 AS t2, COUNTIF(sym50_04 IS NOT NULL AND sym50_15 IS NOT NULL) AS n, CORR(sym50_04, sym50_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_04, sym50_15), VAR_POP(sym50_04)) AS slope, AVG(IF(sym50_15 IS NOT NULL, ABS(sym50_15 - sym50_04), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 4 AS t1, 20 AS t2, COUNTIF(sym50_04 IS NOT NULL AND sym50_20 IS NOT NULL) AS n, CORR(sym50_04, sym50_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_04, sym50_20), VAR_POP(sym50_04)) AS slope, AVG(IF(sym50_20 IS NOT NULL, ABS(sym50_20 - sym50_04), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 4 AS t1, 25 AS t2, COUNTIF(sym50_04 IS NOT NULL AND sym50_25 IS NOT NULL) AS n, CORR(sym50_04, sym50_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_04, sym50_25), VAR_POP(sym50_04)) AS slope, AVG(IF(sym50_25 IS NOT NULL, ABS(sym50_25 - sym50_04), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 4 AS t1, 30 AS t2, COUNTIF(sym50_04 IS NOT NULL AND sym50_30 IS NOT NULL) AS n, CORR(sym50_04, sym50_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_04, sym50_30), VAR_POP(sym50_04)) AS slope, AVG(IF(sym50_30 IS NOT NULL, ABS(sym50_30 - sym50_04), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 5 AS t1, 6 AS t2, COUNTIF(sym50_05 IS NOT NULL AND sym50_06 IS NOT NULL) AS n, CORR(sym50_05, sym50_06) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_05, sym50_06), VAR_POP(sym50_05)) AS slope, AVG(IF(sym50_06 IS NOT NULL, ABS(sym50_06 - sym50_05), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 5 AS t1, 8 AS t2, COUNTIF(sym50_05 IS NOT NULL AND sym50_08 IS NOT NULL) AS n, CORR(sym50_05, sym50_08) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_05, sym50_08), VAR_POP(sym50_05)) AS slope, AVG(IF(sym50_08 IS NOT NULL, ABS(sym50_08 - sym50_05), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 5 AS t1, 10 AS t2, COUNTIF(sym50_05 IS NOT NULL AND sym50_10 IS NOT NULL) AS n, CORR(sym50_05, sym50_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_05, sym50_10), VAR_POP(sym50_05)) AS slope, AVG(IF(sym50_10 IS NOT NULL, ABS(sym50_10 - sym50_05), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 5 AS t1, 12 AS t2, COUNTIF(sym50_05 IS NOT NULL AND sym50_12 IS NOT NULL) AS n, CORR(sym50_05, sym50_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_05, sym50_12), VAR_POP(sym50_05)) AS slope, AVG(IF(sym50_12 IS NOT NULL, ABS(sym50_12 - sym50_05), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 5 AS t1, 15 AS t2, COUNTIF(sym50_05 IS NOT NULL AND sym50_15 IS NOT NULL) AS n, CORR(sym50_05, sym50_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_05, sym50_15), VAR_POP(sym50_05)) AS slope, AVG(IF(sym50_15 IS NOT NULL, ABS(sym50_15 - sym50_05), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 5 AS t1, 20 AS t2, COUNTIF(sym50_05 IS NOT NULL AND sym50_20 IS NOT NULL) AS n, CORR(sym50_05, sym50_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_05, sym50_20), VAR_POP(sym50_05)) AS slope, AVG(IF(sym50_20 IS NOT NULL, ABS(sym50_20 - sym50_05), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 5 AS t1, 25 AS t2, COUNTIF(sym50_05 IS NOT NULL AND sym50_25 IS NOT NULL) AS n, CORR(sym50_05, sym50_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_05, sym50_25), VAR_POP(sym50_05)) AS slope, AVG(IF(sym50_25 IS NOT NULL, ABS(sym50_25 - sym50_05), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 5 AS t1, 30 AS t2, COUNTIF(sym50_05 IS NOT NULL AND sym50_30 IS NOT NULL) AS n, CORR(sym50_05, sym50_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_05, sym50_30), VAR_POP(sym50_05)) AS slope, AVG(IF(sym50_30 IS NOT NULL, ABS(sym50_30 - sym50_05), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 6 AS t1, 8 AS t2, COUNTIF(sym50_06 IS NOT NULL AND sym50_08 IS NOT NULL) AS n, CORR(sym50_06, sym50_08) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_06, sym50_08), VAR_POP(sym50_06)) AS slope, AVG(IF(sym50_08 IS NOT NULL, ABS(sym50_08 - sym50_06), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 6 AS t1, 10 AS t2, COUNTIF(sym50_06 IS NOT NULL AND sym50_10 IS NOT NULL) AS n, CORR(sym50_06, sym50_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_06, sym50_10), VAR_POP(sym50_06)) AS slope, AVG(IF(sym50_10 IS NOT NULL, ABS(sym50_10 - sym50_06), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 6 AS t1, 12 AS t2, COUNTIF(sym50_06 IS NOT NULL AND sym50_12 IS NOT NULL) AS n, CORR(sym50_06, sym50_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_06, sym50_12), VAR_POP(sym50_06)) AS slope, AVG(IF(sym50_12 IS NOT NULL, ABS(sym50_12 - sym50_06), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 6 AS t1, 15 AS t2, COUNTIF(sym50_06 IS NOT NULL AND sym50_15 IS NOT NULL) AS n, CORR(sym50_06, sym50_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_06, sym50_15), VAR_POP(sym50_06)) AS slope, AVG(IF(sym50_15 IS NOT NULL, ABS(sym50_15 - sym50_06), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 6 AS t1, 20 AS t2, COUNTIF(sym50_06 IS NOT NULL AND sym50_20 IS NOT NULL) AS n, CORR(sym50_06, sym50_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_06, sym50_20), VAR_POP(sym50_06)) AS slope, AVG(IF(sym50_20 IS NOT NULL, ABS(sym50_20 - sym50_06), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 6 AS t1, 25 AS t2, COUNTIF(sym50_06 IS NOT NULL AND sym50_25 IS NOT NULL) AS n, CORR(sym50_06, sym50_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_06, sym50_25), VAR_POP(sym50_06)) AS slope, AVG(IF(sym50_25 IS NOT NULL, ABS(sym50_25 - sym50_06), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 6 AS t1, 30 AS t2, COUNTIF(sym50_06 IS NOT NULL AND sym50_30 IS NOT NULL) AS n, CORR(sym50_06, sym50_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_06, sym50_30), VAR_POP(sym50_06)) AS slope, AVG(IF(sym50_30 IS NOT NULL, ABS(sym50_30 - sym50_06), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 8 AS t1, 10 AS t2, COUNTIF(sym50_08 IS NOT NULL AND sym50_10 IS NOT NULL) AS n, CORR(sym50_08, sym50_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_08, sym50_10), VAR_POP(sym50_08)) AS slope, AVG(IF(sym50_10 IS NOT NULL, ABS(sym50_10 - sym50_08), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 8 AS t1, 12 AS t2, COUNTIF(sym50_08 IS NOT NULL AND sym50_12 IS NOT NULL) AS n, CORR(sym50_08, sym50_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_08, sym50_12), VAR_POP(sym50_08)) AS slope, AVG(IF(sym50_12 IS NOT NULL, ABS(sym50_12 - sym50_08), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 8 AS t1, 15 AS t2, COUNTIF(sym50_08 IS NOT NULL AND sym50_15 IS NOT NULL) AS n, CORR(sym50_08, sym50_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_08, sym50_15), VAR_POP(sym50_08)) AS slope, AVG(IF(sym50_15 IS NOT NULL, ABS(sym50_15 - sym50_08), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 8 AS t1, 20 AS t2, COUNTIF(sym50_08 IS NOT NULL AND sym50_20 IS NOT NULL) AS n, CORR(sym50_08, sym50_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_08, sym50_20), VAR_POP(sym50_08)) AS slope, AVG(IF(sym50_20 IS NOT NULL, ABS(sym50_20 - sym50_08), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 8 AS t1, 25 AS t2, COUNTIF(sym50_08 IS NOT NULL AND sym50_25 IS NOT NULL) AS n, CORR(sym50_08, sym50_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_08, sym50_25), VAR_POP(sym50_08)) AS slope, AVG(IF(sym50_25 IS NOT NULL, ABS(sym50_25 - sym50_08), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 8 AS t1, 30 AS t2, COUNTIF(sym50_08 IS NOT NULL AND sym50_30 IS NOT NULL) AS n, CORR(sym50_08, sym50_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_08, sym50_30), VAR_POP(sym50_08)) AS slope, AVG(IF(sym50_30 IS NOT NULL, ABS(sym50_30 - sym50_08), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 10 AS t1, 12 AS t2, COUNTIF(sym50_10 IS NOT NULL AND sym50_12 IS NOT NULL) AS n, CORR(sym50_10, sym50_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_10, sym50_12), VAR_POP(sym50_10)) AS slope, AVG(IF(sym50_12 IS NOT NULL, ABS(sym50_12 - sym50_10), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 10 AS t1, 15 AS t2, COUNTIF(sym50_10 IS NOT NULL AND sym50_15 IS NOT NULL) AS n, CORR(sym50_10, sym50_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_10, sym50_15), VAR_POP(sym50_10)) AS slope, AVG(IF(sym50_15 IS NOT NULL, ABS(sym50_15 - sym50_10), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 10 AS t1, 20 AS t2, COUNTIF(sym50_10 IS NOT NULL AND sym50_20 IS NOT NULL) AS n, CORR(sym50_10, sym50_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_10, sym50_20), VAR_POP(sym50_10)) AS slope, AVG(IF(sym50_20 IS NOT NULL, ABS(sym50_20 - sym50_10), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 10 AS t1, 25 AS t2, COUNTIF(sym50_10 IS NOT NULL AND sym50_25 IS NOT NULL) AS n, CORR(sym50_10, sym50_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_10, sym50_25), VAR_POP(sym50_10)) AS slope, AVG(IF(sym50_25 IS NOT NULL, ABS(sym50_25 - sym50_10), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 10 AS t1, 30 AS t2, COUNTIF(sym50_10 IS NOT NULL AND sym50_30 IS NOT NULL) AS n, CORR(sym50_10, sym50_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_10, sym50_30), VAR_POP(sym50_10)) AS slope, AVG(IF(sym50_30 IS NOT NULL, ABS(sym50_30 - sym50_10), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 12 AS t1, 15 AS t2, COUNTIF(sym50_12 IS NOT NULL AND sym50_15 IS NOT NULL) AS n, CORR(sym50_12, sym50_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_12, sym50_15), VAR_POP(sym50_12)) AS slope, AVG(IF(sym50_15 IS NOT NULL, ABS(sym50_15 - sym50_12), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 12 AS t1, 20 AS t2, COUNTIF(sym50_12 IS NOT NULL AND sym50_20 IS NOT NULL) AS n, CORR(sym50_12, sym50_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_12, sym50_20), VAR_POP(sym50_12)) AS slope, AVG(IF(sym50_20 IS NOT NULL, ABS(sym50_20 - sym50_12), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 12 AS t1, 25 AS t2, COUNTIF(sym50_12 IS NOT NULL AND sym50_25 IS NOT NULL) AS n, CORR(sym50_12, sym50_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_12, sym50_25), VAR_POP(sym50_12)) AS slope, AVG(IF(sym50_25 IS NOT NULL, ABS(sym50_25 - sym50_12), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 12 AS t1, 30 AS t2, COUNTIF(sym50_12 IS NOT NULL AND sym50_30 IS NOT NULL) AS n, CORR(sym50_12, sym50_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_12, sym50_30), VAR_POP(sym50_12)) AS slope, AVG(IF(sym50_30 IS NOT NULL, ABS(sym50_30 - sym50_12), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 15 AS t1, 20 AS t2, COUNTIF(sym50_15 IS NOT NULL AND sym50_20 IS NOT NULL) AS n, CORR(sym50_15, sym50_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_15, sym50_20), VAR_POP(sym50_15)) AS slope, AVG(IF(sym50_20 IS NOT NULL, ABS(sym50_20 - sym50_15), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 15 AS t1, 25 AS t2, COUNTIF(sym50_15 IS NOT NULL AND sym50_25 IS NOT NULL) AS n, CORR(sym50_15, sym50_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_15, sym50_25), VAR_POP(sym50_15)) AS slope, AVG(IF(sym50_25 IS NOT NULL, ABS(sym50_25 - sym50_15), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 15 AS t1, 30 AS t2, COUNTIF(sym50_15 IS NOT NULL AND sym50_30 IS NOT NULL) AS n, CORR(sym50_15, sym50_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_15, sym50_30), VAR_POP(sym50_15)) AS slope, AVG(IF(sym50_30 IS NOT NULL, ABS(sym50_30 - sym50_15), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 20 AS t1, 25 AS t2, COUNTIF(sym50_20 IS NOT NULL AND sym50_25 IS NOT NULL) AS n, CORR(sym50_20, sym50_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_20, sym50_25), VAR_POP(sym50_20)) AS slope, AVG(IF(sym50_25 IS NOT NULL, ABS(sym50_25 - sym50_20), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 20 AS t1, 30 AS t2, COUNTIF(sym50_20 IS NOT NULL AND sym50_30 IS NOT NULL) AS n, CORR(sym50_20, sym50_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_20, sym50_30), VAR_POP(sym50_20)) AS slope, AVG(IF(sym50_30 IS NOT NULL, ABS(sym50_30 - sym50_20), NULL)) AS mean_abs_change),
        STRUCT('sym50' AS metric, 25 AS t1, 30 AS t2, COUNTIF(sym50_25 IS NOT NULL AND sym50_30 IS NOT NULL) AS n, CORR(sym50_25, sym50_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym50_25, sym50_30), VAR_POP(sym50_25)) AS slope, AVG(IF(sym50_30 IS NOT NULL, ABS(sym50_30 - sym50_25), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 1 AS t1, 2 AS t2, COUNTIF(sym_01 IS NOT NULL AND sym_02 IS NOT NULL) AS n, CORR(sym_01, sym_02) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_01, sym_02), VAR_POP(sym_01)) AS slope, AVG(IF(sym_02 IS NOT NULL, ABS(sym_02 - sym_01), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 1 AS t1, 3 AS t2, COUNTIF(sym_01 IS NOT NULL AND sym_03 IS NOT NULL) AS n, CORR(sym_01, sym_03) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_01, sym_03), VAR_POP(sym_01)) AS slope, AVG(IF(sym_03 IS NOT NULL, ABS(sym_03 - sym_01), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 1 AS t1, 4 AS t2, COUNTIF(sym_01 IS NOT NULL AND sym_04 IS NOT NULL) AS n, CORR(sym_01, sym_04) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_01, sym_04), VAR_POP(sym_01)) AS slope, AVG(IF(sym_04 IS NOT NULL, ABS(sym_04 - sym_01), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 1 AS t1, 5 AS t2, COUNTIF(sym_01 IS NOT NULL AND sym_05 IS NOT NULL) AS n, CORR(sym_01, sym_05) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_01, sym_05), VAR_POP(sym_01)) AS slope, AVG(IF(sym_05 IS NOT NULL, ABS(sym_05 - sym_01), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 1 AS t1, 6 AS t2, COUNTIF(sym_01 IS NOT NULL AND sym_06 IS NOT NULL) AS n, CORR(sym_01, sym_06) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_01, sym_06), VAR_POP(sym_01)) AS slope, AVG(IF(sym_06 IS NOT NULL, ABS(sym_06 - sym_01), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 1 AS t1, 8 AS t2, COUNTIF(sym_01 IS NOT NULL AND sym_08 IS NOT NULL) AS n, CORR(sym_01, sym_08) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_01, sym_08), VAR_POP(sym_01)) AS slope, AVG(IF(sym_08 IS NOT NULL, ABS(sym_08 - sym_01), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 1 AS t1, 10 AS t2, COUNTIF(sym_01 IS NOT NULL AND sym_10 IS NOT NULL) AS n, CORR(sym_01, sym_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_01, sym_10), VAR_POP(sym_01)) AS slope, AVG(IF(sym_10 IS NOT NULL, ABS(sym_10 - sym_01), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 1 AS t1, 12 AS t2, COUNTIF(sym_01 IS NOT NULL AND sym_12 IS NOT NULL) AS n, CORR(sym_01, sym_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_01, sym_12), VAR_POP(sym_01)) AS slope, AVG(IF(sym_12 IS NOT NULL, ABS(sym_12 - sym_01), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 1 AS t1, 15 AS t2, COUNTIF(sym_01 IS NOT NULL AND sym_15 IS NOT NULL) AS n, CORR(sym_01, sym_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_01, sym_15), VAR_POP(sym_01)) AS slope, AVG(IF(sym_15 IS NOT NULL, ABS(sym_15 - sym_01), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 1 AS t1, 20 AS t2, COUNTIF(sym_01 IS NOT NULL AND sym_20 IS NOT NULL) AS n, CORR(sym_01, sym_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_01, sym_20), VAR_POP(sym_01)) AS slope, AVG(IF(sym_20 IS NOT NULL, ABS(sym_20 - sym_01), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 1 AS t1, 25 AS t2, COUNTIF(sym_01 IS NOT NULL AND sym_25 IS NOT NULL) AS n, CORR(sym_01, sym_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_01, sym_25), VAR_POP(sym_01)) AS slope, AVG(IF(sym_25 IS NOT NULL, ABS(sym_25 - sym_01), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 1 AS t1, 30 AS t2, COUNTIF(sym_01 IS NOT NULL AND sym_30 IS NOT NULL) AS n, CORR(sym_01, sym_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_01, sym_30), VAR_POP(sym_01)) AS slope, AVG(IF(sym_30 IS NOT NULL, ABS(sym_30 - sym_01), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 2 AS t1, 3 AS t2, COUNTIF(sym_02 IS NOT NULL AND sym_03 IS NOT NULL) AS n, CORR(sym_02, sym_03) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_02, sym_03), VAR_POP(sym_02)) AS slope, AVG(IF(sym_03 IS NOT NULL, ABS(sym_03 - sym_02), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 2 AS t1, 4 AS t2, COUNTIF(sym_02 IS NOT NULL AND sym_04 IS NOT NULL) AS n, CORR(sym_02, sym_04) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_02, sym_04), VAR_POP(sym_02)) AS slope, AVG(IF(sym_04 IS NOT NULL, ABS(sym_04 - sym_02), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 2 AS t1, 5 AS t2, COUNTIF(sym_02 IS NOT NULL AND sym_05 IS NOT NULL) AS n, CORR(sym_02, sym_05) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_02, sym_05), VAR_POP(sym_02)) AS slope, AVG(IF(sym_05 IS NOT NULL, ABS(sym_05 - sym_02), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 2 AS t1, 6 AS t2, COUNTIF(sym_02 IS NOT NULL AND sym_06 IS NOT NULL) AS n, CORR(sym_02, sym_06) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_02, sym_06), VAR_POP(sym_02)) AS slope, AVG(IF(sym_06 IS NOT NULL, ABS(sym_06 - sym_02), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 2 AS t1, 8 AS t2, COUNTIF(sym_02 IS NOT NULL AND sym_08 IS NOT NULL) AS n, CORR(sym_02, sym_08) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_02, sym_08), VAR_POP(sym_02)) AS slope, AVG(IF(sym_08 IS NOT NULL, ABS(sym_08 - sym_02), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 2 AS t1, 10 AS t2, COUNTIF(sym_02 IS NOT NULL AND sym_10 IS NOT NULL) AS n, CORR(sym_02, sym_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_02, sym_10), VAR_POP(sym_02)) AS slope, AVG(IF(sym_10 IS NOT NULL, ABS(sym_10 - sym_02), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 2 AS t1, 12 AS t2, COUNTIF(sym_02 IS NOT NULL AND sym_12 IS NOT NULL) AS n, CORR(sym_02, sym_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_02, sym_12), VAR_POP(sym_02)) AS slope, AVG(IF(sym_12 IS NOT NULL, ABS(sym_12 - sym_02), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 2 AS t1, 15 AS t2, COUNTIF(sym_02 IS NOT NULL AND sym_15 IS NOT NULL) AS n, CORR(sym_02, sym_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_02, sym_15), VAR_POP(sym_02)) AS slope, AVG(IF(sym_15 IS NOT NULL, ABS(sym_15 - sym_02), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 2 AS t1, 20 AS t2, COUNTIF(sym_02 IS NOT NULL AND sym_20 IS NOT NULL) AS n, CORR(sym_02, sym_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_02, sym_20), VAR_POP(sym_02)) AS slope, AVG(IF(sym_20 IS NOT NULL, ABS(sym_20 - sym_02), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 2 AS t1, 25 AS t2, COUNTIF(sym_02 IS NOT NULL AND sym_25 IS NOT NULL) AS n, CORR(sym_02, sym_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_02, sym_25), VAR_POP(sym_02)) AS slope, AVG(IF(sym_25 IS NOT NULL, ABS(sym_25 - sym_02), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 2 AS t1, 30 AS t2, COUNTIF(sym_02 IS NOT NULL AND sym_30 IS NOT NULL) AS n, CORR(sym_02, sym_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_02, sym_30), VAR_POP(sym_02)) AS slope, AVG(IF(sym_30 IS NOT NULL, ABS(sym_30 - sym_02), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 3 AS t1, 4 AS t2, COUNTIF(sym_03 IS NOT NULL AND sym_04 IS NOT NULL) AS n, CORR(sym_03, sym_04) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_03, sym_04), VAR_POP(sym_03)) AS slope, AVG(IF(sym_04 IS NOT NULL, ABS(sym_04 - sym_03), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 3 AS t1, 5 AS t2, COUNTIF(sym_03 IS NOT NULL AND sym_05 IS NOT NULL) AS n, CORR(sym_03, sym_05) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_03, sym_05), VAR_POP(sym_03)) AS slope, AVG(IF(sym_05 IS NOT NULL, ABS(sym_05 - sym_03), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 3 AS t1, 6 AS t2, COUNTIF(sym_03 IS NOT NULL AND sym_06 IS NOT NULL) AS n, CORR(sym_03, sym_06) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_03, sym_06), VAR_POP(sym_03)) AS slope, AVG(IF(sym_06 IS NOT NULL, ABS(sym_06 - sym_03), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 3 AS t1, 8 AS t2, COUNTIF(sym_03 IS NOT NULL AND sym_08 IS NOT NULL) AS n, CORR(sym_03, sym_08) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_03, sym_08), VAR_POP(sym_03)) AS slope, AVG(IF(sym_08 IS NOT NULL, ABS(sym_08 - sym_03), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 3 AS t1, 10 AS t2, COUNTIF(sym_03 IS NOT NULL AND sym_10 IS NOT NULL) AS n, CORR(sym_03, sym_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_03, sym_10), VAR_POP(sym_03)) AS slope, AVG(IF(sym_10 IS NOT NULL, ABS(sym_10 - sym_03), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 3 AS t1, 12 AS t2, COUNTIF(sym_03 IS NOT NULL AND sym_12 IS NOT NULL) AS n, CORR(sym_03, sym_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_03, sym_12), VAR_POP(sym_03)) AS slope, AVG(IF(sym_12 IS NOT NULL, ABS(sym_12 - sym_03), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 3 AS t1, 15 AS t2, COUNTIF(sym_03 IS NOT NULL AND sym_15 IS NOT NULL) AS n, CORR(sym_03, sym_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_03, sym_15), VAR_POP(sym_03)) AS slope, AVG(IF(sym_15 IS NOT NULL, ABS(sym_15 - sym_03), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 3 AS t1, 20 AS t2, COUNTIF(sym_03 IS NOT NULL AND sym_20 IS NOT NULL) AS n, CORR(sym_03, sym_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_03, sym_20), VAR_POP(sym_03)) AS slope, AVG(IF(sym_20 IS NOT NULL, ABS(sym_20 - sym_03), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 3 AS t1, 25 AS t2, COUNTIF(sym_03 IS NOT NULL AND sym_25 IS NOT NULL) AS n, CORR(sym_03, sym_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_03, sym_25), VAR_POP(sym_03)) AS slope, AVG(IF(sym_25 IS NOT NULL, ABS(sym_25 - sym_03), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 3 AS t1, 30 AS t2, COUNTIF(sym_03 IS NOT NULL AND sym_30 IS NOT NULL) AS n, CORR(sym_03, sym_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_03, sym_30), VAR_POP(sym_03)) AS slope, AVG(IF(sym_30 IS NOT NULL, ABS(sym_30 - sym_03), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 4 AS t1, 5 AS t2, COUNTIF(sym_04 IS NOT NULL AND sym_05 IS NOT NULL) AS n, CORR(sym_04, sym_05) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_04, sym_05), VAR_POP(sym_04)) AS slope, AVG(IF(sym_05 IS NOT NULL, ABS(sym_05 - sym_04), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 4 AS t1, 6 AS t2, COUNTIF(sym_04 IS NOT NULL AND sym_06 IS NOT NULL) AS n, CORR(sym_04, sym_06) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_04, sym_06), VAR_POP(sym_04)) AS slope, AVG(IF(sym_06 IS NOT NULL, ABS(sym_06 - sym_04), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 4 AS t1, 8 AS t2, COUNTIF(sym_04 IS NOT NULL AND sym_08 IS NOT NULL) AS n, CORR(sym_04, sym_08) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_04, sym_08), VAR_POP(sym_04)) AS slope, AVG(IF(sym_08 IS NOT NULL, ABS(sym_08 - sym_04), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 4 AS t1, 10 AS t2, COUNTIF(sym_04 IS NOT NULL AND sym_10 IS NOT NULL) AS n, CORR(sym_04, sym_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_04, sym_10), VAR_POP(sym_04)) AS slope, AVG(IF(sym_10 IS NOT NULL, ABS(sym_10 - sym_04), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 4 AS t1, 12 AS t2, COUNTIF(sym_04 IS NOT NULL AND sym_12 IS NOT NULL) AS n, CORR(sym_04, sym_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_04, sym_12), VAR_POP(sym_04)) AS slope, AVG(IF(sym_12 IS NOT NULL, ABS(sym_12 - sym_04), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 4 AS t1, 15 AS t2, COUNTIF(sym_04 IS NOT NULL AND sym_15 IS NOT NULL) AS n, CORR(sym_04, sym_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_04, sym_15), VAR_POP(sym_04)) AS slope, AVG(IF(sym_15 IS NOT NULL, ABS(sym_15 - sym_04), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 4 AS t1, 20 AS t2, COUNTIF(sym_04 IS NOT NULL AND sym_20 IS NOT NULL) AS n, CORR(sym_04, sym_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_04, sym_20), VAR_POP(sym_04)) AS slope, AVG(IF(sym_20 IS NOT NULL, ABS(sym_20 - sym_04), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 4 AS t1, 25 AS t2, COUNTIF(sym_04 IS NOT NULL AND sym_25 IS NOT NULL) AS n, CORR(sym_04, sym_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_04, sym_25), VAR_POP(sym_04)) AS slope, AVG(IF(sym_25 IS NOT NULL, ABS(sym_25 - sym_04), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 4 AS t1, 30 AS t2, COUNTIF(sym_04 IS NOT NULL AND sym_30 IS NOT NULL) AS n, CORR(sym_04, sym_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_04, sym_30), VAR_POP(sym_04)) AS slope, AVG(IF(sym_30 IS NOT NULL, ABS(sym_30 - sym_04), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 5 AS t1, 6 AS t2, COUNTIF(sym_05 IS NOT NULL AND sym_06 IS NOT NULL) AS n, CORR(sym_05, sym_06) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_05, sym_06), VAR_POP(sym_05)) AS slope, AVG(IF(sym_06 IS NOT NULL, ABS(sym_06 - sym_05), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 5 AS t1, 8 AS t2, COUNTIF(sym_05 IS NOT NULL AND sym_08 IS NOT NULL) AS n, CORR(sym_05, sym_08) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_05, sym_08), VAR_POP(sym_05)) AS slope, AVG(IF(sym_08 IS NOT NULL, ABS(sym_08 - sym_05), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 5 AS t1, 10 AS t2, COUNTIF(sym_05 IS NOT NULL AND sym_10 IS NOT NULL) AS n, CORR(sym_05, sym_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_05, sym_10), VAR_POP(sym_05)) AS slope, AVG(IF(sym_10 IS NOT NULL, ABS(sym_10 - sym_05), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 5 AS t1, 12 AS t2, COUNTIF(sym_05 IS NOT NULL AND sym_12 IS NOT NULL) AS n, CORR(sym_05, sym_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_05, sym_12), VAR_POP(sym_05)) AS slope, AVG(IF(sym_12 IS NOT NULL, ABS(sym_12 - sym_05), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 5 AS t1, 15 AS t2, COUNTIF(sym_05 IS NOT NULL AND sym_15 IS NOT NULL) AS n, CORR(sym_05, sym_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_05, sym_15), VAR_POP(sym_05)) AS slope, AVG(IF(sym_15 IS NOT NULL, ABS(sym_15 - sym_05), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 5 AS t1, 20 AS t2, COUNTIF(sym_05 IS NOT NULL AND sym_20 IS NOT NULL) AS n, CORR(sym_05, sym_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_05, sym_20), VAR_POP(sym_05)) AS slope, AVG(IF(sym_20 IS NOT NULL, ABS(sym_20 - sym_05), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 5 AS t1, 25 AS t2, COUNTIF(sym_05 IS NOT NULL AND sym_25 IS NOT NULL) AS n, CORR(sym_05, sym_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_05, sym_25), VAR_POP(sym_05)) AS slope, AVG(IF(sym_25 IS NOT NULL, ABS(sym_25 - sym_05), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 5 AS t1, 30 AS t2, COUNTIF(sym_05 IS NOT NULL AND sym_30 IS NOT NULL) AS n, CORR(sym_05, sym_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_05, sym_30), VAR_POP(sym_05)) AS slope, AVG(IF(sym_30 IS NOT NULL, ABS(sym_30 - sym_05), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 6 AS t1, 8 AS t2, COUNTIF(sym_06 IS NOT NULL AND sym_08 IS NOT NULL) AS n, CORR(sym_06, sym_08) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_06, sym_08), VAR_POP(sym_06)) AS slope, AVG(IF(sym_08 IS NOT NULL, ABS(sym_08 - sym_06), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 6 AS t1, 10 AS t2, COUNTIF(sym_06 IS NOT NULL AND sym_10 IS NOT NULL) AS n, CORR(sym_06, sym_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_06, sym_10), VAR_POP(sym_06)) AS slope, AVG(IF(sym_10 IS NOT NULL, ABS(sym_10 - sym_06), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 6 AS t1, 12 AS t2, COUNTIF(sym_06 IS NOT NULL AND sym_12 IS NOT NULL) AS n, CORR(sym_06, sym_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_06, sym_12), VAR_POP(sym_06)) AS slope, AVG(IF(sym_12 IS NOT NULL, ABS(sym_12 - sym_06), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 6 AS t1, 15 AS t2, COUNTIF(sym_06 IS NOT NULL AND sym_15 IS NOT NULL) AS n, CORR(sym_06, sym_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_06, sym_15), VAR_POP(sym_06)) AS slope, AVG(IF(sym_15 IS NOT NULL, ABS(sym_15 - sym_06), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 6 AS t1, 20 AS t2, COUNTIF(sym_06 IS NOT NULL AND sym_20 IS NOT NULL) AS n, CORR(sym_06, sym_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_06, sym_20), VAR_POP(sym_06)) AS slope, AVG(IF(sym_20 IS NOT NULL, ABS(sym_20 - sym_06), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 6 AS t1, 25 AS t2, COUNTIF(sym_06 IS NOT NULL AND sym_25 IS NOT NULL) AS n, CORR(sym_06, sym_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_06, sym_25), VAR_POP(sym_06)) AS slope, AVG(IF(sym_25 IS NOT NULL, ABS(sym_25 - sym_06), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 6 AS t1, 30 AS t2, COUNTIF(sym_06 IS NOT NULL AND sym_30 IS NOT NULL) AS n, CORR(sym_06, sym_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_06, sym_30), VAR_POP(sym_06)) AS slope, AVG(IF(sym_30 IS NOT NULL, ABS(sym_30 - sym_06), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 8 AS t1, 10 AS t2, COUNTIF(sym_08 IS NOT NULL AND sym_10 IS NOT NULL) AS n, CORR(sym_08, sym_10) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_08, sym_10), VAR_POP(sym_08)) AS slope, AVG(IF(sym_10 IS NOT NULL, ABS(sym_10 - sym_08), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 8 AS t1, 12 AS t2, COUNTIF(sym_08 IS NOT NULL AND sym_12 IS NOT NULL) AS n, CORR(sym_08, sym_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_08, sym_12), VAR_POP(sym_08)) AS slope, AVG(IF(sym_12 IS NOT NULL, ABS(sym_12 - sym_08), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 8 AS t1, 15 AS t2, COUNTIF(sym_08 IS NOT NULL AND sym_15 IS NOT NULL) AS n, CORR(sym_08, sym_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_08, sym_15), VAR_POP(sym_08)) AS slope, AVG(IF(sym_15 IS NOT NULL, ABS(sym_15 - sym_08), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 8 AS t1, 20 AS t2, COUNTIF(sym_08 IS NOT NULL AND sym_20 IS NOT NULL) AS n, CORR(sym_08, sym_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_08, sym_20), VAR_POP(sym_08)) AS slope, AVG(IF(sym_20 IS NOT NULL, ABS(sym_20 - sym_08), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 8 AS t1, 25 AS t2, COUNTIF(sym_08 IS NOT NULL AND sym_25 IS NOT NULL) AS n, CORR(sym_08, sym_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_08, sym_25), VAR_POP(sym_08)) AS slope, AVG(IF(sym_25 IS NOT NULL, ABS(sym_25 - sym_08), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 8 AS t1, 30 AS t2, COUNTIF(sym_08 IS NOT NULL AND sym_30 IS NOT NULL) AS n, CORR(sym_08, sym_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_08, sym_30), VAR_POP(sym_08)) AS slope, AVG(IF(sym_30 IS NOT NULL, ABS(sym_30 - sym_08), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 10 AS t1, 12 AS t2, COUNTIF(sym_10 IS NOT NULL AND sym_12 IS NOT NULL) AS n, CORR(sym_10, sym_12) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_10, sym_12), VAR_POP(sym_10)) AS slope, AVG(IF(sym_12 IS NOT NULL, ABS(sym_12 - sym_10), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 10 AS t1, 15 AS t2, COUNTIF(sym_10 IS NOT NULL AND sym_15 IS NOT NULL) AS n, CORR(sym_10, sym_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_10, sym_15), VAR_POP(sym_10)) AS slope, AVG(IF(sym_15 IS NOT NULL, ABS(sym_15 - sym_10), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 10 AS t1, 20 AS t2, COUNTIF(sym_10 IS NOT NULL AND sym_20 IS NOT NULL) AS n, CORR(sym_10, sym_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_10, sym_20), VAR_POP(sym_10)) AS slope, AVG(IF(sym_20 IS NOT NULL, ABS(sym_20 - sym_10), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 10 AS t1, 25 AS t2, COUNTIF(sym_10 IS NOT NULL AND sym_25 IS NOT NULL) AS n, CORR(sym_10, sym_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_10, sym_25), VAR_POP(sym_10)) AS slope, AVG(IF(sym_25 IS NOT NULL, ABS(sym_25 - sym_10), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 10 AS t1, 30 AS t2, COUNTIF(sym_10 IS NOT NULL AND sym_30 IS NOT NULL) AS n, CORR(sym_10, sym_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_10, sym_30), VAR_POP(sym_10)) AS slope, AVG(IF(sym_30 IS NOT NULL, ABS(sym_30 - sym_10), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 12 AS t1, 15 AS t2, COUNTIF(sym_12 IS NOT NULL AND sym_15 IS NOT NULL) AS n, CORR(sym_12, sym_15) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_12, sym_15), VAR_POP(sym_12)) AS slope, AVG(IF(sym_15 IS NOT NULL, ABS(sym_15 - sym_12), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 12 AS t1, 20 AS t2, COUNTIF(sym_12 IS NOT NULL AND sym_20 IS NOT NULL) AS n, CORR(sym_12, sym_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_12, sym_20), VAR_POP(sym_12)) AS slope, AVG(IF(sym_20 IS NOT NULL, ABS(sym_20 - sym_12), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 12 AS t1, 25 AS t2, COUNTIF(sym_12 IS NOT NULL AND sym_25 IS NOT NULL) AS n, CORR(sym_12, sym_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_12, sym_25), VAR_POP(sym_12)) AS slope, AVG(IF(sym_25 IS NOT NULL, ABS(sym_25 - sym_12), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 12 AS t1, 30 AS t2, COUNTIF(sym_12 IS NOT NULL AND sym_30 IS NOT NULL) AS n, CORR(sym_12, sym_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_12, sym_30), VAR_POP(sym_12)) AS slope, AVG(IF(sym_30 IS NOT NULL, ABS(sym_30 - sym_12), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 15 AS t1, 20 AS t2, COUNTIF(sym_15 IS NOT NULL AND sym_20 IS NOT NULL) AS n, CORR(sym_15, sym_20) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_15, sym_20), VAR_POP(sym_15)) AS slope, AVG(IF(sym_20 IS NOT NULL, ABS(sym_20 - sym_15), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 15 AS t1, 25 AS t2, COUNTIF(sym_15 IS NOT NULL AND sym_25 IS NOT NULL) AS n, CORR(sym_15, sym_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_15, sym_25), VAR_POP(sym_15)) AS slope, AVG(IF(sym_25 IS NOT NULL, ABS(sym_25 - sym_15), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 15 AS t1, 30 AS t2, COUNTIF(sym_15 IS NOT NULL AND sym_30 IS NOT NULL) AS n, CORR(sym_15, sym_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_15, sym_30), VAR_POP(sym_15)) AS slope, AVG(IF(sym_30 IS NOT NULL, ABS(sym_30 - sym_15), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 20 AS t1, 25 AS t2, COUNTIF(sym_20 IS NOT NULL AND sym_25 IS NOT NULL) AS n, CORR(sym_20, sym_25) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_20, sym_25), VAR_POP(sym_20)) AS slope, AVG(IF(sym_25 IS NOT NULL, ABS(sym_25 - sym_20), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 20 AS t1, 30 AS t2, COUNTIF(sym_20 IS NOT NULL AND sym_30 IS NOT NULL) AS n, CORR(sym_20, sym_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_20, sym_30), VAR_POP(sym_20)) AS slope, AVG(IF(sym_30 IS NOT NULL, ABS(sym_30 - sym_20), NULL)) AS mean_abs_change),
        STRUCT('sym' AS metric, 25 AS t1, 30 AS t2, COUNTIF(sym_25 IS NOT NULL AND sym_30 IS NOT NULL) AS n, CORR(sym_25, sym_30) AS pearson, SAFE_DIVIDE(COVAR_POP(sym_25, sym_30), VAR_POP(sym_25)) AS slope, AVG(IF(sym_30 IS NOT NULL, ABS(sym_30 - sym_25), NULL)) AS mean_abs_change)
      ] AS s
    FROM wide
    GROUP BY start_year
  )
SELECT 'active' AS benchmark, start_year, s.*
FROM stats, UNNEST(s) AS s
WHERE s.n > 0;

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.sym_survivorship_auc`
OPTIONS (description = 'AUC of age-5 symmetric PiP for later publishing. Built by paper/sql/16_sym_stability_survivorship.sql')
AS
WITH
  base AS (
    SELECT b.cohort5, b.pub_11_15, b.pub_16_20, p.pct_sym50, p.pct_sym
    FROM `scholar-version2.paper_rp.survivorship_base` AS b
    JOIN `scholar-version2.paper_rp.sym_pct_by_age` AS p
      ON p.authorid = b.authorid AND p.start_year = b.start_year AND p.age = 5
  ),
  long_form AS (
    SELECT cohort5, metric, value, outcome, y
    FROM base,
      UNNEST([STRUCT('sym50' AS metric, pct_sym50 AS value), ('sym', pct_sym)]),
      UNNEST([STRUCT('pub_11_15' AS outcome, pub_11_15 AS y), ('pub_16_20', pub_16_20)])
  ),
  grouped AS (
    SELECT CAST(cohort5 AS STRING) AS cohort, metric, value, outcome, y FROM long_form
    UNION ALL
    SELECT 'pooled', metric, value, outcome, y FROM long_form
  ),
  ranked AS (
    SELECT cohort, metric, outcome, y,
      (RANK() OVER (PARTITION BY cohort, metric, outcome ORDER BY value)
       + COUNT(*) OVER (PARTITION BY cohort, metric, outcome) + 1
       - RANK() OVER (PARTITION BY cohort, metric, outcome ORDER BY value DESC)) / 2 AS r
    FROM grouped
  )
SELECT cohort, metric, outcome, COUNTIF(y) AS n_pos, COUNTIF(NOT y) AS n_neg,
  (SUM(IF(y, r, 0)) - COUNTIF(y) * (COUNTIF(y) + 1) / 2) / (COUNTIF(y) * COUNTIF(NOT y)) AS auc
FROM ranked
GROUP BY cohort, metric, outcome;

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.sym_decomposition`
OPTIONS (description = 'Variance split of log sym_tau50 into log F(n) and log Q. Built by paper/sql/16_sym_stability_survivorship.sql')
AS
WITH
  d AS (
    SELECT age, LN(sym_tau50) AS ls, LN(prod_pct) AS lf, LN(quality_mult_tau50) AS lq, quality_mult_tau50 AS q
    FROM `scholar-version2.paper_rp.sym_by_age`
    WHERE age IN (5, 10, 20) AND start_year BETWEEN 1975 AND 2005 AND sym_tau50 > 0
  ),
  zeros AS (
    SELECT age, COUNTIF(sym_tau50 = 0) / COUNT(*) AS share_zero
    FROM `scholar-version2.paper_rp.sym_by_age`
    WHERE age IN (5, 10, 20) AND start_year BETWEEN 1975 AND 2005
    GROUP BY age
  )
SELECT d.age, COUNT(*) AS n,
  VAR_POP(lf) / VAR_POP(ls) AS share_productivity,
  VAR_POP(lq) / VAR_POP(ls) AS share_quality,
  2 * COVAR_POP(lf, lq) / VAR_POP(ls) AS share_covariance,
  COUNTIF(q > 1) / COUNT(*) AS share_q_above_1,
  APPROX_QUANTILES(q, 10) AS q_deciles,
  ANY_VALUE(z.share_zero) AS share_zero
FROM d
JOIN zeros AS z USING (age)
GROUP BY d.age;
