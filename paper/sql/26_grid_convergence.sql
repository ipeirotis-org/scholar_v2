-- 26: Does the 40-class grid change the results? Reads pip_exact_grid (run_25.sh grid): active authors at career age
-- 10, start years 1960-2015, PiP on grids K = 40 (production), 200 and 1000, and exact (K = 0), at tau = 0.5, 0.75, 0.9.
-- tau_pct = 100 tau (windows cannot be partitioned by a FLOAT64).
--   grid_validation    K = 40 against the production scores of 12 (pip_variants): the largest absolute difference
--                      (zero up to rounding if the rounding identity of 25 holds)
--   grid_convergence   per tau and grid, against the exact score, start years 1975-2010: mean difference, mean and
--                      largest absolute difference, within-cohort rank correlation (Hazen percentiles within start year,
--                      pooled), and the share of the exact cohort top 10% that the grid's cohort top 10% contains
--   grid_prediction    the Table 4 outcomes (career age 10, start years 1975-2000, raw scores pooled over cohorts), as
--                      in 11: Spearman with the future top-10% count, ROC AUC for a future top-1% article, Spearman with
--                      the mean percentile of future articles
--   grid_cohorts       per start year: mean, quartiles, 90th percentile and share at zero
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.grid_validation`
OPTIONS (description = 'K = 40 scores of 25 against the production scores of 12. Built by paper/sql/26_grid_convergence.sql')
AS
SELECT CAST(ROUND(e.tau * 100) AS INT64) AS tau_pct, COUNT(*) AS n,
  MAX(ABS(e.score - CASE WHEN e.tau = 0.5 THEN v.sym_tau50 WHEN e.tau = 0.75 THEN v.sym_tau75
                         ELSE v.sym_tau90 END)) AS max_abs_diff
FROM `scholar-version2.paper_rp.pip_exact_grid` AS e
JOIN `scholar-version2.paper_rp.pip_variants` AS v
  ON v.authorid = e.authorid AND v.start_year = e.start_year AND v.age = e.age
WHERE e.k_grid = 40
GROUP BY tau_pct;

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.grid_convergence`
OPTIONS (description = 'Grid scores against the exact integral, career age 10. Built by paper/sql/26_grid_convergence.sql')
AS
WITH
  pairs AS (
    SELECT a.authorid, a.start_year, CAST(ROUND(a.tau * 100) AS INT64) AS tau_pct, a.k_grid, a.score AS s,
      x.score AS s_exact
    FROM `scholar-version2.paper_rp.pip_exact_grid` AS a
    JOIN `scholar-version2.paper_rp.pip_exact_grid` AS x
      ON x.authorid = a.authorid AND x.start_year = a.start_year AND x.age = a.age AND x.tau = a.tau AND x.k_grid = 0
    WHERE a.k_grid != 0 AND a.start_year BETWEEN 1975 AND 2010
  ),
  ranked AS (
    SELECT *,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY s) - RANK() OVER (w ORDER BY s DESC)) / (2 * COUNT(*) OVER w) AS p,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY s_exact) - RANK() OVER (w ORDER BY s_exact DESC))
        / (2 * COUNT(*) OVER w) AS p_exact,
      ROW_NUMBER() OVER (w ORDER BY s DESC, FARM_FINGERPRINT(authorid)) <= 0.1 * COUNT(*) OVER w AS top,
      ROW_NUMBER() OVER (w ORDER BY s_exact DESC, FARM_FINGERPRINT(authorid)) <= 0.1 * COUNT(*) OVER w AS top_exact
    FROM pairs
    WINDOW w AS (PARTITION BY tau_pct, k_grid, start_year)
  )
SELECT tau_pct, k_grid, COUNT(*) AS n,
  AVG(s - s_exact) AS mean_diff, AVG(ABS(s - s_exact)) AS mean_abs_diff, MAX(ABS(s - s_exact)) AS max_abs_diff,
  CORR(p, p_exact) AS rank_corr_within_cohort,
  COUNTIF(top AND top_exact) / COUNTIF(top_exact) AS top10_overlap
FROM ranked
GROUP BY tau_pct, k_grid;

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.grid_prediction`
OPTIONS (description = 'Table 4 outcomes for PiP on each grid, career age 10. Built by paper/sql/26_grid_convergence.sql')
AS
WITH
  joined AS (
    SELECT CAST(ROUND(e.tau * 100) AS INT64) AS tau_pct, e.k_grid, e.score AS x, b.fut_top10, b.fut_any_top1, b.fut_mean_pct, b.fut_articles
    FROM `scholar-version2.paper_rp.pip_exact_grid` AS e
    JOIN `scholar-version2.paper_rp.prediction_base` AS b
      ON b.authorid = e.authorid AND b.start_year = e.start_year AND b.age = e.age
    WHERE b.scenario = 'same_age' AND b.setting = 10
  ),
  ranked AS (
    SELECT tau_pct, k_grid, fut_any_top1,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_top10) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_top10 DESC)) / 2 AS ry
    FROM joined
    WINDOW w AS (PARTITION BY tau_pct, k_grid)
  ),
  ranked_quality AS (
    SELECT tau_pct, k_grid,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_mean_pct) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_mean_pct DESC)) / 2 AS rq
    FROM joined
    WHERE fut_articles > 0
    WINDOW w AS (PARTITION BY tau_pct, k_grid)
  )
SELECT m.*, q.spearman_mean_pct
FROM (
  SELECT tau_pct, k_grid, COUNT(*) AS n, CORR(rx, ry) AS spearman_top10,
    (SUM(IF(fut_any_top1, rx, 0)) - COUNTIF(fut_any_top1) * (COUNTIF(fut_any_top1) + 1) / 2)
      / (COUNTIF(fut_any_top1) * COUNTIF(NOT fut_any_top1)) AS auc_any_top1
  FROM ranked GROUP BY tau_pct, k_grid
) AS m
JOIN (SELECT tau_pct, k_grid, CORR(rx, rq) AS spearman_mean_pct FROM ranked_quality GROUP BY tau_pct, k_grid) AS q
USING (tau_pct, k_grid);

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.grid_cohorts`
OPTIONS (description = 'Distribution of PiP by start year on each grid, career age 10. Built by paper/sql/26_grid_convergence.sql')
AS
SELECT CAST(ROUND(tau * 100) AS INT64) AS tau_pct, k_grid, start_year, COUNT(*) AS n, AVG(score) AS mean,
  APPROX_QUANTILES(score, 100)[OFFSET(25)] AS p25,
  APPROX_QUANTILES(score, 100)[OFFSET(50)] AS median,
  APPROX_QUANTILES(score, 100)[OFFSET(75)] AS p75,
  APPROX_QUANTILES(score, 100)[OFFSET(90)] AS p90,
  COUNTIF(score = 0) / COUNT(*) AS share_zero
FROM `scholar-version2.paper_rp.pip_exact_grid`
GROUP BY tau_pct, k_grid, start_year;
