-- 23: Holdout check for the choice of tau. The cutoff tau = 0.5 was chosen from results that pooled all cohorts. Here
-- the same-age prediction results of 20 (`baseline_prediction_long`, raw predictors) are split by start year into a
-- choice sample (1975-1990) and a test sample (1991 and later: 1991-2005 at age 5, 1991-2000 at age 10, 1991-1995 at
-- age 15). If tau had been tuned to noise, the ordering of the cutoffs and the comparisons with other indicators would
-- not carry over to the test cohorts.
--   tau_holdout   Spearman / AUC per (setting, sample, predictor), same definitions as 11
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.tau_holdout`
OPTIONS (description = 'Prediction results by choice (1975-1990) and test (1991+) cohorts. Built by paper/sql/23_tau_holdout.sql')
AS
WITH
  long_form AS (
    SELECT setting, IF(start_year <= 1990, 'choice', 'test') AS sample, predictor, x,
      fut_top10, fut_any_top1, fut_mean_pct, fut_articles
    FROM `scholar-version2.paper_rp.baseline_prediction_long`
    WHERE scenario = 'same_age' AND ENDS_WITH(predictor, '_raw') AND x IS NOT NULL
  ),
  ranked AS (
    SELECT setting, sample, predictor, fut_any_top1,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_top10) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_top10 DESC)) / 2 AS ry
    FROM long_form
    WINDOW w AS (PARTITION BY setting, sample, predictor)
  ),
  ranked_quality AS (
    SELECT setting, sample, predictor,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_mean_pct) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_mean_pct DESC)) / 2 AS rq
    FROM long_form
    WHERE fut_articles > 0
    WINDOW w AS (PARTITION BY setting, sample, predictor)
  )
SELECT m.*, q.spearman_mean_pct
FROM (
  SELECT setting, sample, predictor, COUNT(*) AS n, CORR(rx, ry) AS spearman_top10,
    (SUM(IF(fut_any_top1, rx, 0)) - COUNTIF(fut_any_top1) * (COUNTIF(fut_any_top1) + 1) / 2)
      / (COUNTIF(fut_any_top1) * COUNTIF(NOT fut_any_top1)) AS auc_any_top1
  FROM ranked GROUP BY setting, sample, predictor
) AS m
JOIN (SELECT setting, sample, predictor, CORR(rx, rq) AS spearman_mean_pct FROM ranked_quality
      GROUP BY setting, sample, predictor) AS q
USING (setting, sample, predictor);
