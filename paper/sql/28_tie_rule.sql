-- 28: What does the tie rule of the standing change? Reads pip_variants (12), which holds PiP with a tie counted one
-- half (sym_tauXX, the paper's rule) and with a tie counted zero (strict_tauXX, the rule of the first version).
-- Eligible authors at career age 10, start years 1960-2015, cutoffs tau = 0.5, 0.75 and 0.9.
--   tie_rule_comparison  one row per rule ('half' or 'zero') and cutoff (tau_pct = 100 tau):
--     mean, mean_min, mean_max      mean score over all authors, and the lowest and highest cohort mean
--     mean_abs_gap, share_within_05, max_abs_gap
--                                   distance between the score and the author's percentile in the cohort (Hazen
--                                   mid-rank of the score within the start year)
--     p25_*, median_*, p75_*, p90_* range of the cohort quantiles over start years; median_1960, median_2010
--     floor_min, floor_max          range over cohorts of the lowest score in the cohort
--     share_at_floor_min, _max      range over cohorts of the share of authors with that lowest score
--     rank_corr_between_rules       within-cohort rank correlation of the two rules' scores at this cutoff
--     top10_overlap                 share of each cohort's top 10% under one rule that the other rule also puts there
--     spearman_top10, auc_any_top1, spearman_mean_pct
--                                   prediction of the next ten years (start years 1975-2000), as in 13
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.tie_rule_comparison`
OPTIONS (description = 'PiP with ties counted one half against ties counted zero, career age 10. Built by paper/sql/28_tie_rule.sql')
AS
WITH
  long_form AS (
    SELECT authorid, start_year, rule, tau_pct, x
    FROM `scholar-version2.paper_rp.pip_variants`,
      UNNEST([STRUCT('half' AS rule, 50 AS tau_pct, sym_tau50 AS x), ('half', 75, sym_tau75), ('half', 90, sym_tau90),
              ('zero', 50, strict_tau50), ('zero', 75, strict_tau75), ('zero', 90, strict_tau90)])
    WHERE age = 10 AND start_year BETWEEN 1960 AND 2015
  ),
  ranked AS (
    SELECT authorid, start_year, rule, tau_pct, x,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY x) - RANK() OVER (w ORDER BY x DESC)) / (2 * COUNT(*) OVER w) AS pct,
      MIN(x) OVER w AS floor_x,
      PERCENTILE_CONT(x, 0.25) OVER w AS p25, PERCENTILE_CONT(x, 0.5) OVER w AS p50,
      PERCENTILE_CONT(x, 0.75) OVER w AS p75, PERCENTILE_CONT(x, 0.9) OVER w AS p90
    FROM long_form
    WINDOW w AS (PARTITION BY rule, tau_pct, start_year)
  ),
  cohorts AS (
    SELECT rule, tau_pct, start_year, COUNT(*) AS n, AVG(x) AS mean, ANY_VALUE(p25) AS p25, ANY_VALUE(p50) AS p50,
      ANY_VALUE(p75) AS p75, ANY_VALUE(p90) AS p90, ANY_VALUE(floor_x) AS floor_x,
      AVG(IF(x = floor_x, 1, 0)) AS share_at_floor, SUM(ABS(x - pct)) AS sum_gap,
      COUNTIF(ABS(x - pct) <= 0.05) AS n_within_05, MAX(ABS(x - pct)) AS max_gap
    FROM ranked
    GROUP BY rule, tau_pct, start_year
  ),
  dist AS (
    SELECT rule, tau_pct, SUM(n) AS authors, COUNT(*) AS cohorts,
      SUM(mean * n) / SUM(n) AS mean, MIN(mean) AS mean_min, MAX(mean) AS mean_max,
      SUM(sum_gap) / SUM(n) AS mean_abs_gap, SUM(n_within_05) / SUM(n) AS share_within_05, MAX(max_gap) AS max_abs_gap,
      MIN(p25) AS p25_min, MAX(p25) AS p25_max, MIN(p50) AS median_min, MAX(p50) AS median_max,
      MIN(p75) AS p75_min, MAX(p75) AS p75_max, MIN(p90) AS p90_min, MAX(p90) AS p90_max,
      MAX(IF(start_year = 1960, p50, NULL)) AS median_1960, MAX(IF(start_year = 2010, p50, NULL)) AS median_2010,
      MIN(floor_x) AS floor_min, MAX(floor_x) AS floor_max,
      MIN(share_at_floor) AS share_at_floor_min, MAX(share_at_floor) AS share_at_floor_max
    FROM cohorts
    GROUP BY rule, tau_pct
  ),
  agreement AS (
    SELECT h.tau_pct, CORR(h.pct, z.pct) AS rank_corr_between_rules,
      COUNTIF(h.pct >= 0.9 AND z.pct >= 0.9) / COUNTIF(h.pct >= 0.9) AS top10_overlap
    FROM ranked AS h
    JOIN ranked AS z USING (authorid, start_year, tau_pct)
    WHERE h.rule = 'half' AND z.rule = 'zero'
    GROUP BY h.tau_pct
  ),
  pred_base AS (
    SELECT l.rule, l.tau_pct, l.x, b.fut_top10, b.fut_any_top1, b.fut_mean_pct, b.fut_articles
    FROM long_form AS l
    JOIN `scholar-version2.paper_rp.prediction_base` AS b
      ON b.authorid = l.authorid AND b.start_year = l.start_year AND b.age = 10
    WHERE b.scenario = 'same_age' AND b.setting = 10
  ),
  pred_ranked AS (
    SELECT rule, tau_pct, fut_any_top1,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_top10) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_top10 DESC)) / 2 AS ry
    FROM pred_base
    WINDOW w AS (PARTITION BY rule, tau_pct)
  ),
  pred_quality AS (
    SELECT rule, tau_pct,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_mean_pct) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_mean_pct DESC)) / 2 AS rq
    FROM pred_base
    WHERE fut_articles > 0
    WINDOW w AS (PARTITION BY rule, tau_pct)
  ),
  pred AS (
    SELECT m.rule, m.tau_pct, m.n_prediction, m.spearman_top10, m.auc_any_top1, q.spearman_mean_pct
    FROM (
      SELECT rule, tau_pct, COUNT(*) AS n_prediction, CORR(rx, ry) AS spearman_top10,
        (SUM(IF(fut_any_top1, rx, 0)) - COUNTIF(fut_any_top1) * (COUNTIF(fut_any_top1) + 1) / 2)
          / (COUNTIF(fut_any_top1) * COUNTIF(NOT fut_any_top1)) AS auc_any_top1
      FROM pred_ranked GROUP BY rule, tau_pct
    ) AS m
    JOIN (SELECT rule, tau_pct, CORR(rx, rq) AS spearman_mean_pct FROM pred_quality GROUP BY rule, tau_pct) AS q
      USING (rule, tau_pct)
  )
SELECT d.*, a.rank_corr_between_rules, a.top10_overlap,
  p.n_prediction, p.spearman_top10, p.auc_any_top1, p.spearman_mean_pct
FROM dist AS d
JOIN agreement AS a USING (tau_pct)
JOIN pred AS p USING (rule, tau_pct);
