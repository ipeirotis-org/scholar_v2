-- 20: PiP against percentile-based scholar indicators, in the prediction setup of 11.
--
-- Predictors at evaluation year Y, from the author's articles published up to Y, each with its percentile y (`below`,
-- share of same-year articles with fewer citations at Y), so they use the same paper percentiles as PiP:
--   top10        number of articles in the top 10% of their year (y >= 0.9)          (Leiden PP-top-10% count)
--   top10_age    top10 / career age                                                  (Bornmann & Marx 2014 style)
--   top1         number of articles in the top 1% (y >= 0.99)
--   i3           sum of the percentiles, sum of y                                    (I3, Leydesdorff & Bornmann 2011)
--   mean_pct     mean of the percentiles
--   t25 .. t90   cutoff-matched sums, T_tau = 1/(1-tau) * sum of (y' - tau)^+ = 1/(1-tau) * integral of n(t) from tau to
--                1, with y' = (FLOOR(40 y) + 1) / 40 the percentile on PiP's 40 classes (see 25). T_tau uses the same
--                percentiles and the same cutoff as PiP at tau and differs from it only in the peer comparison G_t
--                inside the integral. The paper calls it PiP-simple.
--   combo25 .. combo75  w * (cohort percentile of the article count) + (1 - w) * (cohort percentile of the mean
--                percentile), w = 0.25, 0.5, 0.75: a direct mix of quantity and impact (cohort = scenario x setting x
--                start year, Hazen percentiles); not in the paper, kept for the record (FINDINGS.md §19)
-- plus, for reference, PiP at every cutoff of 12 (sym, sym_tau25 .. sym_tau90; pip_sym50 is the main score), the
-- original PiP-AUC, the h-index and the article count. Each also enters as a
-- within-cohort Hazen percentile, (N + rank ascending - rank descending) / 2N within (scenario, setting, start year);
-- the populations of `prediction_base` contain every active author of each cohort at that age, so this is the cohort
-- percentile among active peers.
-- Outcomes and settings are those of 11 (`prediction_base`).
--   baseline_predictors            one row per (scenario, setting, author): predictors and outcomes
--   baseline_prediction            Spearman / AUC per setting and predictor, all cohorts pooled
--   baseline_prediction_by_cohort  the same per start year, same-age settings only (replication across cohorts)
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.baseline_predictors`
OPTIONS (description = 'Percentile-based scholar indicators at the evaluation year, next to PiP and the outcomes of 11. Built by paper/sql/20_percentile_baselines.sql')
AS
WITH
  base AS (
    SELECT scenario, setting, authorid, start_year, age, eval_year, h_index, n_articles,
      fut_top10, fut_any_top1, fut_mean_pct, fut_articles
    FROM `scholar-version2.paper_rp.prediction_base`
  ),
  author_years AS (SELECT DISTINCT authorid, start_year, eval_year FROM base),
  papers AS (
    SELECT x.authorid, x.start_year, x.eval_year, a.below[OFFSET(x.eval_year - a.pub_year)] AS y
    FROM author_years AS x
    JOIN `scholar-version2.paper_rp.author_articles` AS a
      ON a.authorid = x.authorid AND a.start_year = x.start_year
    WHERE a.pub_year <= x.eval_year
  ),
  agg AS (
    SELECT authorid, start_year, eval_year,
      COUNT(*) AS n_papers, COUNTIF(y >= 0.9) AS top10, COUNTIF(y >= 0.99) AS top1, SUM(y) AS i3, AVG(y) AS mean_pct,
      SUM(GREATEST(0, (FLOOR(y * 40) + 1) / 40 - 0.25)) / 0.75 AS t25,
      SUM(GREATEST(0, (FLOOR(y * 40) + 1) / 40 - 0.5)) / 0.5 AS t50,
      SUM(GREATEST(0, (FLOOR(y * 40) + 1) / 40 - 0.75)) / 0.25 AS t75,
      SUM(GREATEST(0, (FLOOR(y * 40) + 1) / 40 - 0.9)) / 0.1 AS t90
    FROM papers
    GROUP BY authorid, start_year, eval_year
  ),
  joined AS (
    SELECT
      b.*, g.n_papers, g.top10, g.top10 / b.age AS top10_age, g.top1, g.i3, g.mean_pct, g.t25, g.t50, g.t75, g.t90,
      v.sym, v.sym_tau25, v.sym_tau50, v.sym_tau60, v.sym_tau75, v.sym_tau90, v.pip
    FROM base AS b
    JOIN agg AS g USING (authorid, start_year, eval_year)
    LEFT JOIN `scholar-version2.paper_rp.pip_variants` AS v
      ON v.authorid = b.authorid AND v.start_year = b.start_year AND v.age = b.age
  ),
  cohort_pct AS (
    SELECT *,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY n_articles) - RANK() OVER (w ORDER BY n_articles DESC))
        / (2 * COUNT(*) OVER w) AS pct_n,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY mean_pct) - RANK() OVER (w ORDER BY mean_pct DESC))
        / (2 * COUNT(*) OVER w) AS pct_mean
    FROM joined
    WINDOW w AS (PARTITION BY scenario, setting, start_year)
  )
SELECT * EXCEPT (pct_n, pct_mean),
  0.25 * pct_n + 0.75 * pct_mean AS combo25,
  0.5 * pct_n + 0.5 * pct_mean AS combo50,
  0.75 * pct_n + 0.25 * pct_mean AS combo75
FROM cohort_pct;

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.baseline_prediction_long`
OPTIONS (description = 'Long form of baseline_predictors with within-cohort percentiles. Built by paper/sql/20_percentile_baselines.sql')
AS
WITH
  long_raw AS (
    SELECT scenario, setting, start_year, authorid, predictor, x, fut_top10, fut_any_top1, fut_mean_pct, fut_articles
    FROM `scholar-version2.paper_rp.baseline_predictors`,
      UNNEST([STRUCT('pip_sym50' AS predictor, sym_tau50 AS x), ('pip_sym0', sym), ('pip_sym25', sym_tau25),
              ('pip_sym60', sym_tau60), ('pip_sym75', sym_tau75), ('pip_sym90', sym_tau90), ('pip_original', pip),
              ('h', CAST(h_index AS FLOAT64)), ('articles', CAST(n_articles AS FLOAT64)),
              ('top10', CAST(top10 AS FLOAT64)), ('top10_age', top10_age), ('top1', CAST(top1 AS FLOAT64)),
              ('i3', i3), ('mean_pct', mean_pct), ('t25', t25), ('t50', t50), ('t75', t75), ('t90', t90),
              ('combo25', combo25), ('combo50', combo50), ('combo75', combo75)])
  )
SELECT scenario, setting, start_year, authorid, CONCAT(predictor, '_raw') AS predictor, x,
  fut_top10, fut_any_top1, fut_mean_pct, fut_articles
FROM long_raw
UNION ALL
SELECT scenario, setting, start_year, authorid, CONCAT(predictor, '_pct'),
  (COUNT(*) OVER w + RANK() OVER (w ORDER BY x) - RANK() OVER (w ORDER BY x DESC)) / (2 * COUNT(*) OVER w),
  fut_top10, fut_any_top1, fut_mean_pct, fut_articles
FROM long_raw
WHERE predictor != 'top10_age'  -- within a cohort at one age it ranks exactly like top10
WINDOW w AS (PARTITION BY scenario, setting, start_year, predictor);

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.baseline_prediction`
OPTIONS (description = 'Spearman / AUC of PiP and percentile-based indicators for future work, cohorts pooled. Built by paper/sql/20_percentile_baselines.sql')
AS
WITH
  ranked AS (
    SELECT scenario, setting, predictor, fut_any_top1,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_top10) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_top10 DESC)) / 2 AS ry
    FROM `scholar-version2.paper_rp.baseline_prediction_long`
    WHERE x IS NOT NULL
    WINDOW w AS (PARTITION BY scenario, setting, predictor)
  ),
  ranked_quality AS (
    SELECT scenario, setting, predictor,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_mean_pct) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_mean_pct DESC)) / 2 AS rq
    FROM `scholar-version2.paper_rp.baseline_prediction_long`
    WHERE x IS NOT NULL AND fut_articles > 0
    WINDOW w AS (PARTITION BY scenario, setting, predictor)
  )
SELECT m.*, q.spearman_mean_pct
FROM (
  SELECT scenario, setting, predictor, COUNT(*) AS n, CORR(rx, ry) AS spearman_top10,
    (SUM(IF(fut_any_top1, rx, 0)) - COUNTIF(fut_any_top1) * (COUNTIF(fut_any_top1) + 1) / 2)
      / (COUNTIF(fut_any_top1) * COUNTIF(NOT fut_any_top1)) AS auc_any_top1
  FROM ranked GROUP BY scenario, setting, predictor
) AS m
JOIN (SELECT scenario, setting, predictor, CORR(rx, rq) AS spearman_mean_pct FROM ranked_quality
      GROUP BY scenario, setting, predictor) AS q
USING (scenario, setting, predictor);

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.baseline_prediction_by_cohort`
OPTIONS (description = 'Spearman / AUC per start-year cohort, same-age settings, raw predictors. Built by paper/sql/20_percentile_baselines.sql')
AS
WITH
  long_form AS (
    SELECT * FROM `scholar-version2.paper_rp.baseline_prediction_long`
    WHERE scenario = 'same_age' AND ENDS_WITH(predictor, '_raw') AND x IS NOT NULL
  ),
  ranked AS (
    SELECT setting, start_year, predictor, fut_any_top1,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_top10) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_top10 DESC)) / 2 AS ry
    FROM long_form
    WINDOW w AS (PARTITION BY setting, start_year, predictor)
  ),
  ranked_quality AS (
    SELECT setting, start_year, predictor,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_mean_pct) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_mean_pct DESC)) / 2 AS rq
    FROM long_form
    WHERE fut_articles > 0
    WINDOW w AS (PARTITION BY setting, start_year, predictor)
  )
SELECT m.*, q.spearman_mean_pct
FROM (
  SELECT setting, start_year, predictor, COUNT(*) AS n, CORR(rx, ry) AS spearman_top10,
    (SUM(IF(fut_any_top1, rx, 0)) - COUNTIF(fut_any_top1) * (COUNTIF(fut_any_top1) + 1) / 2)
      / (COUNTIF(fut_any_top1) * COUNTIF(NOT fut_any_top1)) AS auc_any_top1
  FROM ranked GROUP BY setting, start_year, predictor
) AS m
JOIN (SELECT setting, start_year, predictor, CORR(rx, rq) AS spearman_mean_pct FROM ranked_quality
      GROUP BY setting, start_year, predictor) AS q
USING (setting, start_year, predictor);
