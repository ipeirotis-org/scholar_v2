-- 13: Benchmarks for the PiP variants of 12 (raw scores; all variants are bounded and cohort-relative).
--   variant_prediction  the 11_prediction outcomes (future top-10% count, any future top-1%, future mean quality) per
--                       setting, for each variant; same Spearman / AUC definitions as 11
--   variant_profile     at career age 10, start years 1975-2010: within-cohort Spearman of each variant with article
--                       count, mean paper percentile, h-index, and the number of top-2.5% articles (n_top)
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.variant_prediction`
OPTIONS (description = 'Prediction benchmarks for PiP variants. Built by paper/sql/13_variant_benchmarks.sql')
AS
WITH
  joined AS (
    SELECT b.scenario, b.setting, b.fut_top10, b.fut_any_top1, b.fut_mean_pct, b.fut_articles,
      v.pip, v.pip_tau25, v.pip_tau50, v.sym, v.sym_tau50, CAST(v.h_index AS FLOAT64) AS h
    FROM `scholar-version2.paper_rp.prediction_base` AS b
    JOIN `scholar-version2.paper_rp.pip_variants` AS v
      ON v.authorid = b.authorid AND v.start_year = b.start_year AND v.age = b.age
  ),
  long_form AS (
    SELECT scenario, setting, variant, x, fut_top10, fut_any_top1, fut_mean_pct, fut_articles
    FROM joined,
      UNNEST([STRUCT('pip' AS variant, pip AS x), ('pip_tau25', pip_tau25), ('pip_tau50', pip_tau50),
              ('sym', sym), ('sym_tau50', sym_tau50), ('h_raw', h)])
  ),
  ranked AS (
    SELECT scenario, setting, variant, fut_any_top1,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_top10) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_top10 DESC)) / 2 AS ry
    FROM long_form
    WINDOW w AS (PARTITION BY scenario, setting, variant)
  ),
  ranked_quality AS (
    SELECT scenario, setting, variant,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_mean_pct) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_mean_pct DESC)) / 2 AS rq
    FROM long_form
    WHERE fut_articles > 0
    WINDOW w AS (PARTITION BY scenario, setting, variant)
  )
SELECT m.*, q.spearman_mean_pct
FROM (
  SELECT scenario, setting, variant, COUNT(*) AS n, CORR(rx, ry) AS spearman_top10,
    (SUM(IF(fut_any_top1, rx, 0)) - COUNTIF(fut_any_top1) * (COUNTIF(fut_any_top1) + 1) / 2)
      / (COUNTIF(fut_any_top1) * COUNTIF(NOT fut_any_top1)) AS auc_any_top1
  FROM ranked GROUP BY scenario, setting, variant
) AS m
JOIN (
  SELECT scenario, setting, variant, CORR(rx, rq) AS spearman_mean_pct
  FROM ranked_quality GROUP BY scenario, setting, variant
) AS q USING (scenario, setting, variant);

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.variant_profile`
OPTIONS (description = 'What each PiP variant tracks at career age 10. Built by paper/sql/13_variant_benchmarks.sql')
AS
WITH
  base AS (
    SELECT start_year, variant, x, CAST(n_articles AS FLOAT64) AS n_articles, mean_below,
      CAST(h_index AS FLOAT64) AS h, CAST(n_top AS FLOAT64) AS n_top
    FROM `scholar-version2.paper_rp.pip_variants`,
      UNNEST([STRUCT('pip' AS variant, pip AS x), ('pip_tau25', pip_tau25), ('pip_tau50', pip_tau50),
              ('sym', sym), ('sym_tau50', sym_tau50)])
    WHERE age = 10 AND start_year BETWEEN 1975 AND 2010
  ),
  ranked AS (
    SELECT variant,
      PERCENT_RANK() OVER (w ORDER BY x) AS rx,
      PERCENT_RANK() OVER (w ORDER BY n_articles) AS r_articles,
      PERCENT_RANK() OVER (w ORDER BY mean_below) AS r_mean,
      PERCENT_RANK() OVER (w ORDER BY h) AS r_h,
      PERCENT_RANK() OVER (w ORDER BY n_top) AS r_top
    FROM base
    WINDOW w AS (PARTITION BY start_year, variant)
  )
SELECT variant, COUNT(*) AS n,
  CORR(rx, r_articles) AS vs_articles, CORR(rx, r_mean) AS vs_mean_quality,
  CORR(rx, r_h) AS vs_h, CORR(rx, r_top) AS vs_top_papers
FROM ranked
GROUP BY variant;
