-- 11: Which indicator predicts future work best: raw values or within-cohort percentiles?
--
-- Within one start-year cohort a raw metric and its percentile rank people identically, so they can only differ when
-- people from different cohorts are compared. Two settings, both with 'active' peers and information up to the
-- evaluation year Y only:
--   same_age   career age t1 in {5, 10, 15}, start years 1975 .. 2010 - t1 pooled (a tenure-committee comparison)
--   same_year  Y in {2000, 2005}, career ages 2-30 pooled (comparing people of different seniority at one time)
-- Outcomes come from the author's articles published in Y+1 .. Y+10 (none of them enter the predictors), each scored at
-- its own age 5 within its publication year (`below` from 04):
--   fut_top10     number of future articles in the top 10% of their year (share of same-year articles with fewer
--                 citations >= 0.9)
--   fut_any_top1  at least one future article in the top 1%
--   fut_mean_pct  mean Hazen percentile of the future articles (authors with at least one)
-- Predictors at Y: h-index, PiP-AUC, citations, article count, each raw and as a within-cohort percentile (from 07).
-- Results (`prediction_results`): Spearman correlation with fut_top10 and with fut_mean_pct, and ROC AUC for
-- fut_any_top1, per setting and predictor. Ranks use average ranks for ties.
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.prediction_base`
OPTIONS (description = 'Predictors at evaluation year and outcomes from the next 10 years of articles. Built by paper/sql/11_prediction.sql')
AS
WITH
  base AS (
    SELECT 'same_age' AS scenario, age AS setting, authorid, start_year, age, eval_year,
      h_index, pct_h, pip, pct_pip, citations, pct_citations, n_articles, pct_articles
    FROM `scholar-version2.paper_rp.author_percentiles_by_age`
    WHERE benchmark = 'active' AND age IN (5, 10, 15) AND start_year BETWEEN 1975 AND 2010 - age
    UNION ALL
    SELECT 'same_year', eval_year, authorid, start_year, age, eval_year,
      h_index, pct_h, pip, pct_pip, citations, pct_citations, n_articles, pct_articles
    FROM `scholar-version2.paper_rp.author_percentiles_by_age`
    WHERE benchmark = 'active' AND eval_year IN (2000, 2005) AND age BETWEEN 2 AND 30
  ),
  future AS (
    SELECT authorid, pub_year, below[OFFSET(5)] AS b5, hazen[OFFSET(5)] AS h5
    FROM `scholar-version2.paper_rp.author_articles`
    -- must cover every start year admitted by `base`: same_age reaches 2010 - 5 = 2005, same_year starts at 1970
    WHERE pub_year BETWEEN 1981 AND 2020 AND start_year BETWEEN 1970 AND 2005
  ),
  outcomes AS (
    SELECT b.scenario, b.setting, b.authorid,
      COUNT(f.pub_year) AS fut_articles,
      COUNTIF(f.b5 >= 0.9) AS fut_top10,
      COUNTIF(f.b5 >= 0.99) > 0 AS fut_any_top1,
      AVG(f.h5) AS fut_mean_pct
    FROM base AS b
    LEFT JOIN future AS f
      ON f.authorid = b.authorid AND f.pub_year > b.eval_year AND f.pub_year <= b.eval_year + 10
    GROUP BY b.scenario, b.setting, b.authorid
  )
SELECT b.*, o.fut_articles, o.fut_top10, o.fut_any_top1, o.fut_mean_pct
FROM base AS b
JOIN outcomes AS o USING (scenario, setting, authorid);

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.prediction_results`
OPTIONS (description = 'Spearman / AUC of raw and percentile predictors for future work. Built by paper/sql/11_prediction.sql')
AS
WITH
  long_form AS (
    SELECT scenario, setting, predictor, x, fut_top10, fut_any_top1, fut_mean_pct, fut_articles
    FROM `scholar-version2.paper_rp.prediction_base`,
      UNNEST([STRUCT('h_raw' AS predictor, CAST(h_index AS FLOAT64) AS x), ('h_pct', pct_h),
              ('pip_raw', pip), ('pip_pct', pct_pip),
              ('citations_raw', CAST(citations AS FLOAT64)), ('citations_pct', pct_citations),
              ('articles_raw', CAST(n_articles AS FLOAT64)), ('articles_pct', pct_articles)])
  ),
  ranked AS (
    SELECT scenario, setting, predictor, fut_any_top1,
      -- average ranks: (rank ascending + N + 1 - rank descending) / 2
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_top10) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_top10 DESC)) / 2 AS ry
    FROM long_form
    WINDOW w AS (PARTITION BY scenario, setting, predictor)
  ),
  ranked_quality AS (
    SELECT scenario, setting, predictor,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_mean_pct) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_mean_pct DESC)) / 2 AS rq
    FROM long_form
    WHERE fut_articles > 0
    WINDOW w AS (PARTITION BY scenario, setting, predictor)
  ),
  main AS (
    SELECT scenario, setting, predictor, COUNT(*) AS n,
      CORR(rx, ry) AS spearman_top10,
      (SUM(IF(fut_any_top1, rx, 0)) - COUNTIF(fut_any_top1) * (COUNTIF(fut_any_top1) + 1) / 2)
        / (COUNTIF(fut_any_top1) * COUNTIF(NOT fut_any_top1)) AS auc_any_top1
    FROM ranked
    GROUP BY scenario, setting, predictor
  ),
  quality AS (
    SELECT scenario, setting, predictor, COUNT(*) AS n_with_future, CORR(rx, rq) AS spearman_mean_pct
    FROM ranked_quality
    GROUP BY scenario, setting, predictor
  )
SELECT * FROM main JOIN quality USING (scenario, setting, predictor);
