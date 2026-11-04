-- 09: Does an early-career indicator predict who keeps publishing? (the survivorship question in v3's limitations)
--
-- Population: authors in the 'active' peer group at career age 5, start years 1980-2005 (so age 20 is observable).
-- Outcomes, from article counts (S2 disambiguation can split a profile, which looks like stopping):
--   pub_11_15  published at least one article at career ages 11-15, i.e. n_articles(15) > n_articles(10)
--   pub_16_20  published at least one article at career ages 16-20
-- Predictors: within-cohort percentiles at age 5 (from 07) of PiP-AUC, h-index, citations, article count, mean
-- paper percentile and v3's summed paper percentiles.
-- Output 1 (`survivorship_auc`): ROC AUC of each predictor for each outcome, by 5-year start cohort and pooled,
--   computed as the Mann-Whitney statistic with average ranks for ties.
-- Output 2 (`survivorship_deciles`): outcome rates by predictor decile at age 5, pooled over cohorts.
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.survivorship_base`
OPTIONS (description = 'Active authors at career age 5 (start 1980-2005) with age-5 percentiles and later publishing outcomes. Built by paper/sql/09_survivorship.sql')
AS
WITH
  at5 AS (
    SELECT authorid, start_year, pct_pip, pct_h, pct_citations, pct_articles, pct_mean, pct_sum
    FROM `scholar-version2.paper_rp.author_percentiles_by_age`
    WHERE benchmark = 'active' AND age = 5 AND start_year BETWEEN 1980 AND 2005
  ),
  counts AS (
    SELECT authorid,
      MAX(IF(age = 10, n_articles, NULL)) AS n10,
      MAX(IF(age = 15, n_articles, NULL)) AS n15,
      MAX(IF(age = 20, n_articles, NULL)) AS n20
    FROM `scholar-version2.paper_rp.author_pip_by_age`
    WHERE benchmark = 'active' AND age IN (10, 15, 20) AND start_year BETWEEN 1980 AND 2005
    GROUP BY authorid
  )
SELECT
  a.*,
  DIV(a.start_year, 5) * 5 AS cohort5,
  c.n15 > c.n10 AS pub_11_15,
  c.n20 > c.n15 AS pub_16_20
FROM at5 AS a
JOIN counts AS c USING (authorid);

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.survivorship_auc`
OPTIONS (description = 'ROC AUC of age-5 percentiles for later publishing. Built by paper/sql/09_survivorship.sql')
AS
WITH
  long_form AS (
    SELECT cohort5, metric, value, outcome, y
    FROM `scholar-version2.paper_rp.survivorship_base`,
      UNNEST([STRUCT('pip' AS metric, pct_pip AS value), ('h', pct_h), ('citations', pct_citations),
              ('articles', pct_articles), ('mean', pct_mean), ('sum', pct_sum)]),
      UNNEST([STRUCT('pub_11_15' AS outcome, pub_11_15 AS y), ('pub_16_20', pub_16_20)])
  ),
  grouped AS (
    SELECT CAST(cohort5 AS STRING) AS cohort, metric, value, outcome, y FROM long_form
    UNION ALL
    SELECT 'pooled', metric, value, outcome, y FROM long_form
  ),
  ranked AS (
    SELECT cohort, metric, outcome, y,
      -- average rank for ties: (rank ascending + N + 1 - rank descending) / 2
      (RANK() OVER (PARTITION BY cohort, metric, outcome ORDER BY value)
       + COUNT(*) OVER (PARTITION BY cohort, metric, outcome) + 1
       - RANK() OVER (PARTITION BY cohort, metric, outcome ORDER BY value DESC)) / 2 AS r
    FROM grouped
  )
SELECT cohort, metric, outcome,
  COUNTIF(y) AS n_pos, COUNTIF(NOT y) AS n_neg,
  (SUM(IF(y, r, 0)) - COUNTIF(y) * (COUNTIF(y) + 1) / 2) / (COUNTIF(y) * COUNTIF(NOT y)) AS auc
FROM ranked
GROUP BY cohort, metric, outcome;

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.survivorship_deciles`
OPTIONS (description = 'Later publishing rates by decile of age-5 percentiles. Built by paper/sql/09_survivorship.sql')
AS
SELECT metric, LEAST(9, CAST(FLOOR(value * 10) AS INT64)) + 1 AS decile, COUNT(*) AS authors,
  AVG(IF(pub_11_15, 1, 0)) AS rate_11_15, AVG(IF(pub_16_20, 1, 0)) AS rate_16_20
FROM `scholar-version2.paper_rp.survivorship_base`,
  UNNEST([STRUCT('pip' AS metric, pct_pip AS value), ('h', pct_h), ('citations', pct_citations),
          ('articles', pct_articles), ('mean', pct_mean), ('sum', pct_sum)])
GROUP BY metric, decile;
