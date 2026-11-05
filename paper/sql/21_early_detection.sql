-- 21: Early detection. How soon does each indicator pick out the scholars who will do well later?
-- Run through paper/sql/run_21.sh, which fills {SYM_TABLE} and {SUFFIX} for an eligibility rule:
--   active  {SYM_TABLE} = sym_by_age (15),       {SUFFIX} = ''       three articles and an h-index of three
--   min3    {SYM_TABLE} = sym_by_age_min3 (27),  {SUFFIX} = '_min3'  three articles, no h-index condition
--
-- Fixed populations, so that accuracy at different measurement ages can be compared: authors eligible under the rule
-- at career age s, for s = 3 and s = 5, start years 1975-2000. Eligibility persists, so every member is eligible (and
-- has a PiP score) at every later age. Each indicator is measured at career ages t = s .. 10 and predicts one fixed
-- outcome window: the author's articles published at career ages 11-20, each scored at its own age 5 within its
-- publication year (`below` / `hazen` from 04), so no outcome article enters any predictor.
--   fut_top10      number of those articles in the top 10% of their year
--   fut_any_top1   at least one in the top 1%
--   fut_mean       mean Hazen percentile (authors with at least one)
--   star           top 10% of the start-year cohort (within the population) by fut_top10; ties broken by fut_mean,
--                  then by a hash of the author id
-- Predictors at age t, each as a within-cohort Hazen percentile (start year x population x age), which ranks authors
-- within a cohort exactly as the raw value does and lets cohorts pool: PiP (sym_tau50), the original PiP-AUC, h-index,
-- citations, article count, top-10% count, I3 (sum of article percentiles), mean article percentile, the cutoff-matched
-- sum T_0.5 (sum of (y' - 0.5)^+ on PiP's 40 classes, see 20) and combo50 (the mean of the cohort percentiles of the
-- article count and of the mean article percentile).
--   early_detection_base{SUFFIX}      one row per (population, age, author): predictors and outcomes
--   early_detection_results{SUFFIX}   per (population, age, predictor): Spearman with fut_top10 and fut_mean, AUC
--                                     for fut_any_top1 and for star, and precision of the cohort's top 10% by the
--                                     predictor at finding stars (random = 0.10). precision_top10 picks the top 10%
--                                     with ties at the boundary broken by a hash of the author id; precision_top10_frac
--                                     is its expectation under random tie-breaking (authors tied at the boundary count
--                                     with weight (slots left) / (number tied)).
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.early_detection_base{SUFFIX}`
OPTIONS (description = 'Fixed populations measured at career ages s..10, outcomes from ages 11-20. Built by paper/sql/21_early_detection.sql')
AS
WITH
  pop AS (
    SELECT s AS pop_age, authorid, start_year
    FROM `scholar-version2.paper_rp.{SYM_TABLE}`, UNNEST([3, 5]) AS s
    WHERE age = s AND start_year BETWEEN 1975 AND 2000
  ),
  measured AS (
    SELECT p.pop_age, p.authorid, p.start_year, y.age AS t, y.eval_year,
      y.sym_tau50, y.pip, y.h_index, y.citations, y.n_articles
    FROM pop AS p
    JOIN `scholar-version2.paper_rp.{SYM_TABLE}` AS y
      ON y.authorid = p.authorid AND y.start_year = p.start_year AND y.age BETWEEN p.pop_age AND 10
  ),
  author_ages AS (SELECT DISTINCT authorid, start_year, t, eval_year FROM measured),
  pct_at_t AS (
    SELECT x.authorid, x.start_year, x.t,
      COUNTIF(a.below[OFFSET(x.eval_year - a.pub_year)] >= 0.9) AS top10,
      SUM(a.below[OFFSET(x.eval_year - a.pub_year)]) AS i3,
      AVG(a.below[OFFSET(x.eval_year - a.pub_year)]) AS mean_pct,
      SUM(GREATEST(0, (FLOOR(a.below[OFFSET(x.eval_year - a.pub_year)] * 40) + 1) / 40 - 0.5)) / 0.5 AS t50
    FROM author_ages AS x
    JOIN `scholar-version2.paper_rp.author_articles` AS a
      ON a.authorid = x.authorid AND a.start_year = x.start_year
    WHERE a.pub_year <= x.eval_year
    GROUP BY x.authorid, x.start_year, x.t
  ),
  future AS (
    SELECT p.pop_age, p.authorid, p.start_year,
      COUNT(a.pub_year) AS fut_articles,
      COUNTIF(a.below[OFFSET(5)] >= 0.9) AS fut_top10,
      COUNTIF(a.below[OFFSET(5)] >= 0.99) > 0 AS fut_any_top1,
      AVG(a.hazen[OFFSET(5)]) AS fut_mean
    FROM pop AS p
    LEFT JOIN `scholar-version2.paper_rp.author_articles` AS a
      ON a.authorid = p.authorid AND a.start_year = p.start_year
     AND a.pub_year BETWEEN p.start_year + 11 AND p.start_year + 20
    GROUP BY p.pop_age, p.authorid, p.start_year
  ),
  stars AS (
    SELECT pop_age, authorid, start_year,
      ROW_NUMBER() OVER (PARTITION BY pop_age, start_year
                         ORDER BY fut_top10 DESC, IFNULL(fut_mean, 0) DESC, FARM_FINGERPRINT(authorid))
        <= 0.1 * COUNT(*) OVER (PARTITION BY pop_age, start_year) AS star
    FROM future
  )
SELECT m.*, c.top10, c.i3, c.mean_pct, c.t50, f.fut_articles, f.fut_top10, f.fut_any_top1, f.fut_mean, s.star
FROM measured AS m
JOIN pct_at_t AS c USING (authorid, start_year, t)
JOIN future AS f USING (pop_age, authorid, start_year)
JOIN stars AS s USING (pop_age, authorid, start_year);

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.early_detection_results{SUFFIX}`
OPTIONS (description = 'Early-detection accuracy per population, measurement age and predictor. Built by paper/sql/21_early_detection.sql')
AS
WITH
  wide AS (
    SELECT *,
      0.5 * (COUNT(*) OVER c + RANK() OVER (c ORDER BY n_articles) - RANK() OVER (c ORDER BY n_articles DESC))
        / (2 * COUNT(*) OVER c)
      + 0.5 * (COUNT(*) OVER c + RANK() OVER (c ORDER BY mean_pct) - RANK() OVER (c ORDER BY mean_pct DESC))
        / (2 * COUNT(*) OVER c) AS combo50
    FROM `scholar-version2.paper_rp.early_detection_base{SUFFIX}`
    WINDOW c AS (PARTITION BY pop_age, t, start_year)
  ),
  long_raw AS (
    SELECT pop_age, t, start_year, authorid, predictor, x, fut_top10, fut_any_top1, fut_mean, fut_articles, star
    FROM wide,
      UNNEST([STRUCT('pip' AS predictor, sym_tau50 AS x), ('pip_original', pip), ('h', CAST(h_index AS FLOAT64)),
              ('citations', CAST(citations AS FLOAT64)), ('articles', CAST(n_articles AS FLOAT64)),
              ('top10', CAST(top10 AS FLOAT64)), ('i3', i3), ('mean_pct', mean_pct), ('t50', t50),
              ('combo50', combo50)])
  ),
  long_form AS (
    SELECT *,
      (COUNT(*) OVER c + RANK() OVER (c ORDER BY x) - RANK() OVER (c ORDER BY x DESC)) / (2 * COUNT(*) OVER c) AS xp,
      ROW_NUMBER() OVER (c ORDER BY x DESC, FARM_FINGERPRINT(authorid)) <= 0.1 * COUNT(*) OVER c AS picked,
      -- expected inclusion in the top 10% under random tie-breaking: above = authors with a higher value,
      -- tied = authors with the same value (this one included), slots = 0.1 * cohort size
      GREATEST(0, LEAST(1, (0.1 * COUNT(*) OVER c - (RANK() OVER (c ORDER BY x DESC) - 1))
        / (COUNT(*) OVER c + 2 - RANK() OVER (c ORDER BY x) - RANK() OVER (c ORDER BY x DESC)))) AS w_pick
    FROM long_raw
    WINDOW c AS (PARTITION BY pop_age, t, predictor, start_year)
  ),
  ranked AS (
    SELECT pop_age, t, predictor, fut_any_top1, star, picked, w_pick,
      (RANK() OVER (w ORDER BY xp) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY xp DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_top10) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_top10 DESC)) / 2 AS ry
    FROM long_form
    WINDOW w AS (PARTITION BY pop_age, t, predictor)
  ),
  ranked_quality AS (
    SELECT pop_age, t, predictor,
      (RANK() OVER (w ORDER BY xp) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY xp DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_mean) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_mean DESC)) / 2 AS rq
    FROM long_form
    WHERE fut_articles > 0
    WINDOW w AS (PARTITION BY pop_age, t, predictor)
  )
SELECT m.*, q.spearman_mean_pct
FROM (
  SELECT pop_age, t, predictor, COUNT(*) AS n,
    CORR(rx, ry) AS spearman_top10,
    (SUM(IF(fut_any_top1, rx, 0)) - COUNTIF(fut_any_top1) * (COUNTIF(fut_any_top1) + 1) / 2)
      / (COUNTIF(fut_any_top1) * COUNTIF(NOT fut_any_top1)) AS auc_any_top1,
    (SUM(IF(star, rx, 0)) - COUNTIF(star) * (COUNTIF(star) + 1) / 2)
      / (COUNTIF(star) * COUNTIF(NOT star)) AS auc_star,
    COUNTIF(picked AND star) / COUNTIF(picked) AS precision_top10,
    SUM(IF(star, w_pick, 0)) / SUM(w_pick) AS precision_top10_frac,
    COUNTIF(star) / COUNT(*) AS star_share
  FROM ranked GROUP BY pop_age, t, predictor
) AS m
JOIN (SELECT pop_age, t, predictor, CORR(rx, rq) AS spearman_mean_pct FROM ranked_quality
      GROUP BY pop_age, t, predictor) AS q
USING (pop_age, t, predictor);
