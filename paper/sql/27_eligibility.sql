-- 27: Eligibility without the h-index condition. The main analysis compares authors with three articles and an h-index
-- of three (the 'active' rule of 06). The rule conditions on citations, and the h-index is also one of the indicators
-- PiP is compared with, so this step repeats the same-age prediction under 'min3' (three articles, any h-index). The
-- early-detection comparison under 'min3' is 21 run through run_21.sh min3, which reads sym_by_age_min3.
-- Needs pip_exact_min3 (run_25.sh min3: PiP on the production grid, K = 40, tau = 0.5, min3 peers).
--   sym_by_age_min3            the columns of sym_by_age (15) that 21 reads, for min3 peers: start years 1975-2005,
--                              career ages 3-10 and 15
--   author_field_age10_min3    main field at career age 10 as in 17, for min3 authors with start years 1975-2000
--   eligibility_excluded       share of min3 authors who fail the h-index condition, by career age and main field
--   eligibility_prediction     the same-age settings of 11 / 20 (career ages 5, 10, 15; start years 1975 .. 2010 - age;
--                              raw values pooled over cohorts), for both rules: Spearman with the future top-10% count,
--                              ROC AUC for a future top-1% article, Spearman with the mean percentile of future articles.
--                              Under 'active' the numbers equal those of 20, which checks this query.
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.sym_by_age_min3`
PARTITION BY RANGE_BUCKET(start_year, GENERATE_ARRAY(1950, 2026, 1))
CLUSTER BY age, authorid
OPTIONS (description = 'PiP (tau 0.5, 40 classes) and author metrics for min3 peers, ages 3-10 and 15. Built by paper/sql/27_eligibility.sql')
AS
SELECT p.authorid, p.start_year, p.age, p.eval_year, p.n_peers, p.n_articles, p.citations, p.h_index, p.prod_pct,
  p.pip, e.score AS sym_tau50
FROM `scholar-version2.paper_rp.author_pip_by_age` AS p
JOIN `scholar-version2.paper_rp.pip_exact_min3` AS e
  ON e.authorid = p.authorid AND e.start_year = p.start_year AND e.age = p.age
WHERE p.benchmark = 'min3' AND e.k_grid = 40 AND e.tau = 0.5;

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.author_field_age10_min3`
OPTIONS (description = 'Main S2 field at career age 10 for min3 authors. Built by paper/sql/27_eligibility.sql')
AS
WITH
  pop AS (
    SELECT authorid, start_year
    FROM `scholar-version2.paper_rp.author_pip_by_age`
    WHERE benchmark = 'min3' AND age = 10 AND start_year BETWEEN 1975 AND 2000
  ),
  af AS (
    SELECT a.authorid, a.start_year, f AS field, COUNT(*) AS n
    FROM `scholar-version2.paper_rp.author_articles` AS a
    JOIN pop USING (authorid, start_year)
    JOIN `scholar-version2.paper_rp.article_percentiles` AS ap ON ap.corpusid = a.corpusid,
      UNNEST(ap.fields) AS f
    WHERE a.start_year BETWEEN 1975 AND 2000 AND a.pub_year <= a.start_year + 10
    GROUP BY a.authorid, a.start_year, f
  ),
  best AS (
    SELECT authorid, start_year,
      ARRAY_AGG(STRUCT(field, n) ORDER BY n DESC, field LIMIT 1)[OFFSET(0)].field AS main_field
    FROM af
    GROUP BY authorid, start_year
  )
SELECT p.authorid, p.start_year, IFNULL(b.main_field, 'Unknown') AS main_field
FROM pop AS p
LEFT JOIN best AS b USING (authorid, start_year);

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.eligibility_excluded`
OPTIONS (description = 'Share of min3 authors without an h-index of three, by career age and field. Built by paper/sql/27_eligibility.sql')
AS
SELECT m.age, IFNULL(f.main_field, '(all fields)') AS main_field, COUNT(*) AS authors_min3,
  COUNTIF(m.h_index >= 3) AS authors_active, 1 - COUNTIF(m.h_index >= 3) / COUNT(*) AS share_excluded
FROM `scholar-version2.paper_rp.sym_by_age_min3` AS m
JOIN `scholar-version2.paper_rp.author_field_age10_min3` AS f USING (authorid, start_year)
WHERE m.age IN (3, 5, 10) AND m.start_year BETWEEN 1975 AND 2000
GROUP BY ROLLUP (m.age, f.main_field)
HAVING m.age IS NOT NULL;

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.eligibility_prediction`
OPTIONS (description = 'Same-age prediction under the active and min3 rules. Built by paper/sql/27_eligibility.sql')
AS
WITH
  scores AS (
    SELECT 'active' AS rule, authorid, start_year, age, eval_year, sym_tau50, pip, h_index, citations, n_articles
    FROM `scholar-version2.paper_rp.sym_by_age`
    WHERE age IN (5, 10, 15) AND start_year BETWEEN 1975 AND 2010 - age
    UNION ALL
    SELECT 'min3', authorid, start_year, age, eval_year, sym_tau50, pip, h_index, citations, n_articles
    FROM `scholar-version2.paper_rp.sym_by_age_min3`
    WHERE age IN (5, 10, 15) AND start_year BETWEEN 1975 AND 2010 - age
  ),
  author_years AS (SELECT DISTINCT authorid, start_year, eval_year FROM scores),
  agg AS (
    SELECT x.authorid, x.start_year, x.eval_year,
      COUNTIF(a.below[OFFSET(x.eval_year - a.pub_year)] >= 0.9) AS top10,
      SUM(a.below[OFFSET(x.eval_year - a.pub_year)]) AS i3,
      AVG(a.below[OFFSET(x.eval_year - a.pub_year)]) AS mean_pct,
      SUM(GREATEST(0, (FLOOR(a.below[OFFSET(x.eval_year - a.pub_year)] * 40) + 1) / 40 - 0.5)) / 0.5 AS t50
    FROM author_years AS x
    JOIN `scholar-version2.paper_rp.author_articles` AS a
      ON a.authorid = x.authorid AND a.start_year = x.start_year
    WHERE a.pub_year <= x.eval_year
    GROUP BY x.authorid, x.start_year, x.eval_year
  ),
  outcomes AS (
    SELECT x.authorid, x.eval_year,
      COUNT(a.pub_year) AS fut_articles,
      COUNTIF(a.below[OFFSET(5)] >= 0.9) AS fut_top10,
      COUNTIF(a.below[OFFSET(5)] >= 0.99) > 0 AS fut_any_top1,
      AVG(a.hazen[OFFSET(5)]) AS fut_mean_pct
    FROM author_years AS x
    LEFT JOIN `scholar-version2.paper_rp.author_articles` AS a
      ON a.authorid = x.authorid AND a.start_year = x.start_year
     AND a.pub_year > x.eval_year AND a.pub_year <= x.eval_year + 10
    GROUP BY x.authorid, x.eval_year
  ),
  base AS (
    SELECT s.*, g.top10, g.i3, g.mean_pct, g.t50, o.fut_articles, o.fut_top10, o.fut_any_top1, o.fut_mean_pct,
      0.5 * (COUNT(*) OVER c + RANK() OVER (c ORDER BY s.n_articles) - RANK() OVER (c ORDER BY s.n_articles DESC))
        / (2 * COUNT(*) OVER c)
      + 0.5 * (COUNT(*) OVER c + RANK() OVER (c ORDER BY g.mean_pct) - RANK() OVER (c ORDER BY g.mean_pct DESC))
        / (2 * COUNT(*) OVER c) AS combo50
    FROM scores AS s
    JOIN agg AS g USING (authorid, start_year, eval_year)
    JOIN outcomes AS o USING (authorid, eval_year)
    WINDOW c AS (PARTITION BY s.rule, s.age, s.start_year)
  ),
  long_form AS (
    SELECT rule, age, predictor, x, fut_top10, fut_any_top1, fut_mean_pct, fut_articles
    FROM base,
      UNNEST([STRUCT('pip' AS predictor, sym_tau50 AS x), ('pip_original', pip), ('h', CAST(h_index AS FLOAT64)),
              ('citations', CAST(citations AS FLOAT64)), ('articles', CAST(n_articles AS FLOAT64)),
              ('top10', CAST(top10 AS FLOAT64)), ('i3', i3), ('mean_pct', mean_pct), ('t50', t50),
              ('combo50', combo50)])
  ),
  ranked AS (
    SELECT rule, age, predictor, fut_any_top1,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_top10) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_top10 DESC)) / 2 AS ry
    FROM long_form
    WINDOW w AS (PARTITION BY rule, age, predictor)
  ),
  ranked_quality AS (
    SELECT rule, age, predictor,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_mean_pct) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_mean_pct DESC)) / 2 AS rq
    FROM long_form
    WHERE fut_articles > 0
    WINDOW w AS (PARTITION BY rule, age, predictor)
  )
SELECT m.*, q.spearman_mean_pct
FROM (
  SELECT rule, age, predictor, COUNT(*) AS n, CORR(rx, ry) AS spearman_top10,
    (SUM(IF(fut_any_top1, rx, 0)) - COUNTIF(fut_any_top1) * (COUNTIF(fut_any_top1) + 1) / 2)
      / (COUNTIF(fut_any_top1) * COUNTIF(NOT fut_any_top1)) AS auc_any_top1
  FROM ranked GROUP BY rule, age, predictor
) AS m
JOIN (SELECT rule, age, predictor, CORR(rx, rq) AS spearman_mean_pct FROM ranked_quality
      GROUP BY rule, age, predictor) AS q
USING (rule, age, predictor);
