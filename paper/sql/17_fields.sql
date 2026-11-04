-- 17: Fields. The main indicator uses paper percentiles within publication year (all fields pooled) and peers within
-- start year (all fields pooled). This step asks how scores differ by field and whether the prediction results hold
-- inside every field.
--   author_field_age10  each active author's main field at career age 10: the most frequent S2 field of study among
--                       their articles published by then (whole counting; ties by name); 'Unknown' if none has a field
--   field_profile       per field, career age 10, start years 1975-2010: score medians, and the share of authors in the
--                       top 10% of their cohort on sym_tau50 (10% would mean no field advantage)
--   field_prediction    per field, the 11_prediction outcomes (career age 10, start years 1975-2000) for sym_tau50,
--                       current PiP and the raw h-index: Spearman with future top-10% count, AUC for any future top-1%,
--                       Spearman with future mean quality
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.author_field_age10`
OPTIONS (description = 'Main S2 field of study per active author at career age 10. Built by paper/sql/17_fields.sql')
AS
WITH
  pop AS (
    SELECT authorid, start_year
    FROM `scholar-version2.paper_rp.author_pip_by_age`
    WHERE benchmark = 'active' AND age = 10 AND start_year BETWEEN 1960 AND 2015
  ),
  af AS (
    SELECT a.authorid, a.start_year, f AS field, COUNT(*) AS n
    FROM `scholar-version2.paper_rp.author_articles` AS a
    JOIN pop USING (authorid, start_year)
    JOIN `scholar-version2.paper_rp.article_percentiles` AS ap ON ap.corpusid = a.corpusid,
      UNNEST(ap.fields) AS f
    WHERE a.start_year BETWEEN 1960 AND 2015 AND a.pub_year <= a.start_year + 10
    GROUP BY a.authorid, a.start_year, f
  ),
  best AS (
    SELECT authorid, start_year,
      ARRAY_AGG(STRUCT(field, n) ORDER BY n DESC, field LIMIT 1)[OFFSET(0)].field AS main_field,
      MAX(n) / SUM(n) AS main_share
    FROM af
    GROUP BY authorid, start_year
  )
SELECT p.authorid, p.start_year, IFNULL(b.main_field, 'Unknown') AS main_field, b.main_share
FROM pop AS p
LEFT JOIN best AS b USING (authorid, start_year);

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.field_profile`
OPTIONS (description = 'Score levels by main field at career age 10. Built by paper/sql/17_fields.sql')
AS
SELECT f.main_field, COUNT(*) AS authors,
  APPROX_QUANTILES(v.sym_tau50, 100)[OFFSET(50)] AS sym_tau50_median,
  APPROX_QUANTILES(v.sym_tau50, 100)[OFFSET(90)] AS sym_tau50_p90,
  APPROX_QUANTILES(v.pip, 100)[OFFSET(50)] AS pip_median,
  APPROX_QUANTILES(v.h_index, 100)[OFFSET(50)] AS h_median,
  APPROX_QUANTILES(v.n_articles, 100)[OFFSET(50)] AS articles_median,
  AVG(IF(p.pct_sym50 >= 0.9, 1, 0)) AS share_top10_sym_tau50
FROM `scholar-version2.paper_rp.author_field_age10` AS f
JOIN `scholar-version2.paper_rp.pip_variants` AS v ON v.authorid = f.authorid AND v.start_year = f.start_year AND v.age = 10
JOIN `scholar-version2.paper_rp.sym_pct_by_age` AS p ON p.authorid = f.authorid AND p.start_year = f.start_year AND p.age = 10
WHERE f.start_year BETWEEN 1975 AND 2010
GROUP BY f.main_field;

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.field_prediction`
OPTIONS (description = 'Prediction benchmarks within each main field (career age 10). Built by paper/sql/17_fields.sql')
AS
WITH
  joined AS (
    SELECT f.main_field, b.fut_top10, b.fut_any_top1, b.fut_mean_pct, b.fut_articles,
      v.sym_tau50, v.pip, CAST(v.h_index AS FLOAT64) AS h
    FROM `scholar-version2.paper_rp.prediction_base` AS b
    JOIN `scholar-version2.paper_rp.pip_variants` AS v
      ON v.authorid = b.authorid AND v.start_year = b.start_year AND v.age = b.age
    JOIN `scholar-version2.paper_rp.author_field_age10` AS f
      ON f.authorid = b.authorid AND f.start_year = b.start_year
    WHERE b.scenario = 'same_age' AND b.setting = 10
  ),
  long_form AS (
    SELECT main_field, predictor, x, fut_top10, fut_any_top1, fut_mean_pct, fut_articles
    FROM joined, UNNEST([STRUCT('sym_tau50' AS predictor, sym_tau50 AS x), ('pip', pip), ('h_raw', h)])
  ),
  ranked AS (
    SELECT main_field, predictor, fut_any_top1, fut_articles,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_top10) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_top10 DESC)) / 2 AS ry
    FROM long_form
    WINDOW w AS (PARTITION BY main_field, predictor)
  ),
  ranked_quality AS (
    SELECT main_field, predictor,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY fut_mean_pct) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY fut_mean_pct DESC)) / 2 AS rq
    FROM long_form
    WHERE fut_articles > 0
    WINDOW w AS (PARTITION BY main_field, predictor)
  )
SELECT m.*, q.spearman_mean_pct
FROM (
  SELECT main_field, predictor, COUNT(*) AS n, CORR(rx, ry) AS spearman_top10,
    SAFE_DIVIDE(SUM(IF(fut_any_top1, rx, 0)) - COUNTIF(fut_any_top1) * (COUNTIF(fut_any_top1) + 1) / 2,
                COUNTIF(fut_any_top1) * COUNTIF(NOT fut_any_top1)) AS auc_any_top1
  FROM ranked GROUP BY main_field, predictor
) AS m
JOIN (
  SELECT main_field, predictor, CORR(rx, rq) AS spearman_mean_pct FROM ranked_quality GROUP BY main_field, predictor
) AS q USING (main_field, predictor);
