-- 18: Field-normalized variant of the main indicator, at career age 10.
--
-- fn_sym50: symmetric PiP with tau = 0.5 where (1) a paper's percentile is computed within (the author's main field,
-- publication year) when the paper carries that field, otherwise the pooled percentile, and (2) peers are active
-- authors with the same main field and start year. Main field from 17. Authors with no field are left out.
-- Outcomes (start years 1975-2000; articles published at career ages 11-20, scored at their own age 5) are computed
-- both pooled (from 11) and field-normalized (same percentile rule as the score), so neither variant is favored.
--   field_normalized_scores      one row per author: pooled and field-normalized scores and outcomes
--   field_normalized_prediction  Spearman / AUC of each predictor for each outcome, all fields together
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.field_normalized_scores`
OPTIONS (description = 'Field-normalized symmetric PiP at career age 10, with pooled and field-normalized outcomes. Built by paper/sql/18_field_normalized.sql')
AS
WITH
  pop AS (
    SELECT p.authorid, p.start_year, f.main_field
    FROM `scholar-version2.paper_rp.author_pip_by_age` AS p
    JOIN `scholar-version2.paper_rp.author_field_age10` AS f USING (authorid, start_year)
    WHERE p.benchmark = 'active' AND p.age = 10 AND p.start_year BETWEEN 1975 AND 2010 AND f.main_field != 'Unknown'
  ),
  papers AS (
    SELECT pop.authorid, pop.start_year, pop.main_field, a.pub_year,
      pop.start_year + 10 - a.pub_year AS age_at_eval,
      a.cites[OFFSET(pop.start_year + 10 - a.pub_year)] AS c,
      a.below[OFFSET(pop.start_year + 10 - a.pub_year)] AS y_pooled,
      pop.main_field IN UNNEST(ap.fields) AS has_main
    FROM pop
    JOIN `scholar-version2.paper_rp.author_articles` AS a USING (authorid, start_year)
    JOIN `scholar-version2.paper_rp.article_percentiles` AS ap ON ap.corpusid = a.corpusid
    WHERE a.start_year BETWEEN 1975 AND 2010 AND a.pub_year <= pop.start_year + 10
  ),
  papers_fn AS (
    SELECT p.authorid, p.start_year, p.main_field, IF(p.has_main, l.n_less / l.n_total, p.y_pooled) AS y
    FROM papers AS p
    LEFT JOIN `scholar-version2.paper_rp.citation_percentile_lookup` AS l
      ON p.has_main AND l.ref_set = 'article' AND l.field = p.main_field AND l.pub_year = p.pub_year
     AND l.age = p.age_at_eval AND l.citations = p.c
  ),
  buckets AS (
    SELECT authorid, start_year, main_field, LEAST(39, CAST(FLOOR(y * 40) AS INT64)) AS b, COUNT(*) AS cnt
    FROM papers_fn
    GROUP BY authorid, start_year, main_field, b
  ),
  nj AS (
    SELECT g.authorid, g.start_year, g.main_field, g.j,
      SUM(IFNULL(b.cnt, 0)) OVER (PARTITION BY g.authorid, g.start_year ORDER BY g.j DESC
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_j
    FROM (SELECT authorid, start_year, main_field, j FROM pop, UNNEST(GENERATE_ARRAY(0, 39)) AS j) AS g
    LEFT JOIN buckets AS b ON b.authorid = g.authorid AND b.start_year = g.start_year AND b.b = g.j
  ),
  dist_j AS (
    SELECT main_field, start_year, j, m,
      (SUM(cnt) OVER (PARTITION BY main_field, start_year, j ORDER BY m) - cnt)
        / SUM(cnt) OVER (PARTITION BY main_field, start_year, j) AS g_below
    FROM (SELECT main_field, start_year, j, n_j AS m, COUNT(*) AS cnt FROM nj GROUP BY main_field, start_year, j, n_j)
  ),
  scores AS (
    SELECT n.authorid, n.start_year, SUM(IF(n.j >= 20, g.g_below, 0)) / 20 AS fn_sym50
    FROM nj AS n
    JOIN dist_j AS g ON g.main_field = n.main_field AND g.start_year = n.start_year AND g.j = n.j AND g.m = n.n_j
    GROUP BY n.authorid, n.start_year
  ),
  future AS (
    SELECT pop.authorid, pop.start_year, pop.main_field, a.pub_year, a.cites[OFFSET(5)] AS c5,
      a.below[OFFSET(5)] AS y5_pooled, pop.main_field IN UNNEST(ap.fields) AS has_main
    FROM pop
    JOIN `scholar-version2.paper_rp.author_articles` AS a USING (authorid, start_year)
    JOIN `scholar-version2.paper_rp.article_percentiles` AS ap ON ap.corpusid = a.corpusid
    WHERE pop.start_year <= 2000 AND a.start_year BETWEEN 1975 AND 2000
      AND a.pub_year BETWEEN pop.start_year + 11 AND pop.start_year + 20
  ),
  future_out AS (
    SELECT f.authorid, f.start_year,
      COUNT(*) AS fn_fut_articles,
      COUNTIF(IF(f.has_main, l.n_less / l.n_total, f.y5_pooled) >= 0.9) AS fn_fut_top10,
      COUNTIF(IF(f.has_main, l.n_less / l.n_total, f.y5_pooled) >= 0.99) > 0 AS fn_any_top1,
      AVG(IF(f.has_main, l.n_less / l.n_total, f.y5_pooled)) AS fn_fut_mean
    FROM future AS f
    LEFT JOIN `scholar-version2.paper_rp.citation_percentile_lookup` AS l
      ON f.has_main AND l.ref_set = 'article' AND l.field = f.main_field AND l.pub_year = f.pub_year
     AND l.age = 5 AND l.citations = f.c5
    GROUP BY f.authorid, f.start_year
  )
SELECT
  pop.authorid, pop.start_year, pop.main_field, s.fn_sym50, v.sym_tau50, v.pip, v.h_index,
  b.fut_top10, b.fut_any_top1, b.fut_mean_pct, b.fut_articles,
  IFNULL(o.fn_fut_top10, 0) AS fn_fut_top10, IFNULL(o.fn_any_top1, FALSE) AS fn_any_top1, o.fn_fut_mean,
  IFNULL(o.fn_fut_articles, 0) AS fn_fut_articles
FROM pop
JOIN scores AS s USING (authorid, start_year)
JOIN `scholar-version2.paper_rp.pip_variants` AS v
  ON v.authorid = pop.authorid AND v.start_year = pop.start_year AND v.age = 10
LEFT JOIN `scholar-version2.paper_rp.prediction_base` AS b
  ON b.authorid = pop.authorid AND b.start_year = pop.start_year AND b.scenario = 'same_age' AND b.setting = 10
LEFT JOIN future_out AS o ON o.authorid = pop.authorid AND o.start_year = pop.start_year;

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.field_normalized_prediction`
OPTIONS (description = 'Pooled vs field-normalized symmetric PiP vs h-index on pooled and field-normalized outcomes. Built by paper/sql/18_field_normalized.sql')
AS
WITH
  base AS (SELECT * FROM `scholar-version2.paper_rp.field_normalized_scores` WHERE start_year <= 2000),
  long_form AS (
    SELECT predictor, x, outcome, y, binary_y, quality
    FROM base,
      UNNEST([STRUCT('sym_tau50 pooled' AS predictor, sym_tau50 AS x), ('sym_tau50 field-normalized', fn_sym50),
              ('h-index', CAST(h_index AS FLOAT64))]),
      UNNEST([STRUCT('pooled' AS outcome, CAST(fut_top10 AS FLOAT64) AS y, fut_any_top1 AS binary_y,
                     IF(fut_articles > 0, fut_mean_pct, NULL) AS quality),
              ('field-normalized', CAST(fn_fut_top10 AS FLOAT64), fn_any_top1,
                     IF(fn_fut_articles > 0, fn_fut_mean, NULL))])
  ),
  ranked AS (
    SELECT predictor, outcome, binary_y, quality,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY y) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY y DESC)) / 2 AS ry
    FROM long_form
    WINDOW w AS (PARTITION BY predictor, outcome)
  ),
  ranked_quality AS (
    SELECT predictor, outcome,
      (RANK() OVER (w ORDER BY x) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY x DESC)) / 2 AS rx,
      (RANK() OVER (w ORDER BY quality) + COUNT(*) OVER w + 1 - RANK() OVER (w ORDER BY quality DESC)) / 2 AS rq
    FROM long_form
    WHERE quality IS NOT NULL
    WINDOW w AS (PARTITION BY predictor, outcome)
  )
SELECT m.*, q.spearman_quality
FROM (
  SELECT predictor, outcome, COUNT(*) AS n, CORR(rx, ry) AS spearman_top10,
    (SUM(IF(binary_y, rx, 0)) - COUNTIF(binary_y) * (COUNTIF(binary_y) + 1) / 2)
      / (COUNTIF(binary_y) * COUNTIF(NOT binary_y)) AS auc_any_top1
  FROM ranked GROUP BY predictor, outcome
) AS m
JOIN (SELECT predictor, outcome, CORR(rx, rq) AS spearman_quality FROM ranked_quality GROUP BY predictor, outcome) AS q
USING (predictor, outcome);
