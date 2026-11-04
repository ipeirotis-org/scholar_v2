#!/usr/bin/env bash
# Exports the data behind the paper's figures from BigQuery (dataset paper_rp) to paper/figures/data/*.csv.
# Run from anywhere with bq authenticated: paper/figures/export_figure_data.sh
set -euo pipefail
cd "$(dirname "$0")/data"
q() { bq query --use_legacy_sql=false --format=csv --max_rows=100000 --quiet "$1" | grep -v -E "WARNING|Waiting"; }

# Fig. 1: paper percentile stability (pooled fields, cohorts 1980-1995) and by field (r between ages 3 and 10)
q 'SELECT t1, t2, SUM(n) AS papers, AVG(pearson) AS r, AVG(slope) AS slope
   FROM `scholar-version2.paper_rp.paper_percentile_stability`
   WHERE field = "*" AND pub_year BETWEEN 1980 AND 1995 GROUP BY t1, t2 ORDER BY t1, t2' > fig1_pooled.csv
q 'SELECT field, SUM(IF(t1 = 3 AND t2 = 10, n, 0)) AS papers,
     AVG(IF(t1 = 3 AND t2 = 10, pearson, NULL)) AS r_3_10, AVG(IF(t1 = 5 AND t2 = 30, pearson, NULL)) AS r_5_30
   FROM `scholar-version2.paper_rp.paper_percentile_stability`
   WHERE pub_year BETWEEN 1985 AND 1995 AND field != "*" GROUP BY field HAVING papers > 20000 ORDER BY r_3_10 DESC' > fig1_fields.csv

# Fig. 2: score distributions at career age 10 by start year: sym_tau50 and raw h-index (exact quantiles)
q 'SELECT start_year, COUNT(*) AS authors, AVG(sym_tau50) AS sym_mean,
     ANY_VALUE(s25) AS sym_p25, ANY_VALUE(s50) AS sym_p50, ANY_VALUE(s75) AS sym_p75, ANY_VALUE(s90) AS sym_p90,
     ANY_VALUE(h25) AS h_p25, ANY_VALUE(h50) AS h_p50, ANY_VALUE(h75) AS h_p75, ANY_VALUE(h90) AS h_p90, AVG(h_index) AS h_mean
   FROM (
     SELECT start_year, sym_tau50, h_index,
       PERCENTILE_CONT(sym_tau50, 0.25) OVER w AS s25, PERCENTILE_CONT(sym_tau50, 0.5) OVER w AS s50,
       PERCENTILE_CONT(sym_tau50, 0.75) OVER w AS s75, PERCENTILE_CONT(sym_tau50, 0.9) OVER w AS s90,
       PERCENTILE_CONT(h_index, 0.25) OVER w AS h25, PERCENTILE_CONT(h_index, 0.5) OVER w AS h50,
       PERCENTILE_CONT(h_index, 0.75) OVER w AS h75, PERCENTILE_CONT(h_index, 0.9) OVER w AS h90
     FROM `scholar-version2.paper_rp.sym_by_age` WHERE age = 10 AND start_year BETWEEN 1960 AND 2015
     WINDOW w AS (PARTITION BY start_year))
   GROUP BY start_year ORDER BY start_year' > fig2_cohorts.csv

# Fig. 3: standing curves g(theta) for three real authors at career age 10 (start year 1995) chosen at the 25th, 50th
# and 90th percentile of sym_tau50, plus their scores. Curves are recomputed here on the 40-point grid.
q 'WITH
  cohort AS (SELECT authorid, sym_tau50, n_articles, prod_pct FROM `scholar-version2.paper_rp.sym_by_age`
             WHERE start_year = 1995 AND age = 10),
  qs AS (SELECT q, qa[OFFSET(q)] AS qv
         FROM (SELECT APPROX_QUANTILES(sym_tau50, 100) AS qa FROM cohort), UNNEST([25, 50, 90]) AS q),
  picks AS (
    SELECT q, authorid FROM (
      SELECT qs.q, c.authorid, ROW_NUMBER() OVER (PARTITION BY qs.q ORDER BY ABS(c.sym_tau50 - qs.qv), c.authorid) AS rn
      FROM cohort AS c CROSS JOIN qs)
    WHERE rn = 1),
  papers AS (
    SELECT a.authorid, a.below[OFFSET(1995 + 10 - a.pub_year)] AS y
    FROM `scholar-version2.paper_rp.author_articles` a JOIN cohort USING (authorid)
    WHERE a.start_year = 1995 AND a.pub_year <= 2005),
  nj AS (SELECT p.authorid, j, COUNTIF(p.y >= j / 40) AS n_j FROM papers p, UNNEST(GENERATE_ARRAY(0, 39)) j GROUP BY p.authorid, j),
  dist AS (SELECT j, n_j AS m, COUNT(*) AS cnt FROM nj GROUP BY j, n_j),
  g AS (SELECT j, m, (SUM(cnt) OVER (PARTITION BY j ORDER BY m) - cnt) / SUM(cnt) OVER (PARTITION BY j) AS g_below FROM dist)
  SELECT p.q AS sym_tau50_quantile, nj.j, nj.j / 40 AS theta, nj.n_j, g.g_below, c.sym_tau50, c.n_articles, c.prod_pct
  FROM picks p JOIN nj USING (authorid) JOIN g ON g.j = nj.j AND g.m = nj.n_j JOIN cohort c USING (authorid)
  ORDER BY p.q, nj.j' > fig3_curves.csv

# Fig. 5: prediction benchmarks (variants incl. raw h) and the h-index percentile from 11
q 'SELECT scenario, setting, variant AS predictor, spearman_top10, auc_any_top1, spearman_mean_pct
   FROM `scholar-version2.paper_rp.variant_prediction`
   WHERE variant IN ("sym_tau50", "pip", "h_raw")
   UNION ALL
   SELECT scenario, setting, predictor, spearman_top10, auc_any_top1, spearman_mean_pct
   FROM `scholar-version2.paper_rp.prediction_results` WHERE predictor = "h_pct"
   ORDER BY scenario, setting, predictor' > fig5_prediction.csv

# Fig. 6: share of each field in its cohort top 10%, pooled vs field-normalized score (career age 10)
q 'WITH r AS (SELECT main_field,
     (COUNT(*) OVER w + RANK() OVER (w ORDER BY sym_tau50) - RANK() OVER (w ORDER BY sym_tau50 DESC)) / (2 * COUNT(*) OVER w) AS p_pooled,
     (COUNT(*) OVER w + RANK() OVER (w ORDER BY fn_sym50) - RANK() OVER (w ORDER BY fn_sym50 DESC)) / (2 * COUNT(*) OVER w) AS p_fn
     FROM `scholar-version2.paper_rp.field_normalized_scores` WINDOW w AS (PARTITION BY start_year))
   SELECT main_field, COUNT(*) AS authors, AVG(IF(p_pooled >= 0.9, 1, 0)) AS top10_pooled, AVG(IF(p_fn >= 0.9, 1, 0)) AS top10_fn
   FROM r GROUP BY main_field HAVING authors >= 5000 ORDER BY top10_pooled DESC' > fig6_fields.csv
ls -la
