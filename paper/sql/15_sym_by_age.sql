-- 15: Symmetric PiP at every career age 1-30 for 'active' peers (the paper's main indicator: sym_tau50).
--
-- Run through paper/sql/run_15.sh ({LO}/{HI} = range of start years; {MODE_PREFIX} = CREATE or INSERT).
-- Same construction as 12 (40-point grid t_j = j/40, n_j = articles with percentile y >= t_j at the evaluation year,
-- G_j(m) = share of peers with strictly fewer than m articles above t_j), for every (start_year, age) peer group:
--   sym, sym_tau25, sym_tau50   averages of G_j(n_j) over t_j >= 0, 0.25, 0.5
--   prod_pct                    F(n) = share of peers with at most n articles (same as 06)
--   quality_mult_tau50          sym_tau50 / prod_pct: the quality multiplier in sym_tau50 = F(n) x Q (can exceed 1)
--   g0                          G_0(n): share of peers with strictly fewer articles (the t = 0 point of the curve)
{MODE_PREFIX}
WITH
  targets AS (
    SELECT authorid, start_year, age, n_articles, n_peers, prod_pct, pip, h_index, citations
    FROM `scholar-version2.paper_rp.author_pip_by_age`
    WHERE benchmark = 'active' AND start_year BETWEEN {LO} AND {HI}
  ),
  papers AS (
    SELECT t.authorid, t.start_year, t.age, a.below[OFFSET(t.start_year + t.age - a.pub_year)] AS y
    FROM targets AS t
    JOIN `scholar-version2.paper_rp.author_articles` AS a
      ON a.authorid = t.authorid AND a.start_year = t.start_year
    WHERE a.start_year BETWEEN {LO} AND {HI} AND a.pub_year <= t.start_year + t.age
  ),
  buckets AS (
    SELECT authorid, start_year, age, LEAST(39, CAST(FLOOR(y * 40) AS INT64)) AS b, COUNT(*) AS cnt
    FROM papers
    GROUP BY authorid, start_year, age, b
  ),
  nj AS (
    SELECT g.authorid, g.start_year, g.age, g.j,
      SUM(IFNULL(b.cnt, 0)) OVER (PARTITION BY g.authorid, g.start_year, g.age ORDER BY g.j DESC
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_j
    FROM (SELECT authorid, start_year, age, j FROM targets, UNNEST(GENERATE_ARRAY(0, 39)) AS j) AS g
    LEFT JOIN buckets AS b
      ON b.authorid = g.authorid AND b.start_year = g.start_year AND b.age = g.age AND b.b = g.j
  ),
  dist_j AS (
    SELECT start_year, age, j, m,
      (SUM(cnt) OVER (PARTITION BY start_year, age, j ORDER BY m) - cnt)
        / SUM(cnt) OVER (PARTITION BY start_year, age, j) AS g_below
    FROM (SELECT start_year, age, j, n_j AS m, COUNT(*) AS cnt FROM nj GROUP BY start_year, age, j, n_j)
  ),
  scores AS (
    SELECT n.authorid, n.start_year, n.age,
      AVG(g.g_below) AS sym,
      SUM(IF(n.j >= 10, g.g_below, 0)) / 30 AS sym_tau25,
      SUM(IF(n.j >= 20, g.g_below, 0)) / 20 AS sym_tau50,
      MAX(IF(n.j = 0, g.g_below, NULL)) AS g0,
      MAX(IF(n.j = 39, n.n_j, NULL)) AS n_top
    FROM nj AS n
    JOIN dist_j AS g ON g.start_year = n.start_year AND g.age = n.age AND g.j = n.j AND g.m = n.n_j
    GROUP BY n.authorid, n.start_year, n.age
  )
SELECT
  t.authorid, t.start_year, t.age, t.start_year + t.age AS eval_year, t.n_peers, t.n_articles, t.citations, t.h_index,
  t.prod_pct, t.pip, s.sym, s.sym_tau25, s.sym_tau50, s.g0, s.n_top,
  SAFE_DIVIDE(s.sym_tau50, t.prod_pct) AS quality_mult_tau50
FROM targets AS t
JOIN scores AS s USING (authorid, start_year, age);
