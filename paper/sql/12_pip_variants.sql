-- 12: PiP variants, all in threshold form on a 40-point grid t_j = j/40 (j = 0..39), for 'active' peers.
--
-- For author i at (start_year, age), n_j = number of i's articles whose percentile y (`below`, at the evaluation
-- year) is at least t_j; n_0 = all articles.
--   pip_grid   (1/40) sum_j F(n_j), F(m) = share of peers with at most m articles in total. This is the current
--              step-form PiP on a grid (sanity check against `pip`; the left-edge sum slightly overestimates).
--   pip_tau25  the same restricted to t >= 0.25 and rescaled: (1/(1-tau)) * integral from tau to 1. Papers below
--              the 25th percentile earn nothing, and the score is still monotone.
--   pip_tau50  tau = 0.5
--   sym        (1/40) sum_j G_j(n_j), G_j(m) = share of peers with strictly fewer than m articles above t_j: your
--              count above each threshold compared with peers' counts above the same threshold (symmetric).
--   sym_tau50  symmetric, restricted to t >= 0.5
--   n_top      articles with y >= 0.975 (top 2.5% of their year), to see which variants reward top papers
-- Peers: all 'active' authors at the same (start_year, age), so every targeted peer group is complete.
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.pip_variants`
OPTIONS (description = 'PiP variants (current on a grid, tau-cut, symmetric) for active authors. Built by paper/sql/12_pip_variants.sql')
AS
WITH
  targets AS (
    SELECT authorid, start_year, age, n_articles, n_peers, pip, mean_below, h_index
    FROM `scholar-version2.paper_rp.author_pip_by_age`
    WHERE benchmark = 'active'
      AND ((age IN (5, 10, 15) AND start_year BETWEEN 1960 AND 2025 - age)
           OR (start_year + age IN (2000, 2005) AND age BETWEEN 2 AND 30))
  ),
  papers AS (
    SELECT t.authorid, t.start_year, t.age, a.below[OFFSET(t.start_year + t.age - a.pub_year)] AS y
    FROM targets AS t
    JOIN `scholar-version2.paper_rp.author_articles` AS a
      ON a.authorid = t.authorid AND a.start_year = t.start_year
    WHERE a.start_year BETWEEN 1960 AND 2020 AND a.pub_year <= t.start_year + t.age
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
  -- distribution of n_j among peers, per threshold: G_j(m) = (peers with n_j <= m - peers with n_j = m) / peers
  dist_j AS (
    SELECT start_year, age, j, m, cnt,
      (SUM(cnt) OVER (PARTITION BY start_year, age, j ORDER BY m) - cnt)
        / SUM(cnt) OVER (PARTITION BY start_year, age, j) AS g_below
    FROM (SELECT start_year, age, j, n_j AS m, COUNT(*) AS cnt FROM nj GROUP BY start_year, age, j, n_j)
  ),
  -- F(m) = share of peers with at most m articles in total, for every integer m up to the group maximum
  totals AS (SELECT start_year, age, m, cnt FROM dist_j WHERE j = 0),
  dense AS (
    SELECT d.start_year, d.age, m
    FROM (SELECT start_year, age, MAX(m) AS mx FROM totals GROUP BY start_year, age) AS d,
      UNNEST(GENERATE_ARRAY(0, d.mx)) AS m
  ),
  f_dense AS (
    SELECT d.start_year, d.age, d.m,
      SUM(IFNULL(t.cnt, 0)) OVER (PARTITION BY d.start_year, d.age ORDER BY d.m)
        / SUM(IFNULL(t.cnt, 0)) OVER (PARTITION BY d.start_year, d.age) AS f
    FROM dense AS d
    LEFT JOIN totals AS t ON t.start_year = d.start_year AND t.age = d.age AND t.m = d.m
  ),
  scores AS (
    SELECT n.authorid, n.start_year, n.age,
      AVG(f.f) AS pip_grid,
      SUM(IF(n.j >= 10, f.f, 0)) / 30 AS pip_tau25,
      SUM(IF(n.j >= 20, f.f, 0)) / 20 AS pip_tau50,
      AVG(g.g_below) AS sym,
      SUM(IF(n.j >= 20, g.g_below, 0)) / 20 AS sym_tau50,
      MAX(IF(n.j = 39, n.n_j, NULL)) AS n_top
    FROM nj AS n
    JOIN f_dense AS f ON f.start_year = n.start_year AND f.age = n.age AND f.m = n.n_j
    JOIN dist_j AS g ON g.start_year = n.start_year AND g.age = n.age AND g.j = n.j AND g.m = n.n_j
    GROUP BY n.authorid, n.start_year, n.age
  )
SELECT t.*, s.pip_grid, s.pip_tau25, s.pip_tau50, s.sym, s.sym_tau50, s.n_top
FROM targets AS t
JOIN scores AS s USING (authorid, start_year, age);
