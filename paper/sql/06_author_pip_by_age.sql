-- 06: Author metrics and step-form PiP-AUC at every career age, with peers defined as of the evaluation year.
--
-- Run through paper/sql/run_06.sh, which substitutes {LO} and {HI} (a range of career-start years), {BENCHMARK}
-- (the peer rule's name) and {MIN_H}, and either creates the table or appends to it.
--
-- For author i with career start s (first article, from 05) and career age a = 1..min(30, 2025 - s), evaluated at
-- year Y = s + a, using only articles published by Y and their citations through Y:
--   n_articles  articles published by Y; n_cited those with at least one citation by Y; citations their total
--   h_index     h-index from citations through Y
--   peers       authors with the same start year, at least 3 articles by Y and h-index >= {MIN_H} at Y (no condition
--               on anything after Y). Benchmarks: 'active' (MIN_H = 3, the as-of-year version of the app's
--               active_authors) and 'min3' (MIN_H = 0, any author with 3 articles).
--   prod_pct    F(n): share of peers with at most n_articles articles (the productivity percentile)
--   pip         step-form PiP-AUC: sum over the author's articles in descending percentile order (rank k) of
--               y_k * p(k), where y = `below` from 04 (share of same-year articles with fewer citations at Y) and
--               p(k) = share of peers with exactly k articles. pip = prod_pct * (weighted mean of y), and
--               y_min * prod_pct <= pip <= y_max * prod_pct.
--   pip_hazen   the same with Hazen percentiles
--   mean_below, mean_hazen  plain means of the paper percentiles; sum_hazen  their sum (applied to current
--               percentiles)
-- Only peers get a row.
{MODE_PREFIX}
WITH
  aa AS (
    SELECT authorid, start_year, corpusid, pub_year, cites, below, hazen
    FROM `scholar-version2.paper_rp.author_articles`
    WHERE start_year BETWEEN {LO} AND {HI}
  ),
  grid AS (
    SELECT
      aa.authorid,
      aa.start_year,
      a AS age,
      aa.corpusid,
      aa.cites[OFFSET(aa.start_year + a - aa.pub_year)] AS c,
      aa.below[OFFSET(aa.start_year + a - aa.pub_year)] AS y,
      aa.hazen[OFFSET(aa.start_year + a - aa.pub_year)] AS yh
    FROM aa, UNNEST(GENERATE_ARRAY(1, LEAST(30, 2025 - aa.start_year))) AS a
    WHERE aa.pub_year <= aa.start_year + a
  ),
  counts AS (
    SELECT authorid, start_year, age, COUNT(*) AS n_articles, COUNTIF(c > 0) AS n_cited, SUM(c) AS citations,
      COUNTIF(c >= k_c) AS h_index
    FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY authorid, age ORDER BY c DESC, corpusid) AS k_c
      FROM grid
    )
    GROUP BY authorid, start_year, age
  ),
  peers AS (
    SELECT * FROM counts WHERE n_articles >= 3 AND h_index >= {MIN_H}
  ),
  count_dist AS (
    SELECT
      start_year, age, k, n_exact,
      SUM(n_exact) OVER (PARTITION BY start_year, age) AS n_peers,
      SUM(n_exact) OVER (PARTITION BY start_year, age ORDER BY k) AS n_at_most
    FROM (
      SELECT start_year, age, n_articles AS k, COUNT(*) AS n_exact
      FROM peers
      GROUP BY start_year, age, n_articles
    )
  ),
  ranked AS (
    SELECT
      g.authorid, g.start_year, g.age, g.c, g.y, g.yh,
      ROW_NUMBER() OVER (PARTITION BY g.authorid, g.age ORDER BY g.y DESC, g.c DESC, g.corpusid) AS k_y,
      ROW_NUMBER() OVER (PARTITION BY g.authorid, g.age ORDER BY g.yh DESC, g.c DESC, g.corpusid) AS k_h
    FROM grid AS g
    JOIN peers USING (authorid, start_year, age)
  ),
  scored AS (
    SELECT
      r.authorid, r.start_year, r.age,
      SUM(r.y * IFNULL(p1.n_exact / p1.n_peers, 0)) AS pip,
      SUM(r.yh * IFNULL(p2.n_exact / p2.n_peers, 0)) AS pip_hazen,
      AVG(r.y) AS mean_below,
      AVG(r.yh) AS mean_hazen,
      SUM(r.yh) AS sum_hazen,
      MAX(r.y) AS max_below,
      MIN(r.y) AS min_below
    FROM ranked AS r
    LEFT JOIN count_dist AS p1 ON p1.start_year = r.start_year AND p1.age = r.age AND p1.k = r.k_y
    LEFT JOIN count_dist AS p2 ON p2.start_year = r.start_year AND p2.age = r.age AND p2.k = r.k_h
    GROUP BY r.authorid, r.start_year, r.age
  )
SELECT
  '{BENCHMARK}' AS benchmark,
  s.authorid,
  s.start_year,
  s.age,
  s.start_year + s.age AS eval_year,
  c.n_articles,
  c.n_cited,
  c.citations,
  c.h_index,
  f.n_peers,
  f.n_at_most / f.n_peers AS prod_pct,
  s.pip,
  s.pip_hazen,
  s.mean_below,
  s.mean_hazen,
  s.sum_hazen,
  s.max_below,
  s.min_below
FROM scored AS s
JOIN counts AS c USING (authorid, start_year, age)
JOIN count_dist AS f ON f.start_year = s.start_year AND f.age = s.age AND f.k = c.n_articles;
