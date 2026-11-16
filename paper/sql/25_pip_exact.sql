-- 25: PiP from order statistics, on any grid or none. Run through paper/sql/run_25.sh.
--
-- PiP_tau(a) = 1/(1-tau) * integral from tau to 1 of g_t(n_a(t)) dt, where n_a(t) is the number of the author's articles
-- with percentile y >= t (`below` at the evaluation year) and the standing g_t(m) is the share of peers with fewer than
-- m such articles plus half the share with exactly m (a tie counts one half). With L_t(m) = share of peers with fewer
-- than m articles at or above t, g_t(m) = (L_t(m) + L_t(m + 1)) / 2, and L_t(0) = 0.
-- Sort the author's percentiles y_(1) >= y_(2) >= ..., and set y_(0) = 1 and y_(n+1) = 0. On (y_(k+1), y_(k)] the author
-- has exactly k articles at or above t (k = 0 above the author's best article), and a peer b has fewer than k exactly
-- when its k-th best percentile Y_k^b is below t (or b has fewer than k articles). Hence
--   integral over (x0, x1] of L_t(k) dt = H_k(x1) - H_k(x0),  H_k(x) = (1/|P|) * sum over peers b of (x - Y_k^b)^+,
-- with Y_k^b = 0 for peers with fewer than k articles (x >= 0 throughout), H_0 = 0, and
--   PiP_tau(a) = 1/(1-tau) * sum over k >= 0 of (1/2) * [H_k(y_(k)) - H_k(lo_k) + H_(k+1)(y_(k)) - H_(k+1)(lo_k)],
--   lo_k = max(tau, y_(k+1)), over the k with y_(k) > lo_k.
-- H_k is evaluated by sorting the peers' Y_k^b together with the query points within (start year, age, grid, k): an
-- as-of running count and sum. The cost grows with the number of articles, not with the number of thresholds.
--
-- Grids. For K > 0 every percentile is first rounded up to the next multiple of 1/K, (FLOOR(K y) + 1) / K. On
-- (j/K, (j+1)/K] a rounded percentile is >= t exactly when y >= j/K, so the integral of the rounded percentiles equals
-- the left Riemann sum on t_j = j/K that 12 and 15 compute directly (the production grid is K = 40). K = 0 is the exact
-- integral of Eq. 4.
-- Peers: every author of the benchmark in the same (start year, age), as in 06.
-- Placeholders filled by run_25.sh: {MODE_PREFIX} {BENCHMARK} {LO} {HI} {AGES} {GRIDS} {TAUS}.
{MODE_PREFIX}
WITH
  targets AS (
    SELECT authorid, start_year, age
    FROM `scholar-version2.paper_rp.author_pip_by_age`
    WHERE benchmark = '{BENCHMARK}' AND start_year BETWEEN {LO} AND {HI} AND age IN ({AGES})
  ),
  group_size AS (
    SELECT start_year, age, COUNT(*) AS n_peers FROM targets GROUP BY start_year, age
  ),
  papers AS (
    SELECT p.authorid, p.start_year, p.age, g AS k_grid, IF(g = 0, p.y, (FLOOR(p.y * g) + 1) / g) AS v
    FROM (
      SELECT t.authorid, t.start_year, t.age, a.below[OFFSET(t.start_year + t.age - a.pub_year)] AS y
      FROM targets AS t
      JOIN `scholar-version2.paper_rp.author_articles` AS a
        ON a.authorid = t.authorid AND a.start_year = t.start_year
      WHERE a.start_year BETWEEN {LO} AND {HI} AND a.pub_year <= t.start_year + t.age
    ) AS p,
      UNNEST([{GRIDS}]) AS g
  ),
  ranked AS (
    SELECT authorid, start_year, age, k_grid, v,
      ROW_NUMBER() OVER w AS k,
      LEAD(v) OVER w AS v_next
    FROM papers
    WINDOW w AS (PARTITION BY authorid, start_year, age, k_grid ORDER BY v DESC)
  ),
  n_at_least AS (
    SELECT start_year, age, k_grid, k, COUNT(*) AS n_k FROM ranked GROUP BY start_year, age, k_grid, k
  ),
  intervals AS (
    -- k >= 1: the author has exactly k articles at or above t on (max(tau, v_(k+1)), v_(k)]
    SELECT r.authorid, r.start_year, r.age, r.k_grid, r.k, tau, r.v AS hi, GREATEST(tau, IFNULL(r.v_next, 0)) AS lo
    FROM ranked AS r, UNNEST([{TAUS}]) AS tau
    UNION ALL
    -- k = 0: no article at or above t on (max(tau, v_(1)), 1]
    SELECT r.authorid, r.start_year, r.age, r.k_grid, 0, tau, 1.0, GREATEST(tau, r.v)
    FROM ranked AS r, UNNEST([{TAUS}]) AS tau
    WHERE r.k = 1
  ),
  queries AS (
    -- the standing on interval k is (L_t(k) + L_t(k+1)) / 2, so each interval asks for H_k and H_(k+1) at both ends
    -- with weight 1/2; H_0 = 0 (no peer has fewer than zero articles) and is skipped
    SELECT i.authorid, i.start_year, i.age, i.k_grid, kk AS k, i.tau, q.x, q.sgn
    FROM intervals AS i,
      UNNEST([i.k, i.k + 1]) AS kk,
      UNNEST([STRUCT(i.hi AS x, 0.5 AS sgn), STRUCT(i.lo AS x, -0.5 AS sgn)]) AS q
    WHERE i.hi > i.lo AND kk >= 1
  ),
  points AS (
    SELECT start_year, age, k_grid, k, v AS x, 1 AS is_peer,
      CAST(NULL AS STRING) AS authorid, CAST(NULL AS FLOAT64) AS tau, 0.0 AS sgn
    FROM ranked
    UNION ALL
    SELECT start_year, age, k_grid, k, x, 0, authorid, tau, sgn FROM queries
  ),
  asof AS (
    -- peers sort before queries at equal values; a peer at Y = x adds (x - Y) = 0 either way
    SELECT authorid, start_year, age, k_grid, k, tau, sgn, x, is_peer,
      SUM(is_peer) OVER w AS c_le,
      SUM(IF(is_peer = 1, x, 0)) OVER w AS s_le
    FROM points
    WINDOW w AS (PARTITION BY start_year, age, k_grid, k ORDER BY x, is_peer DESC
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  ),
  contrib AS (
    SELECT q.authorid, q.start_year, q.age, q.k_grid, q.tau,
      -- peers without a k-th article have Y_k = 0 and add x each; no peer has one when k exceeds every peer's count
      SUM(q.sgn * (q.x * (q.c_le + g.n_peers - IFNULL(n.n_k, 0)) - q.s_le) / g.n_peers) AS integral
    FROM asof AS q
    JOIN group_size AS g USING (start_year, age)
    LEFT JOIN n_at_least AS n USING (start_year, age, k_grid, k)
    WHERE q.is_peer = 0
    GROUP BY q.authorid, q.start_year, q.age, q.k_grid, q.tau
  )
SELECT '{BENCHMARK}' AS benchmark, t.authorid, t.start_year, t.age, t.start_year + t.age AS eval_year,
  g AS k_grid, tau, IFNULL(c.integral, 0) / (1 - tau) AS score
FROM targets AS t
CROSS JOIN UNNEST([{GRIDS}]) AS g
CROSS JOIN UNNEST([{TAUS}]) AS tau
LEFT JOIN contrib AS c
  ON c.authorid = t.authorid AND c.start_year = t.start_year AND c.age = t.age AND c.k_grid = g AND c.tau = tau;
