-- 14: How much would the PiP variants change the live Google Scholar scores?
--
-- Population: active GS authors (h-index >= 3 and at least 3 cited works), peers = active authors with the same
-- year_of_first_pub, as in the live app. Paper percentile y = the live num_citations_percentile (share of cited works
-- from the same year with at most as many citations). Same 40-point threshold grid as 12:
--   pip_grid   current PiP on the grid; F(m) = share of peers with at most m cited works (exact, not interpolated)
--   sym        compare your count above each threshold with peers' counts above the same threshold
--   sym_tau50  symmetric, thresholds >= 0.5 only
-- Output: one row per active author with the live score and the three variants.
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.gs_variant_scores`
OPTIONS (description = 'PiP variants on live GS data, for impact comparison. Built by paper/sql/14_gs_variant_impact.sql')
AS
WITH
  authors AS (
    SELECT scholar_id, year_of_first_pub AS cohort
    FROM `scholar-version2.gs_statistics.stats_author_current`
    WHERE hindex >= 3 AND total_publications_with_citations >= 3 AND year_of_first_pub IS NOT NULL
  ),
  buckets AS (
    SELECT a.scholar_id, a.cohort, LEAST(39, CAST(FLOOR(p.num_citations_percentile * 40) AS INT64)) AS b, COUNT(*) AS cnt
    FROM authors AS a
    JOIN `scholar-version2.gs_statistics.ranked_publication_current` AS p USING (scholar_id)
    GROUP BY a.scholar_id, a.cohort, b
  ),
  nj AS (
    SELECT g.scholar_id, g.cohort, g.j,
      SUM(IFNULL(b.cnt, 0)) OVER (PARTITION BY g.scholar_id ORDER BY g.j DESC
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_j
    FROM (SELECT scholar_id, cohort, j FROM authors, UNNEST(GENERATE_ARRAY(0, 39)) AS j) AS g
    LEFT JOIN buckets AS b ON b.scholar_id = g.scholar_id AND b.b = g.j
  ),
  dist_j AS (
    SELECT cohort, j, m, cnt,
      (SUM(cnt) OVER (PARTITION BY cohort, j ORDER BY m) - cnt) / SUM(cnt) OVER (PARTITION BY cohort, j) AS g_below
    FROM (SELECT cohort, j, n_j AS m, COUNT(*) AS cnt FROM nj GROUP BY cohort, j, n_j)
  ),
  totals AS (SELECT cohort, m, cnt FROM dist_j WHERE j = 0),
  f_dense AS (
    SELECT d.cohort, d.m,
      SUM(IFNULL(t.cnt, 0)) OVER (PARTITION BY d.cohort ORDER BY d.m) / SUM(IFNULL(t.cnt, 0)) OVER (PARTITION BY d.cohort) AS f
    FROM (SELECT cohort, m FROM (SELECT cohort, MAX(m) AS mx FROM totals GROUP BY cohort), UNNEST(GENERATE_ARRAY(0, mx)) AS m) AS d
    LEFT JOIN totals AS t ON t.cohort = d.cohort AND t.m = d.m
  ),
  scores AS (
    SELECT n.scholar_id, n.cohort,
      AVG(f.f) AS pip_grid,
      AVG(g.g_below) AS sym,
      SUM(IF(n.j >= 20, g.g_below, 0)) / 20 AS sym_tau50,
      MAX(IF(n.j = 39, n.n_j, NULL)) AS n_top,
      MAX(IF(n.j = 0, n.n_j, NULL)) AS n_cited
    FROM nj AS n
    JOIN f_dense AS f ON f.cohort = n.cohort AND f.m = n.n_j
    JOIN dist_j AS g ON g.cohort = n.cohort AND g.j = n.j AND g.m = n.n_j
    GROUP BY n.scholar_id, n.cohort
  )
SELECT s.*, l.pip_auc_score AS pip_live, (SELECT COUNT(*) FROM authors a WHERE a.cohort = s.cohort) AS cohort_size
FROM scores AS s
JOIN `scholar-version2.gs_statistics.stats_author_pip_scores_current` AS l USING (scholar_id);
