-- 10: Variant: paper percentiles relative to same-cohort peers' papers (age-matched), at career age 10.
-- Each paper's percentile = share of papers published the same year by active peers with the same start year that have
-- strictly fewer citations at Y = start + 10. Compares the resulting PiP with the main definition (same-year articles).
-- Decided 2026-11-04: paper variant only; the app keeps same-year percentiles. Results in FINDINGS.md §6.
WITH
  peers AS (
    SELECT authorid, start_year, n_articles, n_peers, prod_pct, pip
    FROM `scholar-version2.paper_rp.author_pip_by_age`
    WHERE benchmark = 'active' AND age = 10 AND start_year BETWEEN 1975 AND 2010
  ),
  weights AS (
    SELECT start_year, n_articles AS k, COUNT(*) / ANY_VALUE(n_peers) AS pk
    FROM peers GROUP BY start_year, n_articles
  ),
  papers AS (
    SELECT p.authorid, p.start_year, a.pub_year, a.corpusid,
      a.cites[OFFSET(p.start_year + 10 - a.pub_year)] AS c,
      a.below[OFFSET(p.start_year + 10 - a.pub_year)] AS y_global
    FROM peers p
    JOIN `scholar-version2.paper_rp.author_articles` a
      ON a.authorid = p.authorid AND a.start_year = p.start_year
    WHERE a.start_year BETWEEN 1975 AND 2010 AND a.pub_year <= p.start_year + 10
  ),
  -- The reference set is distinct papers, so a paper with several same-cohort co-authors counts once
  -- (c is the same for every co-author: same publication year, same evaluation year).
  ref_papers AS (
    SELECT DISTINCT start_year, pub_year, corpusid, c FROM papers
  ),
  ref_pct AS (
    SELECT start_year, pub_year, corpusid,
      -- share of same-cohort peers' papers from the same year with strictly fewer citations
      (RANK() OVER (PARTITION BY start_year, pub_year ORDER BY c) - 1)
        / COUNT(*) OVER (PARTITION BY start_year, pub_year) AS y_peer
    FROM ref_papers
  ),
  peer_pct AS (
    SELECT p.*, r.y_peer
    FROM papers AS p
    JOIN ref_pct AS r USING (start_year, pub_year, corpusid)
  ),
  ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY authorid ORDER BY y_peer DESC, c DESC, corpusid) AS k
    FROM peer_pct
  ),
  scored AS (
    SELECT r.authorid, r.start_year,
      SUM(r.y_peer * IFNULL(w.pk, 0)) AS pip_peer,
      AVG(r.y_peer) AS mean_peer, AVG(r.y_global) AS mean_global
    FROM ranked r LEFT JOIN weights w ON w.start_year = r.start_year AND w.k = r.k
    GROUP BY r.authorid, r.start_year
  ),
  j AS (
    SELECT p.*, s.pip_peer, s.mean_peer, s.mean_global,
      p.pip / p.prod_pct AS w_global, s.pip_peer / p.prod_pct AS w_peer
    FROM peers p JOIN scored s USING (authorid, start_year)
  ),
  pct AS (
    SELECT *,
      (COUNT(*) OVER (PARTITION BY start_year) + RANK() OVER (PARTITION BY start_year ORDER BY pip) - RANK() OVER (PARTITION BY start_year ORDER BY pip DESC)) / (2 * COUNT(*) OVER (PARTITION BY start_year)) AS r_pip,
      (COUNT(*) OVER (PARTITION BY start_year) + RANK() OVER (PARTITION BY start_year ORDER BY pip_peer) - RANK() OVER (PARTITION BY start_year ORDER BY pip_peer DESC)) / (2 * COUNT(*) OVER (PARTITION BY start_year)) AS r_pip_peer,
      (COUNT(*) OVER (PARTITION BY start_year) + RANK() OVER (PARTITION BY start_year ORDER BY n_articles) - RANK() OVER (PARTITION BY start_year ORDER BY n_articles DESC)) / (2 * COUNT(*) OVER (PARTITION BY start_year)) AS r_n,
      (COUNT(*) OVER (PARTITION BY start_year) + RANK() OVER (PARTITION BY start_year ORDER BY mean_peer) - RANK() OVER (PARTITION BY start_year ORDER BY mean_peer DESC)) / (2 * COUNT(*) OVER (PARTITION BY start_year)) AS r_mean_peer,
      (COUNT(*) OVER (PARTITION BY start_year) + RANK() OVER (PARTITION BY start_year ORDER BY mean_global) - RANK() OVER (PARTITION BY start_year ORDER BY mean_global DESC)) / (2 * COUNT(*) OVER (PARTITION BY start_year)) AS r_mean_global
    FROM j
  )
SELECT
  COUNT(*) AS authors,
  ROUND(CORR(r_pip, r_n), 3) AS cur_vs_articles,
  ROUND(CORR(r_pip_peer, r_n), 3) AS peer_vs_articles,
  ROUND(CORR(r_pip, r_mean_global), 3) AS cur_vs_mean_quality,
  ROUND(CORR(r_pip_peer, r_mean_peer), 3) AS peer_vs_mean_quality,
  ROUND(CORR(r_pip, r_pip_peer), 3) AS cur_vs_peer,
  ROUND(VAR_POP(LN(prod_pct)) / VAR_POP(LN(pip)), 3) AS cur_share_prod,
  ROUND(VAR_POP(LN(w_global)) / VAR_POP(LN(pip)), 3) AS cur_share_quality,
  ROUND(VAR_POP(LN(prod_pct)) / VAR_POP(LN(GREATEST(pip_peer, 1e-4))), 3) AS peer_share_prod,
  ROUND(VAR_POP(LN(GREATEST(w_peer, 1e-4))) / VAR_POP(LN(GREATEST(pip_peer, 1e-4))), 3) AS peer_share_quality,
  FORMAT('%.2f / %.2f / %.2f', APPROX_QUANTILES(w_global, 100)[OFFSET(10)], APPROX_QUANTILES(w_global, 100)[OFFSET(50)], APPROX_QUANTILES(w_global, 100)[OFFSET(90)]) AS w_cur_p10_50_90,
  FORMAT('%.2f / %.2f / %.2f', APPROX_QUANTILES(w_peer, 100)[OFFSET(10)], APPROX_QUANTILES(w_peer, 100)[OFFSET(50)], APPROX_QUANTILES(w_peer, 100)[OFFSET(90)]) AS w_peer_p10_50_90,
  ROUND(AVG(mean_global), 3) AS avg_paper_pct_cur,
  ROUND(AVG(mean_peer), 3) AS avg_paper_pct_peer
FROM pct
