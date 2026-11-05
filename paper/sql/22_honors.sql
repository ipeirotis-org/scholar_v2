-- 22: External validation. Does an indicator measured at career age 5 or 10 anticipate later scientific honors?
--
-- Honors come from Wikidata, loaded by paper/honors/fetch_wikidata_honors.py, and reach Semantic Scholar authors two
-- ways:
--   s2 route   Wikidata items carrying a Semantic Scholar author ID (P4012): `wikidata_s2_items` (every linked
--              person) and `wikidata_honors` (curated honors: 'senior' = academies, learned-society fellowships and
--              major prizes; 'early' = early-career awards, not used here)
--   gs route   Wikidata items carrying a Google Scholar ID (P1960) with a senior honor (`wikidata_gs_honors`), linked
--              to S2 through our crawled GS profiles and the title matching of 19 (confident matches only: at least 5
--              matched works and 30% of the profile's eligible titles)
-- Citation-based honors (Clarivate Citation Laureates) are excluded on purpose.
-- Comparison groups: `in_wikidata` (s2 route items) and `in_gs_faculty` (our GS profiles matched to S2).
-- Population: active authors (06) at career age 5 or 10, start years 1960-2000. Predictors: each indicator as a
-- within-cohort Hazen percentile among all active authors of the same start year at that age, including PiP at every
-- cutoff of 12 (p_sym0 .. p_sym90; p_pip is tau = 0.5).
-- Outcome `honored`: holds a senior honor in Wikidata. People whose earliest dated senior honor is at or before the
-- evaluation year are dropped at that age (the honor would precede the predictor). Undated honors count as later
-- honors, which is the typical case for academies and fellowships; `honored_dated` uses dated honors only.
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.honors_base`
OPTIONS (description = 'Indicators at career age 5 and 10 with later Wikidata honors. Built by paper/sql/22_honors.sql')
AS
WITH
  pop AS (
    SELECT s.authorid, s.start_year, s.age, s.eval_year, s.sym_tau50, s.pip, s.h_index, s.citations, s.n_articles,
      v.sym, v.sym_tau25, v.sym_tau60, v.sym_tau75, v.sym_tau90
    FROM `scholar-version2.paper_rp.sym_by_age` AS s
    JOIN `scholar-version2.paper_rp.pip_variants` AS v USING (authorid, start_year, age)
    WHERE s.age IN (5, 10) AND s.start_year BETWEEN 1960 AND 2000
  ),
  papers AS (
    SELECT p.authorid, p.start_year, p.age, a.below[OFFSET(p.eval_year - a.pub_year)] AS y
    FROM pop AS p
    JOIN `scholar-version2.paper_rp.author_articles` AS a
      ON a.authorid = p.authorid AND a.start_year = p.start_year
    WHERE a.pub_year <= p.eval_year
  ),
  agg AS (
    SELECT authorid, start_year, age, COUNTIF(y >= 0.9) AS top10, SUM(y) AS i3, AVG(y) AS mean_pct
    FROM papers GROUP BY authorid, start_year, age
  ),
  joined AS (
    SELECT pop.*, agg.top10, agg.i3, agg.mean_pct FROM pop JOIN agg USING (authorid, start_year, age)
  ),
  pct AS (
    SELECT authorid, start_year, age, eval_year,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY sym_tau50) - RANK() OVER (w ORDER BY sym_tau50 DESC)) / (2 * COUNT(*) OVER w) AS p_pip,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY pip) - RANK() OVER (w ORDER BY pip DESC)) / (2 * COUNT(*) OVER w) AS p_pip_original,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY h_index) - RANK() OVER (w ORDER BY h_index DESC)) / (2 * COUNT(*) OVER w) AS p_h,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY citations) - RANK() OVER (w ORDER BY citations DESC)) / (2 * COUNT(*) OVER w) AS p_citations,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY n_articles) - RANK() OVER (w ORDER BY n_articles DESC)) / (2 * COUNT(*) OVER w) AS p_articles,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY top10) - RANK() OVER (w ORDER BY top10 DESC)) / (2 * COUNT(*) OVER w) AS p_top10,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY i3) - RANK() OVER (w ORDER BY i3 DESC)) / (2 * COUNT(*) OVER w) AS p_i3,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY mean_pct) - RANK() OVER (w ORDER BY mean_pct DESC)) / (2 * COUNT(*) OVER w) AS p_mean_pct,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY sym) - RANK() OVER (w ORDER BY sym DESC)) / (2 * COUNT(*) OVER w) AS p_sym0,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY sym_tau25) - RANK() OVER (w ORDER BY sym_tau25 DESC)) / (2 * COUNT(*) OVER w) AS p_sym25,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY sym_tau60) - RANK() OVER (w ORDER BY sym_tau60 DESC)) / (2 * COUNT(*) OVER w) AS p_sym60,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY sym_tau75) - RANK() OVER (w ORDER BY sym_tau75 DESC)) / (2 * COUNT(*) OVER w) AS p_sym75,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY sym_tau90) - RANK() OVER (w ORDER BY sym_tau90 DESC)) / (2 * COUNT(*) OVER w) AS p_sym90
    FROM joined
    WINDOW w AS (PARTITION BY start_year, age)
  ),
  gs_faculty AS (
    SELECT DISTINCT s2_authorid AS authorid, scholar_id
    FROM `scholar-version2.paper_rp.gs_s2_match`
    WHERE n_matched_best >= 5 AND n_matched_best >= 0.3 * n_eligible
  ),
  all_honors AS (
    SELECT s2_authorid AS authorid, honor_year
    FROM `scholar-version2.paper_rp.wikidata_honors` WHERE category = 'senior'
    UNION ALL
    SELECT f.authorid, g.honor_year
    FROM `scholar-version2.paper_rp.wikidata_gs_honors` AS g JOIN gs_faculty AS f USING (scholar_id)
  ),
  honors AS (
    SELECT authorid, MIN(honor_year) AS first_dated_year, COUNT(*) AS n_honors,
      COUNTIF(honor_year IS NOT NULL) AS n_dated
    FROM all_honors
    GROUP BY authorid
  ),
  wd AS (SELECT DISTINCT s2_authorid AS authorid FROM `scholar-version2.paper_rp.wikidata_s2_items`)
SELECT p.*,
  wd.authorid IS NOT NULL AS in_wikidata,
  p.authorid IN (SELECT authorid FROM gs_faculty) AS in_gs_faculty,
  h.authorid IS NOT NULL AS honored,
  IFNULL(h.n_dated, 0) > 0 AS honored_dated,
  h.first_dated_year
FROM pct AS p
LEFT JOIN wd USING (authorid)
LEFT JOIN honors AS h USING (authorid)
WHERE h.first_dated_year IS NULL OR h.first_dated_year > p.eval_year;
