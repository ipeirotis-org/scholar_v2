-- 24: Cross-sectional check on Google Scholar profiles: do current scores pick out the scholars who hold honors or
-- have a Wikipedia article?
--
-- Population: active GS profiles (h-index >= 3, at least 3 cited works) in cohorts (year of first publication) of at
-- least 30 such authors, as in 14. Predictors, each as a within-cohort Hazen percentile: PiP (sym_tau50 and
-- sym_tau75 from 14), the live original PiP-AUC, h-index, citations, i10-index, cited works, top-10% works (live
-- percentile >= 0.9), I3 (sum of the live work percentiles) and the mean work percentile.
-- Outcomes, from Wikidata items carrying the profile's Google Scholar ID (P1960):
--   honored    holds a curated senior honor (`wikidata_gs_honors`, loaded by paper/honors/fetch_wikidata_honors.py)
--   enwiki     has an English Wikipedia article (`gs_data.wikidata_authors.enwiki_title`)
--   views      English Wikipedia pageviews over the last 60 days (0 without an article)
-- Unlike 22, this is not a prediction: scores and honors are measured now, so it checks agreement, not foresight.
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.gs_honors_base`
OPTIONS (description = 'GS profiles: cohort percentiles of indicators and Wikidata/Wikipedia recognition. Built by paper/sql/24_gs_honors.sql')
AS
WITH
  scores AS (
    SELECT v.scholar_id, v.cohort, v.sym_tau50, v.sym_tau75, v.pip_live,
      a.hindex, a.citedby, a.i10index, a.total_publications_with_citations AS cited_works
    FROM `scholar-version2.paper_rp.gs_variant_scores` AS v
    JOIN `scholar-version2.gs_statistics.stats_author_current` AS a USING (scholar_id)
    WHERE v.cohort_size >= 30
  ),
  works AS (
    SELECT p.scholar_id, COUNTIF(p.num_citations_percentile >= 0.9) AS top10,
      SUM(p.num_citations_percentile) AS i3, AVG(p.num_citations_percentile) AS mean_pct
    FROM `scholar-version2.gs_statistics.ranked_publication_current` AS p
    JOIN scores USING (scholar_id)
    GROUP BY p.scholar_id
  ),
  joined AS (SELECT * FROM scores JOIN works USING (scholar_id)),
  pct AS (
    SELECT scholar_id, cohort,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY sym_tau50) - RANK() OVER (w ORDER BY sym_tau50 DESC)) / (2 * COUNT(*) OVER w) AS p_pip,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY sym_tau75) - RANK() OVER (w ORDER BY sym_tau75 DESC)) / (2 * COUNT(*) OVER w) AS p_sym75,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY pip_live) - RANK() OVER (w ORDER BY pip_live DESC)) / (2 * COUNT(*) OVER w) AS p_pip_original,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY hindex) - RANK() OVER (w ORDER BY hindex DESC)) / (2 * COUNT(*) OVER w) AS p_h,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY citedby) - RANK() OVER (w ORDER BY citedby DESC)) / (2 * COUNT(*) OVER w) AS p_citations,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY i10index) - RANK() OVER (w ORDER BY i10index DESC)) / (2 * COUNT(*) OVER w) AS p_i10,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY cited_works) - RANK() OVER (w ORDER BY cited_works DESC)) / (2 * COUNT(*) OVER w) AS p_articles,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY top10) - RANK() OVER (w ORDER BY top10 DESC)) / (2 * COUNT(*) OVER w) AS p_top10,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY i3) - RANK() OVER (w ORDER BY i3 DESC)) / (2 * COUNT(*) OVER w) AS p_i3,
      (COUNT(*) OVER w + RANK() OVER (w ORDER BY mean_pct) - RANK() OVER (w ORDER BY mean_pct DESC)) / (2 * COUNT(*) OVER w) AS p_mean_pct
    FROM joined
    WINDOW w AS (PARTITION BY cohort)
  ),
  honored AS (SELECT DISTINCT scholar_id FROM `scholar-version2.paper_rp.wikidata_gs_honors`),
  wiki AS (
    SELECT scholar_id, MAX(enwiki_title IS NOT NULL) AS enwiki, MAX(IFNULL(enwiki_views_60d, 0)) AS views
    FROM `scholar-version2.gs_data.wikidata_authors`
    GROUP BY scholar_id
  )
SELECT p.*,
  h.scholar_id IS NOT NULL AS honored,
  IFNULL(w.enwiki, FALSE) AS enwiki,
  IFNULL(w.views, 0) AS views,
  w.scholar_id IS NOT NULL AS in_wikidata
FROM pct AS p
LEFT JOIN honored AS h USING (scholar_id)
LEFT JOIN wiki AS w USING (scholar_id);
