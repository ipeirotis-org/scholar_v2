-- Per-field publication and citation norms, from gs_statistics.author_fields (2026-11-03).
-- Shows why field-relative percentiles are needed: groups differ in papers per career
-- year and in where their median author lands on the overall PiP percentile scale.
-- Run: bq query --use_legacy_sql=false --format=pretty < scripts/field_norms.sql
WITH j AS (
  SELECT IFNULL(f.field_group, '(none)') AS field_group, f.field_source,
         s.total_publications AS pubs, s.citedby, s.hindex,
         GREATEST(EXTRACT(YEAR FROM CURRENT_DATE()) - s.year_of_first_pub, 1) AS career_yrs,
         r.pip_auc_score_percentile AS pip_pct
  FROM `scholar-version2.gs_statistics.stats_author_current` s
  LEFT JOIN `scholar-version2.gs_statistics.author_fields` f USING (scholar_id)
  LEFT JOIN `scholar-version2.gs_statistics.ranked_author_pip_scores_current` r USING (scholar_id)
)
SELECT field_group, COUNT(*) AS authors,
  COUNTIF(field_source = 'coauthors') AS from_coauthors,
  APPROX_QUANTILES(pubs, 2)[OFFSET(1)] AS med_pubs,
  ROUND(APPROX_QUANTILES(pubs / career_yrs, 2)[OFFSET(1)], 1) AS med_pubs_per_yr,
  APPROX_QUANTILES(citedby, 2)[OFFSET(1)] AS med_cites,
  APPROX_QUANTILES(hindex, 2)[OFFSET(1)] AS med_h,
  ROUND(APPROX_QUANTILES(pip_pct, 2)[OFFSET(1)], 2) AS med_pip_pct
FROM j
GROUP BY field_group
ORDER BY med_pubs_per_yr DESC
