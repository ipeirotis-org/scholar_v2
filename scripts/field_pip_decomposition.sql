-- Split each field's PiP-AUC gap into paper count and citations per paper (2026-11-04).
-- PiP-AUC = curve length (paper-count percentile reached) x average curve height (mean
-- within-year citation percentile of the author's papers), so mean log(PiP) gaps between a
-- field and all fields split exactly into from_length + from_height. Active authors only.
-- See TASKS.md "Field-specific normalization" for the 2026-11-04 numbers.
-- Run: bq query --use_legacy_sql=false --format=pretty < scripts/field_pip_decomposition.sql
WITH
  A AS (
    SELECT scholar_id,
           MAX(num_papers_percentile) AS x_end,          -- how far the curve reaches (paper count)
           COUNT(*) AS n_cited,
           APPROX_QUANTILES(num_citations, 2)[OFFSET(1)] AS med_cites_per_paper,
           AVG(num_citations_percentile) AS avg_paper_pct
    FROM `scholar-version2.gs_statistics.stats_author_publication_pip_inputs_current`
    GROUP BY scholar_id
  ),
  J AS (
    SELECT f.field_group, a.*, p.pip_auc_score AS pip, p.pip_auc_score / a.x_end AS height,
           a.n_cited / GREATEST(2026 - s.year_of_first_pub, 1) AS papers_per_yr
    FROM A a
    JOIN `scholar-version2.gs_statistics.stats_author_pip_scores_current` p USING (scholar_id)
    JOIN `scholar-version2.gs_statistics.stats_author_current` s USING (scholar_id)
    JOIN `scholar-version2.gs_statistics.author_fields` f USING (scholar_id)
    WHERE s.hindex >= 3 AND s.total_publications >= 3 AND p.pip_auc_score > 0 AND a.x_end > 0
  ),
  Overall AS (SELECT AVG(LN(x_end)) AS lx, AVG(LN(height)) AS lh, AVG(LN(pip)) AS lp FROM J)
SELECT field_group, COUNT(*) AS n,
  ROUND(APPROX_QUANTILES(papers_per_yr, 2)[OFFSET(1)], 1) AS cited_papers_per_yr,
  APPROX_QUANTILES(med_cites_per_paper, 2)[OFFSET(1)] AS med_cites_per_paper,
  ROUND(APPROX_QUANTILES(x_end, 2)[OFFSET(1)], 2) AS curve_length,
  ROUND(APPROX_QUANTILES(height, 2)[OFFSET(1)], 2) AS curve_height,
  ROUND(APPROX_QUANTILES(pip, 2)[OFFSET(1)], 3) AS pip_median,
  -- exact split of each field's mean log(PiP) gap vs. all fields: length part + height part
  ROUND(AVG(LN(pip)) - ANY_VALUE(o.lp), 2) AS log_pip_gap,
  ROUND(AVG(LN(x_end)) - ANY_VALUE(o.lx), 2) AS from_length,
  ROUND(AVG(LN(height)) - ANY_VALUE(o.lh), 2) AS from_height
FROM J CROSS JOIN Overall o
GROUP BY field_group
ORDER BY log_pip_gap DESC
