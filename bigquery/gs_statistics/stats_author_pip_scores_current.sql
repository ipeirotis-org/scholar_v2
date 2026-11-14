CREATE OR REPLACE VIEW `scholar-version2.gs_statistics.stats_author_pip_scores_current` AS
-- Level 4: Raw PiP (tau = 0.5), 40-class standing curve, and legacy PiP-AUC per author.
--
-- PiP (paper/sql/14_gs_variant_impact.sql, sym_tau50):
--   Each cited work falls into citation-percentile class b = LEAST(39, FLOOR(40 * y)),
--   corresponding to threshold theta_j = j / 40 for j = 0..39. At each j, n_j is the
--   author's count of cited works in class >= j, and g_j is the share of active peers
--   in the same year_of_first_pub cohort with strictly fewer (< n_j) such works
--   (0 when n_j = 0), looked up via RANGE_BUCKET(n_j - 1, m_arr) in dist_pip_thresholds.
--   pip_score averages g_j over the top half of thresholds (j = 20..39, tau = 0.5).
--   pip_standing_json serializes all 40 values [g_0, ..., g_39] for the standing curve
--   chart and interactive cutoff (tau) selector.
--
-- PiP-AUC (pip_auc_score):
--   Area under the legacy step curve: sum over works k (in percentile order) of
--   y_k * (x_k - x_(k-1)). Kept for the third chart tab (PiP-AUC curve).
--
-- Every CTE partitions or groups by scholar_id so WHERE scholar_id = @scholar_id
-- pushes straight down for single-author lookups.
WITH
  RankedPublications AS (
    SELECT
      scholar_id,
      author_pub_id,
      num_citations_percentile,
      num_papers_percentile,
      -- Width of work k's step: x_k - x_(k-1), x_0 = 0, in rank order (ties in x get width 0)
      COALESCE(LAG(num_papers_percentile) OVER(PARTITION BY scholar_id ORDER BY publication_rank), 0) AS prev_num_papers_percentile
    FROM `scholar-version2.gs_statistics.stats_author_publication_pip_inputs_current`
  ),
  AuthorAgg AS (
    SELECT
      scholar_id,
      ROUND(SUM((num_papers_percentile - prev_num_papers_percentile) * num_citations_percentile), 4) AS pip_auc_score,
      ARRAY_AGG(LEAST(39, CAST(FLOOR(num_citations_percentile * 40) AS INT64))) AS pub_buckets
    FROM RankedPublications
    GROUP BY scholar_id
  ),
  AuthorWithCohort AS (
    SELECT
      A.scholar_id,
      AuthStats.year_of_first_pub,
      A.pip_auc_score,
      A.pub_buckets
    FROM AuthorAgg A
    JOIN `scholar-version2.gs_statistics.stats_author_current` AuthStats
      ON A.scholar_id = AuthStats.scholar_id
  ),
  AuthorThresholds AS (
    SELECT
      ac.scholar_id,
      ac.year_of_first_pub,
      ac.pip_auc_score,
      j,
      (SELECT COUNTIF(b >= j) FROM UNNEST(ac.pub_buckets) AS b) AS n_j
    FROM AuthorWithCohort ac,
    UNNEST(GENERATE_ARRAY(0, 39)) AS j
  ),
  AuthorStanding AS (
    SELECT
      t.scholar_id,
      t.year_of_first_pub,
      t.pip_auc_score,
      t.j,
      CASE
        WHEN dt.cohort_size IS NULL THEN NULL
        WHEN t.n_j <= 0 THEN 0.0
        ELSE COALESCE(dt.cum_arr[SAFE_ORDINAL(RANGE_BUCKET(t.n_j - 1, dt.m_arr))], 0) / dt.cohort_size
      END AS g_j
    FROM AuthorThresholds t
    LEFT JOIN `scholar-version2.gs_statistics.dist_pip_thresholds` dt
      ON dt.year_of_first_pub = t.year_of_first_pub
     AND dt.j = t.j
  )
SELECT
  scholar_id,
  ANY_VALUE(year_of_first_pub) AS year_of_first_pub,
  ROUND(AVG(IF(j >= 20, g_j, NULL)), 4) AS pip_score,
  IF(COUNT(g_j) = 40,
     TO_JSON_STRING(ARRAY_AGG(ROUND(g_j, 6) IGNORE NULLS ORDER BY j)),
     NULL) AS pip_standing_json,
  ANY_VALUE(pip_auc_score) AS pip_auc_score
FROM AuthorStanding
GROUP BY scholar_id;
