CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_author_pip_scores_current` AS
-- Computes PiP-AUC score per author via trapezoidal integration and resolves
-- the pip_auc_score_percentile via a floor lookup against the precomputed
-- dist_pip_auc_scores distribution table.
--
-- For a per-author query (WHERE scholar_id = X):
--   - Reads only that author's pip inputs (fast with distribution-based underlying views)
--   - Looks up percentile from dist_pip_auc_scores (small table, fast)
--   - No PERCENT_RANK() computed at query time
WITH
  RankedPublications AS (
    SELECT
      scholar_id,
      author_pub_id,
      num_citations_percentile,
      num_papers_percentile,
      COALESCE(LAG(num_citations_percentile) OVER(PARTITION BY scholar_id ORDER BY num_papers_percentile), num_citations_percentile) AS prev_num_citations_percentile,
      COALESCE(LAG(num_papers_percentile)    OVER(PARTITION BY scholar_id ORDER BY num_papers_percentile), 0)                       AS prev_num_papers_percentile
    FROM `scholar-version2.statistics.stats_author_publication_pip_inputs_current`
  ),
  TrapezoidAreas AS (
    SELECT
      scholar_id,
      (num_papers_percentile - prev_num_papers_percentile) * (num_citations_percentile + prev_num_citations_percentile) / 2 AS area
    FROM RankedPublications
  ),
  AUC AS (
    SELECT
      scholar_id,
      ROUND(SUM(area), 4) AS pip_auc_score
    FROM TrapezoidAreas
    GROUP BY scholar_id
  ),
  AuthorData AS (
    SELECT
      A.scholar_id,
      AuthStats.year_of_first_pub,
      A.pip_auc_score
    FROM AUC A
    JOIN `scholar-version2.statistics.stats_author_current` AuthStats
      ON A.scholar_id = AuthStats.scholar_id
  ),
  ScorePercentile AS (
    -- Floor lookup: MAX(percentile WHERE dist_score <= actual_score) gives an exact
    -- match when the score exists in the distribution, or the nearest-lower
    -- approximation for newly computed scores not yet in the table.
    SELECT
      a.scholar_id,
      MAX(d.percentile) AS pip_auc_score_percentile
    FROM AuthorData a
    LEFT JOIN `scholar-version2.statistics.dist_pip_auc_scores` d
      ON d.year_of_first_pub = a.year_of_first_pub
     AND d.pip_auc_score <= a.pip_auc_score
    GROUP BY a.scholar_id
  )
SELECT
  a.scholar_id,
  a.year_of_first_pub,
  a.pip_auc_score,
  COALESCE(s.pip_auc_score_percentile, 0.0) AS pip_auc_score_percentile
FROM AuthorData a
LEFT JOIN ScorePercentile s ON s.scholar_id = a.scholar_id
ORDER BY a.pip_auc_score DESC;
