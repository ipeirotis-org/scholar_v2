-- Level 5: Temporal PiP-AUC score per author per year — no percentile column.
--
-- Computes PiP-AUC at each point in time by:
--   1. Taking all publications with pub_year <= state_year
--   2. Looking up each pub's cumulative_citations as of state_year → citation percentile
--      (via dist_publication_citations_temporal, L2)
--   3. Counting how many papers the author had → num_papers_percentile
--      (via dist_author_metrics_temporal, L4, using RANGE_BUCKET array lookup)
--   4. Running the same trapezoidal integration → pip_auc_score
--
-- Depends on: intermediate_author_publication_state_temporal (L2),
--   dist_publication_citations_temporal (L2), stats_author_metrics_temporal_view (L3),
--   dist_author_metrics_temporal (L4).
--
-- This is the most expensive view in the system. It should ALWAYS be materialized,
-- never queried live.
--
-- The pip_auc_score_percentile is added by ranked_author_pip_scores_temporal (L7)
-- via dist_pip_auc_scores_temporal (L6).

CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_author_pip_scores_temporal_view` AS

WITH
  -- Get per-publication per-year state from the intermediate view
  PubState AS (
    SELECT
      scholar_id,
      author_pub_id,
      pub_year,
      state_year,
      cumulative_citations_at_state_year
    FROM `scholar-version2.statistics.intermediate_author_publication_state_temporal`
    WHERE cumulative_citations_at_state_year > 0
  ),

  -- Get author's year_of_first_pub and total_publications per state_year
  -- from the temporal metrics view (Tier 1)
  AuthorState AS (
    SELECT
      scholar_id,
      state_year,
      year_of_first_pub,
      total_publications
    FROM `scholar-version2.statistics.stats_author_metrics_temporal_view`
    WHERE year_of_first_pub IS NOT NULL
  ),

  -- Pre-aggregate citation dist breakpoints into sorted arrays per (pub_year, citation_year).
  -- Uses RANGE_BUCKET for O(log n) floor lookup instead of range join.
  CitDistArrays AS (
    SELECT
      pub_year,
      citation_year,
      ARRAY_AGG(metric_value ORDER BY metric_value, percentile) AS values_arr,
      ARRAY_AGG(percentile ORDER BY metric_value, percentile) AS pcts_arr
    FROM `scholar-version2.statistics.dist_publication_citations_temporal`
    WHERE metric_name = 'pub_year_cumulative_citations'
    GROUP BY pub_year, citation_year
  ),

  -- Look up citation percentile for each pub's cumulative_citations at each state_year.
  -- Equi-join on (pub_year, state_year=citation_year) + RANGE_BUCKET for floor lookup.
  PubPercentiles AS (
    SELECT
      ps.scholar_id,
      ps.author_pub_id,
      ps.state_year,
      ps.cumulative_citations_at_state_year,
      COALESCE(
        cd.pcts_arr[SAFE_ORDINAL(RANGE_BUCKET(ps.cumulative_citations_at_state_year, cd.values_arr))],
        0.0
      ) AS citation_percentile
    FROM PubState ps
    LEFT JOIN CitDistArrays cd
      ON cd.pub_year = ps.pub_year
     AND cd.citation_year = ps.state_year
  ),

  -- Rank publications within each author-year by citation percentile (descending)
  RankedPubs AS (
    SELECT
      pp.scholar_id,
      pp.state_year,
      pp.author_pub_id,
      pp.citation_percentile,
      ROW_NUMBER() OVER(PARTITION BY pp.scholar_id, pp.state_year ORDER BY pp.citation_percentile DESC) AS publication_rank,
      a.year_of_first_pub,
      a.total_publications
    FROM PubPercentiles pp
    JOIN AuthorState a ON pp.scholar_id = a.scholar_id AND pp.state_year = a.state_year
  ),

  -- Pre-aggregate dist breakpoints into sorted arrays per (year_of_first_pub, state_year).
  -- Uses RANGE_BUCKET for O(log n) binary search — no correlated scalar subqueries.
  DistArrays AS (
    SELECT
      year_of_first_pub,
      state_year,
      ARRAY_AGG(metric_value ORDER BY metric_value, percentile) AS values_arr,
      ARRAY_AGG(percentile ORDER BY metric_value, percentile) AS pcts_arr
    FROM `scholar-version2.statistics.dist_author_metrics_temporal`
    WHERE benchmark = 'active_authors'
      AND metric_name = 'total_publications'
    GROUP BY year_of_first_pub, state_year
  ),

  InterpolatedPubs AS (
    SELECT
      rp.scholar_id,
      rp.state_year,
      rp.author_pub_id,
      rp.citation_percentile AS num_citations_percentile,
      rp.publication_rank,
      rp.year_of_first_pub,
      CASE
        WHEN RANGE_BUCKET(rp.publication_rank, da.values_arr) = 0 THEN 0.0
        WHEN RANGE_BUCKET(rp.publication_rank, da.values_arr) >= ARRAY_LENGTH(da.values_arr) THEN 1.0
        WHEN da.values_arr[SAFE_ORDINAL(RANGE_BUCKET(rp.publication_rank, da.values_arr))]
           = da.values_arr[SAFE_ORDINAL(RANGE_BUCKET(rp.publication_rank, da.values_arr) + 1)]
          THEN da.pcts_arr[SAFE_ORDINAL(RANGE_BUCKET(rp.publication_rank, da.values_arr))]
        ELSE (
          da.pcts_arr[SAFE_ORDINAL(RANGE_BUCKET(rp.publication_rank, da.values_arr) + 1)]
            * (rp.publication_rank - da.values_arr[SAFE_ORDINAL(RANGE_BUCKET(rp.publication_rank, da.values_arr))])
          + da.pcts_arr[SAFE_ORDINAL(RANGE_BUCKET(rp.publication_rank, da.values_arr))]
            * (da.values_arr[SAFE_ORDINAL(RANGE_BUCKET(rp.publication_rank, da.values_arr) + 1)] - rp.publication_rank)
        ) / (
          da.values_arr[SAFE_ORDINAL(RANGE_BUCKET(rp.publication_rank, da.values_arr) + 1)]
          - da.values_arr[SAFE_ORDINAL(RANGE_BUCKET(rp.publication_rank, da.values_arr))]
        )
      END AS num_papers_percentile
    FROM RankedPubs rp
    LEFT JOIN DistArrays da
      ON rp.year_of_first_pub = da.year_of_first_pub
     AND rp.state_year = da.state_year
  ),

  -- Trapezoidal integration (same pattern as stats_author_pip_scores_current)
  TrapezoidInputs AS (
    SELECT
      scholar_id,
      state_year,
      year_of_first_pub,
      num_citations_percentile,
      num_papers_percentile,
      COALESCE(LAG(num_citations_percentile) OVER(PARTITION BY scholar_id, state_year ORDER BY num_papers_percentile), num_citations_percentile) AS prev_num_citations_percentile,
      COALESCE(LAG(num_papers_percentile)    OVER(PARTITION BY scholar_id, state_year ORDER BY num_papers_percentile), 0) AS prev_num_papers_percentile
    FROM InterpolatedPubs
  ),
  TrapezoidAreas AS (
    SELECT
      scholar_id,
      state_year,
      year_of_first_pub,
      (num_papers_percentile - prev_num_papers_percentile) * (num_citations_percentile + prev_num_citations_percentile) / 2 AS area
    FROM TrapezoidInputs
  )

SELECT
  scholar_id,
  state_year,
  year_of_first_pub,
  ROUND(SUM(area), 4) AS pip_auc_score
FROM TrapezoidAreas
GROUP BY scholar_id, state_year, year_of_first_pub;
