-- Level 5: Temporal PiP-AUC score per author per year — no percentile column.
--
-- Computes PiP-AUC at each point in time by:
--   1. Taking all publications with pub_year <= state_year
--   2. Looking up each pub's cumulative_citations as of state_year → citation percentile
--      (via dist_publication_citations_temporal, L2)
--   3. Counting how many papers the author had → num_papers_percentile
--      (via dist_author_metrics_temporal, L4, using scalar subquery interpolation)
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

  -- Look up citation percentile for each pub's cumulative_citations at each state_year.
  -- Uses dist_publication_citations_temporal with metric 'pub_year_cumulative_citations'.
  -- Floor lookup: MAX(percentile WHERE dist_value <= actual_value).
  PubPercentiles AS (
    SELECT
      ps.scholar_id,
      ps.author_pub_id,
      ps.state_year,
      ps.cumulative_citations_at_state_year,
      MAX(d.percentile) AS citation_percentile
    FROM PubState ps
    JOIN `scholar-version2.statistics.dist_publication_citations_temporal` d
      ON d.metric_name = 'pub_year_cumulative_citations'
     AND d.pub_year = ps.pub_year
     AND d.citation_year = ps.state_year
     AND d.metric_value <= ps.cumulative_citations_at_state_year
    GROUP BY ps.scholar_id, ps.author_pub_id, ps.state_year, ps.cumulative_citations_at_state_year
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

  -- Interpolate num_papers_percentile using scalar subqueries against
  -- dist_author_metrics_temporal. Finds floor and ceiling breakpoints
  -- for publication_rank, then linearly interpolates.
  InterpolatedPubs AS (
    SELECT
      scholar_id,
      state_year,
      author_pub_id,
      citation_percentile AS num_citations_percentile,
      publication_rank,
      year_of_first_pub,
      CASE
        WHEN (SELECT MAX(d.percentile)
              FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
              WHERE d.benchmark = 'active_authors'
                AND d.metric_name = 'total_publications'
                AND d.year_of_first_pub = rp.year_of_first_pub
                AND d.state_year = rp.state_year
                AND d.metric_value <= rp.publication_rank
             ) IS NULL THEN 0.0
        WHEN (SELECT MIN(d.percentile)
              FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
              WHERE d.benchmark = 'active_authors'
                AND d.metric_name = 'total_publications'
                AND d.year_of_first_pub = rp.year_of_first_pub
                AND d.state_year = rp.state_year
                AND d.metric_value >= rp.publication_rank
             ) IS NULL THEN 1.0
        WHEN (SELECT MAX(d.metric_value)
              FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
              WHERE d.benchmark = 'active_authors'
                AND d.metric_name = 'total_publications'
                AND d.year_of_first_pub = rp.year_of_first_pub
                AND d.state_year = rp.state_year
                AND d.metric_value <= rp.publication_rank
             ) = (SELECT MIN(d.metric_value)
              FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
              WHERE d.benchmark = 'active_authors'
                AND d.metric_name = 'total_publications'
                AND d.year_of_first_pub = rp.year_of_first_pub
                AND d.state_year = rp.state_year
                AND d.metric_value >= rp.publication_rank
             ) THEN (SELECT MAX(d.percentile)
              FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
              WHERE d.benchmark = 'active_authors'
                AND d.metric_name = 'total_publications'
                AND d.year_of_first_pub = rp.year_of_first_pub
                AND d.state_year = rp.state_year
                AND d.metric_value <= rp.publication_rank
             )
        ELSE (
          (SELECT MIN(d.percentile)
           FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
           WHERE d.benchmark = 'active_authors'
             AND d.metric_name = 'total_publications'
             AND d.year_of_first_pub = rp.year_of_first_pub
             AND d.state_year = rp.state_year
             AND d.metric_value >= rp.publication_rank
          ) * (rp.publication_rank - (SELECT MAX(d.metric_value)
           FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
           WHERE d.benchmark = 'active_authors'
             AND d.metric_name = 'total_publications'
             AND d.year_of_first_pub = rp.year_of_first_pub
             AND d.state_year = rp.state_year
             AND d.metric_value <= rp.publication_rank
          ))
          +
          (SELECT MAX(d.percentile)
           FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
           WHERE d.benchmark = 'active_authors'
             AND d.metric_name = 'total_publications'
             AND d.year_of_first_pub = rp.year_of_first_pub
             AND d.state_year = rp.state_year
             AND d.metric_value <= rp.publication_rank
          ) * ((SELECT MIN(d.metric_value)
           FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
           WHERE d.benchmark = 'active_authors'
             AND d.metric_name = 'total_publications'
             AND d.year_of_first_pub = rp.year_of_first_pub
             AND d.state_year = rp.state_year
             AND d.metric_value >= rp.publication_rank
          ) - rp.publication_rank)
        ) / (
          (SELECT MIN(d.metric_value)
           FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
           WHERE d.benchmark = 'active_authors'
             AND d.metric_name = 'total_publications'
             AND d.year_of_first_pub = rp.year_of_first_pub
             AND d.state_year = rp.state_year
             AND d.metric_value >= rp.publication_rank
          ) - (SELECT MAX(d.metric_value)
           FROM `scholar-version2.statistics.dist_author_metrics_temporal` d
           WHERE d.benchmark = 'active_authors'
             AND d.metric_name = 'total_publications'
             AND d.year_of_first_pub = rp.year_of_first_pub
             AND d.state_year = rp.state_year
             AND d.metric_value <= rp.publication_rank
          )
        )
      END AS num_papers_percentile
    FROM RankedPubs rp
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
