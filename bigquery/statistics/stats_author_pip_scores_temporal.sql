-- Level 5: Temporal PiP-AUC score per author per year — no percentile column.
--
-- Computes PiP-AUC at each point in time by:
--   1. Taking all publications with pub_year <= state_year
--   2. Looking up each pub's cumulative_citations as of state_year → citation percentile
--      (via dist_publication_citations_temporal, L2)
--   3. Counting how many papers the author had → num_papers_percentile
--      (via dist_author_metrics_temporal, L4)
--   4. Running the same trapezoidal integration → pip_auc_score
--
-- Depends on: intermediate_author_publication_state_temporal (L2),
--   dist_publication_citations_temporal (L2), stats_author_metrics_temporal_view (L3),
--   dist_author_metrics_temporal (L4).
--
-- This is the most expensive view in the system. It should ALWAYS be materialized
-- (daily, like stats_author_metrics_temporal), never queried live.
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
  -- and compute the publication rank (X-axis position before interpolation)
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

  -- Look up num_papers_percentile from dist_author_metrics_temporal.
  -- This maps (year_of_first_pub, state_year, total_publications) → percentile.
  NumPapersPercentile AS (
    SELECT
      year_of_first_pub,
      state_year,
      metric_value AS total_publications,
      percentile AS total_publications_percentile
    FROM `scholar-version2.statistics.dist_author_metrics_temporal`
    WHERE metric_name = 'total_publications'
  ),

  -- Interpolate num_papers_percentile for each publication (same 6-CTE pattern
  -- as stats_author_publication_pip_inputs_current)
  Distances AS (
    SELECT
      rp.*,
      npp.total_publications_percentile,
      rp.publication_rank - npp.total_publications AS distance,
      CASE
        WHEN rp.publication_rank - npp.total_publications >= 0 THEN 'positive'
        ELSE 'negative'
      END AS distance_type
    FROM RankedPubs rp
    JOIN NumPapersPercentile npp
      ON rp.year_of_first_pub = npp.year_of_first_pub
     AND rp.state_year = npp.state_year
  ),
  RankedDistances AS (
    SELECT *,
      ROW_NUMBER() OVER(PARTITION BY scholar_id, state_year, author_pub_id, distance_type ORDER BY ABS(distance)) AS rank_distance
    FROM Distances
  ),
  FilteredDistances AS (
    SELECT * FROM RankedDistances WHERE rank_distance = 1
  ),
  AggregatedDistances AS (
    SELECT
      scholar_id, state_year, author_pub_id,
      MAX(CASE WHEN distance_type = 'positive' THEN total_publications_percentile END)
        OVER(PARTITION BY scholar_id, state_year, author_pub_id) AS positive_percentile,
      MAX(CASE WHEN distance_type = 'negative' THEN total_publications_percentile END)
        OVER(PARTITION BY scholar_id, state_year, author_pub_id) AS negative_percentile,
      MAX(CASE WHEN distance_type = 'positive' THEN ABS(distance) END)
        OVER(PARTITION BY scholar_id, state_year, author_pub_id) AS positive_distance,
      MAX(CASE WHEN distance_type = 'negative' THEN ABS(distance) END)
        OVER(PARTITION BY scholar_id, state_year, author_pub_id) AS negative_distance
    FROM FilteredDistances
  ),
  InterpolatedPubs AS (
    SELECT DISTINCT
      fd.scholar_id,
      fd.state_year,
      fd.author_pub_id,
      fd.citation_percentile AS num_citations_percentile,
      fd.publication_rank,
      fd.year_of_first_pub,
      CASE
        WHEN ad.positive_percentile IS NULL THEN 0.0
        WHEN ad.negative_percentile IS NULL THEN 1.0
        ELSE (ad.negative_percentile * ad.positive_distance + ad.positive_percentile * ad.negative_distance)
             / (ad.positive_distance + ad.negative_distance)
      END AS num_papers_percentile
    FROM FilteredDistances fd
    JOIN AggregatedDistances ad
      ON fd.scholar_id = ad.scholar_id
     AND fd.state_year = ad.state_year
     AND fd.author_pub_id = ad.author_pub_id
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
