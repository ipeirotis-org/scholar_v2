CREATE OR REPLACE MATERIALIZED VIEW `scholar-version2.statistics.stats_author_metrics_temporal` 
   OPTIONS (
  enable_refresh = true,
  refresh_interval_minutes = 24 * 60,
  max_staleness = INTERVAL "1" DAY,
  allow_non_incremental_definition = true
)
AS


WITH PublicationState AS (
  -- Use the intermediate view providing yearly state for each publication linked to its author
  -- *** ASSUMES this view uses stats_publication_citations_temporal correctly ***
  SELECT * FROM `scholar-version2.statistics.intermediate_author_publication_state_temporal`
),

FirstPubYear AS (
  -- Determine the first publication year for each author
  -- Need to join with publication stats to filter for pubs with citations > 0
  SELECT
    ps.scholar_id,
    MIN(ps.pub_year) as year_of_first_pub
  FROM PublicationState ps
  JOIN `scholar-version2.statistics.stats_publication_current` spc -- Join to get current citation count
      ON ps.author_pub_id = spc.author_pub_id
  WHERE spc.num_citations > 0 -- Only consider pubs with at least one citation ever
  GROUP BY ps.scholar_id
),

RankedPublications AS (
  -- Rank publications within each author-year group based on cumulative citations for H-index calculation
  SELECT
    scholar_id,
    state_year,
    author_pub_id,
    cumulative_citations_at_state_year,
    -- Rank papers by cumulative citations descendingly for the H-index calculation
    ROW_NUMBER() OVER (PARTITION BY scholar_id, state_year ORDER BY cumulative_citations_at_state_year DESC) as h_rank
  FROM PublicationState
),

HIndexCalculated AS (
  -- Calculate the standard H-index for each author-year
  -- H-index is the maximum value h such that the author has published h papers that have each been cited at least h times.
  SELECT
    scholar_id,
    state_year,
    -- If no papers have citations > 0 in a year, MAX returns NULL. COALESCE handles this later.
    MAX(LEAST(h_rank, cumulative_citations_at_state_year)) AS h_index
  FROM RankedPublications
  WHERE cumulative_citations_at_state_year > 0 -- Only consider papers with citations for H-index
  GROUP BY scholar_id, state_year
),

PublicationRecentCitations AS (
 -- Calculate the sum of citations over the preceding 5 years for each publication, ending at state_year
 SELECT
    scholar_id,
    author_pub_id,
    state_year,
    -- Sum the yearly citations over a 5-year window (current year + 4 preceding)
    SUM(yearly_citations_at_state_year) OVER (
        PARTITION BY scholar_id, author_pub_id
        ORDER BY state_year
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS citations_last_5_years
 FROM PublicationState
),

RankedRecentPublications AS (
  -- Rank publications based on recent (last 5 years) citations for H-index5y calculation
  SELECT
    prc.scholar_id,
    prc.state_year,
    prc.author_pub_id,
    prc.citations_last_5_years,
    -- Rank papers by recent citations descendingly for the H-index5y calculation
    ROW_NUMBER() OVER (PARTITION BY prc.scholar_id, prc.state_year ORDER BY prc.citations_last_5_years DESC) as h5y_rank
  FROM PublicationRecentCitations prc
  WHERE prc.citations_last_5_years > 0 -- Only consider papers with recent citations
),

HIndex5yCalculated AS (
  -- Calculate H-index (last 5 years) for each author-year
  -- H-index5y is the maximum value h such that the author has published h papers that have each received at least h citations in the last 5 years.
  SELECT
    scholar_id,
    state_year,
    -- If no papers have recent citations > 0 in a year, MAX returns NULL. COALESCE handles this later.
    MAX(LEAST(h5y_rank, citations_last_5_years)) AS h_index_5y
  FROM RankedRecentPublications
  GROUP BY scholar_id, state_year
),

AggregatedMetrics AS (
  -- Aggregate basic metrics per author-year using the base publication state
  SELECT
    ps.scholar_id,
    ps.state_year,
    COUNT(DISTINCT ps.author_pub_id) AS total_publications,
    -- Sum cumulative citations across all publications active for that author *at that state_year*
    SUM(ps.cumulative_citations_at_state_year) AS total_citations,
    -- Count publications with at least 10 cumulative citations at that year
    COUNT(DISTINCT IF(ps.cumulative_citations_at_state_year >= 10, ps.author_pub_id, NULL)) AS i10_index,
    -- Calculate total recent citations for the author in the 5-year window ending at state_year
    -- This subquery sums *all* yearly citations for the author in the window
    (SELECT SUM(ps_inner.yearly_citations_at_state_year)
     FROM PublicationState ps_inner
     WHERE ps_inner.scholar_id = ps.scholar_id
       AND ps_inner.state_year BETWEEN ps.state_year - 4 AND ps.state_year
    ) AS total_recent_citations_5y,
    -- Count publications with at least 10 citations in the last 5 years
    COUNT(DISTINCT IF(prc.citations_last_5_years >= 10, prc.author_pub_id, NULL)) AS i10_index_5y
  FROM PublicationState ps
  -- Join with recent citation calculations needed *only* for i10_index_5y condition
  LEFT JOIN PublicationRecentCitations prc ON ps.scholar_id = prc.scholar_id AND ps.author_pub_id = prc.author_pub_id AND ps.state_year = prc.state_year
  GROUP BY
    ps.scholar_id, ps.state_year
),

-- This CTE combines the aggregated metrics, effectively replacing the need for the separate view
TemporalMetrics AS (
  SELECT
    am.scholar_id,
    am.state_year,
    -- Include year_of_first_pub for potential percentile grouping later
    fpy.year_of_first_pub,
    am.total_publications,
    am.total_citations,
    -- Default total recent citations to 0 if the subquery returns NULL (e.g., for early years)
    COALESCE(am.total_recent_citations_5y, 0) as total_recent_citations_5y,
    -- Use COALESCE to return 0 instead of NULL if H-index wasn't calculated (e.g., no cited papers)
    COALESCE(hic.h_index, 0) AS h_index,
    -- Use COALESCE for 5y metrics as they might be NULL if there are no recent citations
    COALESCE(h5yc.h_index_5y, 0) AS h_index_5y,
    COALESCE(am.i10_index, 0) AS i10_index,
    COALESCE(am.i10_index_5y, 0) AS i10_index_5y

  FROM AggregatedMetrics am
  LEFT JOIN FirstPubYear fpy ON am.scholar_id = fpy.scholar_id
  LEFT JOIN HIndexCalculated hic ON am.scholar_id = hic.scholar_id AND am.state_year = hic.state_year
  LEFT JOIN HIndex5yCalculated h5yc ON am.scholar_id = h5yc.scholar_id AND am.state_year = h5yc.state_year
)

-- Final SELECT calculating percentiles based on the TemporalMetrics CTE defined above
SELECT
  scholar_id,
  state_year,
  year_of_first_pub,

  -- Include the original metric values for reference
  total_publications,
  total_citations,
  total_recent_citations_5y,
  h_index,
  h_index_5y,
  i10_index,
  i10_index_5y,

  -- Calculate Percentiles for each metric using PERCENT_RANK()
  -- Partitioning by year_of_first_pub and state_year compares authors against their peers
  -- (same start year) at the same point in historical time.
  -- Ordering by the metric ASC means higher values get ranks closer to 1.0.

  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY total_publications ASC) AS total_publications_percentile,
  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY total_citations ASC) AS total_citations_percentile,
  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY total_recent_citations_5y ASC) AS total_recent_citations_5y_percentile,
  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY h_index ASC) AS h_index_percentile,
  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY h_index_5y ASC) AS h_index_5y_percentile,
  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY i10_index ASC) AS i10_index_percentile,
  PERCENT_RANK() OVER (PARTITION BY year_of_first_pub, state_year ORDER BY i10_index_5y ASC) AS i10_index_5y_percentile

FROM TemporalMetrics;