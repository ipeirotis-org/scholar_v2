CREATE OR REPLACE VIEW `scholar-version2.statistics.author_metrics_temporal` AS

WITH PublicationState AS (
  -- Base view created in Step 2, providing yearly state for each publication
  SELECT * FROM `scholar-version2.statistics.author_publication_state_temporal`
),

FirstPubYear AS (
  -- Determine the first publication year for each author
  SELECT
    scholar_id,
    MIN(pub_year) as year_of_first_pub
  FROM PublicationState
  GROUP BY scholar_id
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
    -- Sum cumulative citations across all publications for that author *at that year*
    SUM(ps.cumulative_citations_at_state_year) AS total_citations,
    -- Count publications with at least 10 cumulative citations at that year
    COUNT(DISTINCT IF(ps.cumulative_citations_at_state_year >= 10, ps.author_pub_id, NULL)) AS i10_index,
    -- Calculate total recent citations for the author in the 5-year window ending at state_year
    (SELECT SUM(ps_inner.yearly_citations_at_state_year)
     FROM PublicationState ps_inner
     WHERE ps_inner.scholar_id = ps.scholar_id
       AND ps_inner.state_year BETWEEN ps.state_year - 4 AND ps.state_year
    ) AS total_recent_citations_5y,
    -- Count publications with at least 10 citations in the last 5 years
    COUNT(DISTINCT IF(prc.citations_last_5_years >= 10, prc.author_pub_id, NULL)) AS i10_index_5y
  FROM PublicationState ps
  -- Join with recent citation calculations to get citations_last_5_years for the i10_index_5y condition
  LEFT JOIN PublicationRecentCitations prc ON ps.scholar_id = prc.scholar_id AND ps.author_pub_id = prc.author_pub_id AND ps.state_year = prc.state_year
  GROUP BY
    ps.scholar_id, ps.state_year
)

-- Final SELECT combining all calculated metrics
SELECT
  am.scholar_id,
  am.state_year,
  -- Include year_of_first_pub for potential percentile grouping later
  fpy.year_of_first_pub,
  am.total_publications,
  am.total_citations,
  am.total_recent_citations_5y,
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

ORDER BY
  am.scholar_id,
  am.state_year;