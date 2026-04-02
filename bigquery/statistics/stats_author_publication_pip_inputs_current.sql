CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_author_publication_pip_inputs_current` AS
-- Computes num_papers_percentile (the X-axis of the PiP chart) for each publication
-- via interpolation of the author's publication-count percentile.
--
-- Uses array-based lookup: pre-aggregates dist breakpoints into sorted arrays
-- per year_of_first_pub, then uses RANGE_BUCKET to find the floor index in O(log n).
-- A single equi-join on year_of_first_pub broadcasts the small arrays (~75 cohorts)
-- to all publications. No range joins, no correlated subqueries.
--
-- Uses 'active_authors' benchmark for meaningful percentile differentiation.
WITH
  RankedPublications AS (
    SELECT
      bap.scholar_id,
      p.author_pub_id,
      p.num_citations,
      p.num_citations_percentile,
      ROW_NUMBER() OVER(PARTITION BY bap.scholar_id ORDER BY p.num_citations_percentile DESC) AS publication_rank,
      a.year_of_first_pub,
      a.total_publications_with_citations
    FROM `scholar-version2.statistics.ranked_publication_current` p
    JOIN `scholar-version2.statistics.base_author_publications` bap
      ON p.author_pub_id = bap.author_pub_id
    JOIN `scholar-version2.statistics.stats_author_current` a
      ON bap.scholar_id = a.scholar_id
  ),
  -- Pre-aggregate dist breakpoints into sorted arrays per cohort.
  -- Each cohort gets ~1000 (metric_value, percentile) breakpoints sorted by metric_value.
  DistArrays AS (
    SELECT
      year_of_first_pub,
      ARRAY_AGG(metric_value ORDER BY metric_value) AS values_arr,
      ARRAY_AGG(percentile ORDER BY metric_value) AS pcts_arr
    FROM `scholar-version2.statistics.dist_author_metrics`
    WHERE benchmark = 'active_authors'
      AND metric_name = 'total_publications_with_citations'
    GROUP BY year_of_first_pub
  ),
  -- Join publications to their cohort's breakpoint arrays (equi-join, broadcast).
  -- Use RANGE_BUCKET to find the floor index in O(log n).
  Interpolated AS (
    SELECT
      rp.scholar_id,
      rp.author_pub_id,
      rp.num_citations,
      rp.num_citations_percentile,
      rp.publication_rank,
      -- RANGE_BUCKET returns the index of the first element > publication_rank,
      -- so (bucket_idx - 1) is the floor index, bucket_idx is the ceiling index.
      RANGE_BUCKET(rp.publication_rank, da.values_arr) AS bucket_idx,
      da.values_arr,
      da.pcts_arr
    FROM RankedPublications rp
    JOIN DistArrays da ON rp.year_of_first_pub = da.year_of_first_pub
  )
SELECT
  scholar_id,
  author_pub_id,
  num_citations,
  num_citations_percentile,
  publication_rank,
  CASE
    -- Below all breakpoints
    WHEN bucket_idx = 0 THEN 0.0
    -- Above all breakpoints
    WHEN bucket_idx >= ARRAY_LENGTH(values_arr) THEN 1.0
    -- Exact match on floor (floor == ceiling value)
    WHEN values_arr[SAFE_ORDINAL(bucket_idx)] = values_arr[SAFE_ORDINAL(bucket_idx + 1)]
      THEN pcts_arr[SAFE_ORDINAL(bucket_idx)]
    -- Linear interpolation between floor and ceiling
    ELSE (
      pcts_arr[SAFE_ORDINAL(bucket_idx + 1)] * (publication_rank - values_arr[SAFE_ORDINAL(bucket_idx)])
      + pcts_arr[SAFE_ORDINAL(bucket_idx)] * (values_arr[SAFE_ORDINAL(bucket_idx + 1)] - publication_rank)
    ) / (values_arr[SAFE_ORDINAL(bucket_idx + 1)] - values_arr[SAFE_ORDINAL(bucket_idx)])
  END AS num_papers_percentile
FROM Interpolated;
