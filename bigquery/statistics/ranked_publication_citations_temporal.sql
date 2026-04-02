CREATE OR REPLACE VIEW `scholar-version2.statistics.ranked_publication_citations_temporal` AS
-- Level 3: Temporal publication citation stats enriched with 4 percentile columns.
-- Uses RANGE_BUCKET + pre-aggregated arrays for O(log n) floor lookups instead of
-- correlated scalar subqueries (which are slow on 2B rows).
-- Note: scholar_id not included (temporal citation data is per-paper in S2).
WITH
  -- Pre-aggregate dist breakpoints into sorted arrays for pub_year-keyed metrics
  PubYearArrays AS (
    SELECT
      metric_name,
      pub_year,
      citation_year,
      ARRAY_AGG(metric_value ORDER BY metric_value) AS values_arr,
      ARRAY_AGG(percentile ORDER BY metric_value) AS pcts_arr
    FROM `scholar-version2.statistics.dist_publication_citations_temporal`
    WHERE metric_name IN ('pub_year_yearly_citations', 'pub_year_cumulative_citations')
    GROUP BY metric_name, pub_year, citation_year
  ),
  PubYearYearly AS (
    SELECT pub_year, citation_year, values_arr, pcts_arr
    FROM PubYearArrays WHERE metric_name = 'pub_year_yearly_citations'
  ),
  PubYearCumulative AS (
    SELECT pub_year, citation_year, values_arr, pcts_arr
    FROM PubYearArrays WHERE metric_name = 'pub_year_cumulative_citations'
  ),
  -- Pre-aggregate dist breakpoints for age-keyed metrics
  AgeArrays AS (
    SELECT
      metric_name,
      age,
      ARRAY_AGG(metric_value ORDER BY metric_value) AS values_arr,
      ARRAY_AGG(percentile ORDER BY metric_value) AS pcts_arr
    FROM `scholar-version2.statistics.dist_publication_citations_temporal`
    WHERE metric_name IN ('age_yearly_citations', 'age_cumulative_citations')
    GROUP BY metric_name, age
  ),
  AgeYearly AS (
    SELECT age, values_arr, pcts_arr
    FROM AgeArrays WHERE metric_name = 'age_yearly_citations'
  ),
  AgeCumulative AS (
    SELECT age, values_arr, pcts_arr
    FROM AgeArrays WHERE metric_name = 'age_cumulative_citations'
  )
SELECT
  b.author_pub_id,
  b.pub_year,
  b.age,
  b.citation_year,
  b.yearly_citations,
  b.cumulative_citations,
  COALESCE(
    pyy.pcts_arr[SAFE_ORDINAL(RANGE_BUCKET(b.yearly_citations, pyy.values_arr))],
    0.0) AS perc_pub_year_yearly_citations,
  COALESCE(
    pyc.pcts_arr[SAFE_ORDINAL(RANGE_BUCKET(b.cumulative_citations, pyc.values_arr))],
    0.0) AS perc_pub_year_cumulative_citations,
  COALESCE(
    ay.pcts_arr[SAFE_ORDINAL(RANGE_BUCKET(b.yearly_citations, ay.values_arr))],
    0.0) AS perc_age_yearly_citations,
  COALESCE(
    ac.pcts_arr[SAFE_ORDINAL(RANGE_BUCKET(b.cumulative_citations, ac.values_arr))],
    0.0) AS perc_age_cumulative_citations
FROM `scholar-version2.statistics.stats_publication_citations_temporal` b
LEFT JOIN PubYearYearly pyy
  ON pyy.pub_year = b.pub_year AND pyy.citation_year = b.citation_year
LEFT JOIN PubYearCumulative pyc
  ON pyc.pub_year = b.pub_year AND pyc.citation_year = b.citation_year
LEFT JOIN AgeYearly ay ON ay.age = b.age
LEFT JOIN AgeCumulative ac ON ac.age = b.age;
