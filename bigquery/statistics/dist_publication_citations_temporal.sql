-- Level 2: Publication citation percentile distribution for temporal data.
--
-- Stores approximate quantile breakpoints per (partition_keys, metric_name).
-- Four metrics with two partitioning schemes:
--   - pub_year_yearly_citations:       partitioned by (pub_year, citation_year)
--   - pub_year_cumulative_citations:   partitioned by (pub_year, citation_year)
--   - age_yearly_citations:            partitioned by (age)
--   - age_cumulative_citations:        partitioned by (age)
--
-- Uses APPROX_QUANTILES(metric, 1000) instead of exact PERCENT_RANK() to avoid
-- OOM on ~2B rows. Produces 1000 breakpoints per partition with ~0.1% accuracy.
-- Downstream consumers do floor lookups: MAX(percentile) WHERE metric_value <= value.
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by ranked_publication_citations_temporal and
-- stats_author_pip_scores_temporal for fast floor lookups.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_publication_citations_temporal`
CLUSTER BY metric_name, pub_year
AS
WITH
  TemporalData AS (
    SELECT pub_year, age, citation_year, yearly_citations, cumulative_citations
    FROM `scholar-version2.statistics.stats_publication_citations_temporal`
  )

-- Section 1: yearly_citations quantiles partitioned by (pub_year, citation_year)
SELECT DISTINCT * FROM (
  SELECT pub_year, citation_year, CAST(NULL AS INT64) AS age,
         'pub_year_yearly_citations' AS metric_name,
         value AS metric_value, offset / 1000.0 AS percentile
  FROM (
    SELECT pub_year, citation_year,
           APPROX_QUANTILES(yearly_citations, 1000) AS quantiles
    FROM TemporalData GROUP BY pub_year, citation_year
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset

  UNION ALL

  -- Section 2: cumulative_citations quantiles partitioned by (pub_year, citation_year)
  SELECT pub_year, citation_year, CAST(NULL AS INT64) AS age,
         'pub_year_cumulative_citations' AS metric_name,
         value AS metric_value, offset / 1000.0 AS percentile
  FROM (
    SELECT pub_year, citation_year,
           APPROX_QUANTILES(cumulative_citations, 1000) AS quantiles
    FROM TemporalData GROUP BY pub_year, citation_year
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset

  UNION ALL

  -- Section 3: yearly_citations quantiles partitioned by (age)
  SELECT CAST(NULL AS INT64) AS pub_year, CAST(NULL AS INT64) AS citation_year, age,
         'age_yearly_citations' AS metric_name,
         value AS metric_value, offset / 1000.0 AS percentile
  FROM (
    SELECT age,
           APPROX_QUANTILES(yearly_citations, 1000) AS quantiles
    FROM TemporalData GROUP BY age
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset

  UNION ALL

  -- Section 4: cumulative_citations quantiles partitioned by (age)
  SELECT CAST(NULL AS INT64) AS pub_year, CAST(NULL AS INT64) AS citation_year, age,
         'age_cumulative_citations' AS metric_name,
         value AS metric_value, offset / 1000.0 AS percentile
  FROM (
    SELECT age,
           APPROX_QUANTILES(cumulative_citations, 1000) AS quantiles
    FROM TemporalData GROUP BY age
  ), UNNEST(quantiles) AS value WITH OFFSET AS offset
);
