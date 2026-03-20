-- Level 2: Publication citation percentile distribution for temporal data.
--
-- Stores distinct (partition_keys, metric_name, metric_value) → percentile mappings.
-- Four metrics with two partitioning schemes:
--   - pub_year_yearly_citations:       partitioned by (pub_year, citation_year)
--   - pub_year_cumulative_citations:   partitioned by (pub_year, citation_year)
--   - age_yearly_citations:            partitioned by (age)
--   - age_cumulative_citations:        partitioned by (age)
--
-- PERCENT_RANK() is computed over ALL rows (preserving frequency), then DISTINCT
-- collapses tied values since they all receive the same rank.
--
-- Refreshed quarterly by bigquery-materialize-distributions.yml.
-- Used by ranked_publication_citations_temporal to do fast floor lookups.

CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_publication_citations_temporal`
CLUSTER BY metric_name, pub_year
AS
WITH
  TemporalData AS (
    SELECT pub_year, age, citation_year, yearly_citations, cumulative_citations
    FROM `scholar-version2.statistics.stats_publication_citations_temporal`
  )

-- Section 1: yearly_citations percentile partitioned by (pub_year, citation_year)
SELECT DISTINCT
  pub_year,
  citation_year,
  CAST(NULL AS INT64) AS age,
  'pub_year_yearly_citations' AS metric_name,
  yearly_citations AS metric_value,
  PERCENT_RANK() OVER(PARTITION BY pub_year, citation_year ORDER BY yearly_citations ASC) AS percentile
FROM TemporalData

UNION ALL

-- Section 2: cumulative_citations percentile partitioned by (pub_year, citation_year)
SELECT DISTINCT
  pub_year,
  citation_year,
  CAST(NULL AS INT64) AS age,
  'pub_year_cumulative_citations' AS metric_name,
  cumulative_citations AS metric_value,
  PERCENT_RANK() OVER(PARTITION BY pub_year, citation_year ORDER BY cumulative_citations ASC) AS percentile
FROM TemporalData

UNION ALL

-- Section 3: yearly_citations percentile partitioned by (age)
SELECT DISTINCT
  CAST(NULL AS INT64) AS pub_year,
  CAST(NULL AS INT64) AS citation_year,
  age,
  'age_yearly_citations' AS metric_name,
  yearly_citations AS metric_value,
  PERCENT_RANK() OVER(PARTITION BY age ORDER BY yearly_citations ASC) AS percentile
FROM TemporalData

UNION ALL

-- Section 4: cumulative_citations percentile partitioned by (age)
SELECT DISTINCT
  CAST(NULL AS INT64) AS pub_year,
  CAST(NULL AS INT64) AS citation_year,
  age,
  'age_cumulative_citations' AS metric_name,
  cumulative_citations AS metric_value,
  PERCENT_RANK() OVER(PARTITION BY age ORDER BY cumulative_citations ASC) AS percentile
FROM TemporalData;
