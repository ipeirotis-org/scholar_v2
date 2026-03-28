-- Materialize percentile distribution tables and full-table snapshots.
--
-- EXECUTION ORDER IS CRITICAL — each step depends on the previous ones.
--
-- Architecture (3-tier):
--   Tier 1 views (stats_*) — compute raw metric values only, no percentiles.
--   Tier 2 tables (dist_*) — small, store (value → percentile) mappings.
--     These are the only place PERCENT_RANK() runs. Output is compact because
--     DISTINCT collapses tied values that share the same rank.
--     These are EXPENSIVE to compute and change slowly. Refreshed quarterly
--     by bigquery-materialize-distributions.yml (or manually via this script).
--   Tier 3 views (ranked_*) — join Tier 1 + Tier 2 for cheap percentile lookups.
--     Per-author queries are cheap: read one author's data + JOIN small dist table.
--   Full-table snapshots (_table suffix) — only for get_all_authors_stats() which
--     needs to scan all authors at once for the ranking/listing UI.
--     Refreshed daily by bigquery-materialize.yml.
--
-- Per-author app queries (author profile page) use the ranked_* VIEWS directly.
-- All-authors list queries use the _table snapshots.
--
-- Data source: Semantic Scholar bulk datasets (s2_data.*).
--
-- Usage (full refresh — distribution tables + snapshots):
--   bq query --project_id=scholar-version2 --use_legacy_sql=false < bigquery/statistics/materialize_stats.sql

-- Step 0: One-time migration — drop the old BigQuery MATERIALIZED VIEW if it
-- still exists (replaced by this scripted approach).
DROP MATERIALIZED VIEW IF EXISTS `scholar-version2.statistics.stats_author_metrics_temporal`;

-- ── Distribution tables (Tier 2) ───────────────────────────────────────────

-- Step 1: Publication citation percentile distribution.
-- Source: s2_data.papers (one row per paper).
-- Computes PERCENT_RANK by pub_year, stores distinct
-- (pub_year, num_citations) → percentile pairs. Enables fast per-publication
-- percentile lookups in ranked_publication_current without live PERCENT_RANK.
CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_publication_citations`
CLUSTER BY pub_year
AS SELECT * FROM (
  SELECT DISTINCT
    year AS pub_year,
    citationcount AS num_citations,
    PERCENT_RANK() OVER(PARTITION BY year ORDER BY citationcount ASC) AS num_citations_percentile
  FROM `scholar-version2.s2_data.papers`
  WHERE year IS NOT NULL
    AND year > 1950
    AND year <= EXTRACT(YEAR FROM CURRENT_DATE())
    AND citationcount > 0
);

-- Step 2: Author metric percentile distributions.
-- Source: s2_data.authors + s2_data.author_paper_stats.
-- Computes PERCENT_RANK for 5 metrics (hindex5y, citedby5y, i10index5y dropped — not in S2)
-- partitioned by year_of_first_pub cohort. Stored in normalized
-- (year_of_first_pub, metric_name, metric_value, percentile) format.
-- Includes total_publications_with_citations so stats_author_publication_pip_inputs_current
-- can read num_papers distribution from here instead of scanning all authors.
CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_author_metrics`
CLUSTER BY year_of_first_pub, metric_name
AS SELECT * FROM (
  WITH
    CombinedData AS (
      SELECT
        a.authorid AS scholar_id,
        a.hindex,
        a.citationcount AS citedby,
        COALESCE(ps.i10_index, 0) AS i10index,
        COALESCE(ps.total_publications, 0) AS total_publications,
        COALESCE(ps.total_publications_with_citations, 0) AS total_publications_with_citations,
        ps.year_of_first_pub
      FROM `scholar-version2.s2_data.authors` a
      JOIN `scholar-version2.s2_data.author_paper_stats` ps ON a.authorid = ps.authorid
      WHERE a.authorid IS NOT NULL
        AND ps.year_of_first_pub IS NOT NULL
    ),
    WithPercentiles AS (
      SELECT *,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY hindex ASC)                            AS hindex_pct,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY citedby ASC)                           AS citedby_pct,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY i10index ASC)                          AS i10index_pct,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY total_publications ASC)                AS total_publications_pct,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY total_publications_with_citations ASC) AS total_publications_with_citations_pct
      FROM CombinedData
    )
  SELECT DISTINCT year_of_first_pub, 'hindex'                           AS metric_name, hindex                           AS metric_value, hindex_pct                           AS percentile FROM WithPercentiles
  UNION ALL
  SELECT DISTINCT year_of_first_pub, 'citedby',                           citedby,                           citedby_pct                           FROM WithPercentiles
  UNION ALL
  SELECT DISTINCT year_of_first_pub, 'i10index',                          i10index,                          i10index_pct                          FROM WithPercentiles
  UNION ALL
  SELECT DISTINCT year_of_first_pub, 'total_publications',                total_publications,                total_publications_pct                FROM WithPercentiles
  UNION ALL
  SELECT DISTINCT year_of_first_pub, 'total_publications_with_citations', total_publications_with_citations, total_publications_with_citations_pct FROM WithPercentiles
);

-- Step 3: Temporal publication citation distributions.
-- Reads temporal citation data, computes PERCENT_RANK for 4 metric/partition
-- combinations. Used by ranked_publication_citations_temporal.
CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_publication_citations_temporal`
CLUSTER BY metric_name, pub_year
AS
WITH
  TemporalData AS (
    SELECT pub_year, age, citation_year, yearly_citations, cumulative_citations
    FROM `scholar-version2.statistics.stats_publication_citations_temporal`
  )
SELECT DISTINCT pub_year, citation_year, CAST(NULL AS INT64) AS age,
  'pub_year_yearly_citations' AS metric_name, yearly_citations AS metric_value,
  PERCENT_RANK() OVER(PARTITION BY pub_year, citation_year ORDER BY yearly_citations ASC) AS percentile
FROM TemporalData
UNION ALL
SELECT DISTINCT pub_year, citation_year, CAST(NULL AS INT64) AS age,
  'pub_year_cumulative_citations' AS metric_name, cumulative_citations AS metric_value,
  PERCENT_RANK() OVER(PARTITION BY pub_year, citation_year ORDER BY cumulative_citations ASC) AS percentile
FROM TemporalData
UNION ALL
SELECT DISTINCT CAST(NULL AS INT64) AS pub_year, CAST(NULL AS INT64) AS citation_year, age,
  'age_yearly_citations' AS metric_name, yearly_citations AS metric_value,
  PERCENT_RANK() OVER(PARTITION BY age ORDER BY yearly_citations ASC) AS percentile
FROM TemporalData
UNION ALL
SELECT DISTINCT CAST(NULL AS INT64) AS pub_year, CAST(NULL AS INT64) AS citation_year, age,
  'age_cumulative_citations' AS metric_name, cumulative_citations AS metric_value,
  PERCENT_RANK() OVER(PARTITION BY age ORDER BY cumulative_citations ASC) AS percentile
FROM TemporalData;

-- Step 4: Temporal author metric distributions.
-- Reads temporal author metrics, computes PERCENT_RANK for 7 metrics
-- partitioned by (year_of_first_pub, state_year). Used by ranked_author_metrics_temporal.
CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_author_metrics_temporal`
CLUSTER BY year_of_first_pub, state_year, metric_name
AS
WITH
  TemporalData AS (
    SELECT year_of_first_pub, state_year,
      total_publications, total_citations, total_recent_citations_5y,
      h_index, h_index_5y, i10_index, i10_index_5y
    FROM `scholar-version2.statistics.stats_author_metrics_temporal_view`
    WHERE year_of_first_pub IS NOT NULL
  ),
  WithPercentiles AS (
    SELECT *,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY total_publications ASC)       AS total_publications_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY total_citations ASC)          AS total_citations_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY total_recent_citations_5y ASC) AS total_recent_citations_5y_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY h_index ASC)                  AS h_index_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY h_index_5y ASC)               AS h_index_5y_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY i10_index ASC)                AS i10_index_pct,
      PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY i10_index_5y ASC)             AS i10_index_5y_pct
    FROM TemporalData
  )
SELECT DISTINCT year_of_first_pub, state_year, 'total_publications'       AS metric_name, total_publications       AS metric_value, total_publications_pct       AS percentile FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, state_year, 'total_citations',          total_citations,          total_citations_pct          FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, state_year, 'total_recent_citations_5y', total_recent_citations_5y, total_recent_citations_5y_pct FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, state_year, 'h_index',                  h_index,                  h_index_pct                  FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, state_year, 'h_index_5y',               h_index_5y,               h_index_5y_pct               FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, state_year, 'i10_index',                i10_index,                i10_index_pct                FROM WithPercentiles
UNION ALL
SELECT DISTINCT year_of_first_pub, state_year, 'i10_index_5y',             i10_index_5y,             i10_index_5y_pct             FROM WithPercentiles;

-- Step 5: PiP-AUC score percentile distribution.
-- Now that dist_publication_citations and dist_author_metrics exist, the views
-- ranked_publication_current and stats_author_current are fast. This makes computing
-- pip scores for all authors much cheaper than before (no chained PERCENT_RANK scans).
CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_pip_auc_scores`
CLUSTER BY year_of_first_pub
AS SELECT * FROM (
  WITH
    RankedPublications AS (
      SELECT
        scholar_id,
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
      SELECT scholar_id, ROUND(SUM(area), 4) AS pip_auc_score
      FROM TrapezoidAreas
      GROUP BY scholar_id
    ),
    AllScores AS (
      SELECT A.scholar_id, AuthStats.year_of_first_pub, A.pip_auc_score
      FROM AUC A
      JOIN `scholar-version2.statistics.stats_author_current` AuthStats ON A.scholar_id = AuthStats.scholar_id
    )
  SELECT DISTINCT
    year_of_first_pub,
    pip_auc_score,
    PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY pip_auc_score ASC) AS percentile
  FROM AllScores
);

-- Step 6: Temporal PiP-AUC score percentile distribution.
-- Depends on Steps 3+4 (temporal PiP view uses temporal dist lookups).
CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_pip_auc_scores_temporal`
CLUSTER BY year_of_first_pub, state_year
AS
SELECT DISTINCT
  year_of_first_pub,
  state_year,
  pip_auc_score,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub, state_year ORDER BY pip_auc_score ASC) AS percentile
FROM `scholar-version2.statistics.stats_author_pip_scores_temporal_view`
WHERE year_of_first_pub IS NOT NULL;

-- ── Full-table snapshots (for all-authors list queries and temporal) ─────────

-- Step 7: Materialize all-author stats table (with percentiles from ranked view).
-- Used only by get_all_authors_stats() for the full ranking/list view.
-- Per-author profile queries use ranked_author_current VIEW directly (cheap).
CREATE OR REPLACE TABLE `scholar-version2.statistics.ranked_author_current_table`
CLUSTER BY scholar_id, year_of_first_pub
AS SELECT * FROM `scholar-version2.statistics.ranked_author_current`;

-- Step 8: Materialize all-author PiP scores table (with percentile from ranked view).
-- Used only by get_all_authors_stats() alongside ranked_author_current_table.
-- Per-author profile queries use ranked_author_pip_scores_current VIEW directly (cheap).
CREATE OR REPLACE TABLE `scholar-version2.statistics.ranked_author_pip_scores_current_table`
CLUSTER BY scholar_id
AS SELECT * FROM `scholar-version2.statistics.ranked_author_pip_scores_current`;

-- Step 9: Temporal author metrics table (with percentiles from ranked view).
-- Full temporal history — always needs to be materialized as a table since it
-- spans all authors × all historical years.
CREATE OR REPLACE TABLE `scholar-version2.statistics.ranked_author_metrics_temporal_table`
CLUSTER BY scholar_id, state_year
AS SELECT * FROM `scholar-version2.statistics.ranked_author_metrics_temporal`;

-- Step 10: Temporal PiP-AUC scores table (with percentile from ranked view).
-- Like temporal metrics, this must be materialized — running the full PiP pipeline
-- for all authors × all years is the most expensive computation in the system.
CREATE OR REPLACE TABLE `scholar-version2.statistics.ranked_author_pip_scores_temporal_table`
CLUSTER BY scholar_id, state_year
AS SELECT * FROM `scholar-version2.statistics.ranked_author_pip_scores_temporal`;
