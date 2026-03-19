-- Materialize percentile distribution tables and full-table snapshots.
--
-- EXECUTION ORDER IS CRITICAL — each step depends on the previous ones.
--
-- Architecture:
--   Distribution tables (dist_*) — small, store (value → percentile) mappings.
--     These are the only place PERCENT_RANK() runs. Output is compact because
--     DISTINCT collapses tied values that share the same rank.
--     These are EXPENSIVE to compute and change slowly. Refreshed quarterly
--     by bigquery-materialize-distributions.yml (or manually via this script).
--   View definitions (stats_*) — reference distribution tables for fast lookups.
--     Per-author queries are cheap: read one author's data + JOIN small dist table.
--   Full-table snapshots (_table suffix) — only for get_all_authors_stats() which
--     needs to scan all authors at once for the ranking/listing UI.
--     Refreshed daily by bigquery-materialize.yml.
--
-- Per-author app queries (author profile page) use the VIEWS directly.
-- All-authors list queries use the _table snapshots.
--
-- Usage (full refresh — distribution tables + snapshots):
--   bq query --project_id=scholar-version2 --use_legacy_sql=false < bigquery/statistics/materialize_stats.sql

-- Step 0: One-time migration — drop the old BigQuery MATERIALIZED VIEW if it
-- still exists (replaced by this scripted approach).
DROP MATERIALIZED VIEW IF EXISTS `scholar-version2.statistics.stats_author_metrics_temporal`;

-- ── Distribution tables ──────────────────────────────────────────────────────

-- Step 1: Publication citation percentile distribution.
-- Reads raw publication data, computes PERCENT_RANK by pub_year, stores distinct
-- (pub_year, num_citations) → percentile pairs. Enables fast per-publication
-- percentile lookups in stats_publication_current without live PERCENT_RANK.
CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_publication_citations`
CLUSTER BY pub_year
AS SELECT * FROM (
  SELECT DISTINCT
    pub_year,
    num_citations,
    PERCENT_RANK() OVER(PARTITION BY pub_year ORDER BY num_citations ASC) AS num_citations_percentile
  FROM (
    SELECT
      CAST(JSON_EXTRACT_SCALAR(data, '$.data.bib.pub_year') AS INT64) AS pub_year,
      CAST(JSON_EXTRACT_SCALAR(data, '$.data.num_citations') AS INT64) AS num_citations
    FROM `scholar-version2.firestore_export.scholar_raw_pub_raw_latest`
  )
  WHERE pub_year > 1950
    AND pub_year <= EXTRACT(YEAR FROM CURRENT_DATE())
    AND num_citations > 0
);

-- Step 2: Author metric percentile distributions.
-- Reads raw author + publication data, computes PERCENT_RANK for all 8 metrics
-- partitioned by year_of_first_pub cohort. Stored in normalized
-- (year_of_first_pub, metric_name, metric_value, percentile) format.
-- Includes total_publications_with_citations so stats_author_publication_pip_inputs_current
-- can read num_papers distribution from here instead of scanning all authors.
CREATE OR REPLACE TABLE `scholar-version2.statistics.dist_author_metrics`
CLUSTER BY year_of_first_pub, metric_name
AS SELECT * FROM (
  WITH
    AuthorPubs AS (
      SELECT
        JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') AS scholar_id,
        JSON_EXTRACT_SCALAR(pub, '$.author_pub_id') AS author_pub_id,
        CAST(JSON_EXTRACT_SCALAR(pub, '$.bib.pub_year') AS INT64) AS pub_year
      FROM `scholar-version2.firestore_export.scholar_raw_author_raw_latest`,
           UNNEST(JSON_EXTRACT_ARRAY(DATA, '$.data.publications')) AS pub
      WHERE JSON_EXTRACT_SCALAR(pub, '$.author_pub_id') IS NOT NULL
        AND JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') IS NOT NULL
        AND CAST(JSON_EXTRACT_SCALAR(pub, '$.bib.pub_year') AS INT64) IS NOT NULL
    ),
    PubCitations AS (
      SELECT
        JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id') AS author_pub_id,
        CAST(JSON_EXTRACT_SCALAR(data, '$.data.num_citations') AS INT64) AS num_citations
      FROM `scholar-version2.firestore_export.scholar_raw_pub_raw_latest`
    ),
    AuthorPubCounts AS (
      SELECT
        ap.scholar_id,
        COUNT(ap.author_pub_id) AS total_publications,
        COUNT(IF(pc.num_citations > 0, ap.author_pub_id, NULL)) AS total_publications_with_citations,
        MIN(IF(pc.num_citations > 0, ap.pub_year, NULL)) AS year_of_first_pub
      FROM AuthorPubs ap
      LEFT JOIN PubCitations pc ON ap.author_pub_id = pc.author_pub_id
      WHERE ap.pub_year > 1950 AND ap.pub_year <= EXTRACT(YEAR FROM CURRENT_DATE())
      GROUP BY ap.scholar_id
    ),
    ScholarData AS (
      SELECT
        JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') AS scholar_id,
        CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.hindex') AS INT64) AS hindex,
        CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.hindex5y') AS INT64) AS hindex5y,
        CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.citedby') AS INT64) AS citedby,
        CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.citedby5y') AS INT64) AS citedby5y,
        CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.i10index') AS INT64) AS i10index,
        CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.i10index5y') AS INT64) AS i10index5y
      FROM `scholar-version2.firestore_export.scholar_raw_author_raw_latest`
      WHERE JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') IS NOT NULL
    ),
    CombinedData AS (
      SELECT
        s.scholar_id,
        COALESCE(pc.total_publications, 0) AS total_publications,
        COALESCE(pc.total_publications_with_citations, 0) AS total_publications_with_citations,
        pc.year_of_first_pub,
        s.hindex, s.hindex5y, s.citedby, s.citedby5y, s.i10index, s.i10index5y
      FROM ScholarData s
      LEFT JOIN AuthorPubCounts pc ON s.scholar_id = pc.scholar_id
      WHERE pc.year_of_first_pub IS NOT NULL
    ),
    WithPercentiles AS (
      SELECT *,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY hindex ASC)                            AS hindex_pct,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY hindex5y ASC)                          AS hindex5y_pct,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY citedby ASC)                           AS citedby_pct,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY citedby5y ASC)                         AS citedby5y_pct,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY i10index ASC)                          AS i10index_pct,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY i10index5y ASC)                        AS i10index5y_pct,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY total_publications ASC)                AS total_publications_pct,
        PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY total_publications_with_citations ASC) AS total_publications_with_citations_pct
      FROM CombinedData
    )
  SELECT DISTINCT year_of_first_pub, 'hindex'                           AS metric_name, hindex                           AS metric_value, hindex_pct                           AS percentile FROM WithPercentiles
  UNION ALL
  SELECT DISTINCT year_of_first_pub, 'hindex5y',                          hindex5y,                          hindex5y_pct                          FROM WithPercentiles
  UNION ALL
  SELECT DISTINCT year_of_first_pub, 'citedby',                           citedby,                           citedby_pct                           FROM WithPercentiles
  UNION ALL
  SELECT DISTINCT year_of_first_pub, 'citedby5y',                         citedby5y,                         citedby5y_pct                         FROM WithPercentiles
  UNION ALL
  SELECT DISTINCT year_of_first_pub, 'i10index',                          i10index,                          i10index_pct                          FROM WithPercentiles
  UNION ALL
  SELECT DISTINCT year_of_first_pub, 'i10index5y',                        i10index5y,                        i10index5y_pct                        FROM WithPercentiles
  UNION ALL
  SELECT DISTINCT year_of_first_pub, 'total_publications',                total_publications,                total_publications_pct                FROM WithPercentiles
  UNION ALL
  SELECT DISTINCT year_of_first_pub, 'total_publications_with_citations', total_publications_with_citations, total_publications_with_citations_pct FROM WithPercentiles
);

-- Step 3: PiP-AUC score percentile distribution.
-- Now that dist_publication_citations and dist_author_metrics exist, the views
-- stats_publication_current and stats_author_current are fast. This makes computing
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

-- ── Full-table snapshots (for all-authors list queries) ──────────────────────

-- Step 4: Materialize all-author stats table.
-- Used only by get_all_authors_stats() for the full ranking/list view.
-- Per-author profile queries use stats_author_current VIEW directly (cheap).
CREATE OR REPLACE TABLE `scholar-version2.statistics.stats_author_current_table`
CLUSTER BY scholar_id, year_of_first_pub
AS SELECT * FROM `scholar-version2.statistics.stats_author_current`;

-- Step 5: Materialize all-author PiP scores table.
-- Used only by get_all_authors_stats() alongside stats_author_current_table.
-- Per-author profile queries use stats_author_pip_scores_current VIEW directly (cheap).
CREATE OR REPLACE TABLE `scholar-version2.statistics.stats_author_pip_scores_current_table`
CLUSTER BY scholar_id
AS SELECT * FROM `scholar-version2.statistics.stats_author_pip_scores_current`;

-- Step 6: Temporal author metrics table.
-- Full temporal history — always needs to be materialized as a table since it
-- spans all authors × all historical years.
CREATE OR REPLACE TABLE `scholar-version2.statistics.stats_author_metrics_temporal`
CLUSTER BY scholar_id, state_year
AS SELECT * FROM `scholar-version2.statistics.stats_author_metrics_temporal_view`;
