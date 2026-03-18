CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_author_current` AS
-- Computes author metrics and resolves percentiles via the precomputed
-- dist_author_metrics distribution table rather than running 8 live PERCENT_RANK()
-- window functions. This makes per-author page loads cheap: the view reads raw
-- data for the requested author(s) and does a fast JOIN against the small
-- distribution table.
--
-- Lookup strategy (floor approximation):
--   For each metric, MAX(percentile WHERE dist_value <= actual_value AND metric_name = X)
--   gives an exact match when the value exists in the distribution, or the
--   nearest-lower approximation for newly-fetched authors not yet in the table.
WITH
  ScholarData AS (
    SELECT
      JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') AS scholar_id,
      JSON_EXTRACT_SCALAR(DATA, '$.data.name') AS name,
      JSON_EXTRACT_SCALAR(DATA, '$.data.affiliation') AS affiliation,
      JSON_EXTRACT_SCALAR(DATA, '$.data.email_domain') AS email_domain,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.hindex') AS INT64) AS hindex,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.hindex5y') AS INT64) AS hindex5y,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.citedby') AS INT64) AS citedby,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.citedby5y') AS INT64) AS citedby5y,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.i10index') AS INT64) AS i10index,
      CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.i10index5y') AS INT64) AS i10index5y,
      timestamp
    FROM `scholar-version2.firestore_export.scholar_raw_author_raw_latest`
    WHERE JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') IS NOT NULL
  ),
  AuthorPubsData AS (
    SELECT scholar_id, author_pub_id, pub_year
    FROM `scholar-version2.statistics.base_author_publications`
    WHERE pub_year > 1950 AND pub_year <= EXTRACT(YEAR FROM CURRENT_DATE())
  ),
  PublicationCounts AS (
    SELECT
      apd.scholar_id,
      COUNT(apd.author_pub_id) AS total_publications_calculated,
      COUNT(IF(ps.num_citations > 0, apd.author_pub_id, NULL)) AS total_publications_with_citations_calculated
    FROM AuthorPubsData apd
    LEFT JOIN `scholar-version2.statistics.stats_publication_current` ps
      ON apd.author_pub_id = ps.author_pub_id
    GROUP BY apd.scholar_id
  ),
  FirstPubYear AS (
    SELECT apd.scholar_id, MIN(apd.pub_year) AS year_of_first_pub
    FROM AuthorPubsData apd
    JOIN `scholar-version2.statistics.stats_publication_current` ps
      ON apd.author_pub_id = ps.author_pub_id
    WHERE ps.num_citations > 0
    GROUP BY apd.scholar_id
  ),
  CombinedData AS (
    SELECT
      sd.scholar_id,
      sd.name,
      sd.affiliation,
      sd.email_domain,
      sd.hindex,
      sd.hindex5y,
      sd.citedby,
      sd.citedby5y,
      sd.i10index,
      sd.i10index5y,
      COALESCE(pc.total_publications_calculated, 0) AS total_publications,
      COALESCE(pc.total_publications_with_citations_calculated, 0) AS total_publications_with_citations,
      fpy.year_of_first_pub,
      sd.timestamp AS last_updated
    FROM ScholarData sd
    LEFT JOIN FirstPubYear fpy ON sd.scholar_id = fpy.scholar_id
    LEFT JOIN PublicationCounts pc ON sd.scholar_id = pc.scholar_id
  ),
  PercentileLookup AS (
    -- One JOIN against dist_author_metrics, then CASE WHEN routes each metric
    -- to the correct column. MAX() across all distribution rows for the cohort
    -- where dist_value <= author_value gives the floor percentile for each metric.
    SELECT
      c.scholar_id,
      MAX(CASE WHEN d.metric_name = 'hindex'                         AND d.metric_value <= c.hindex                         THEN d.percentile END) AS hindex_percentile,
      MAX(CASE WHEN d.metric_name = 'hindex5y'                       AND d.metric_value <= c.hindex5y                       THEN d.percentile END) AS hindex5y_percentile,
      MAX(CASE WHEN d.metric_name = 'citedby'                        AND d.metric_value <= c.citedby                        THEN d.percentile END) AS citedby_percentile,
      MAX(CASE WHEN d.metric_name = 'citedby5y'                      AND d.metric_value <= c.citedby5y                      THEN d.percentile END) AS citedby5y_percentile,
      MAX(CASE WHEN d.metric_name = 'i10index'                       AND d.metric_value <= c.i10index                       THEN d.percentile END) AS i10index_percentile,
      MAX(CASE WHEN d.metric_name = 'i10index5y'                     AND d.metric_value <= c.i10index5y                     THEN d.percentile END) AS i10index5y_percentile,
      MAX(CASE WHEN d.metric_name = 'total_publications'             AND d.metric_value <= c.total_publications             THEN d.percentile END) AS total_publications_percentile,
      MAX(CASE WHEN d.metric_name = 'total_publications_with_citations' AND d.metric_value <= c.total_publications_with_citations THEN d.percentile END) AS total_publications_with_citations_percentile
    FROM CombinedData c
    JOIN `scholar-version2.statistics.dist_author_metrics` d
      ON d.year_of_first_pub = c.year_of_first_pub
    GROUP BY c.scholar_id
  )
SELECT
  c.*,
  COALESCE(l.hindex_percentile,                         0.0) AS hindex_percentile,
  COALESCE(l.hindex5y_percentile,                       0.0) AS hindex5y_percentile,
  COALESCE(l.citedby_percentile,                        0.0) AS citedby_percentile,
  COALESCE(l.citedby5y_percentile,                      0.0) AS citedby5y_percentile,
  COALESCE(l.i10index_percentile,                       0.0) AS i10index_percentile,
  COALESCE(l.i10index5y_percentile,                     0.0) AS i10index5y_percentile,
  COALESCE(l.total_publications_percentile,             0.0) AS total_publications_percentile,
  COALESCE(l.total_publications_with_citations_percentile, 0.0) AS total_publications_with_citations_percentile
FROM CombinedData c
LEFT JOIN PercentileLookup l ON l.scholar_id = c.scholar_id;
