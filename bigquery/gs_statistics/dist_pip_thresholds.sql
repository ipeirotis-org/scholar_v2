-- Peer publication-count distributions across 40 citation-percentile thresholds,
-- for the PiP score and standing curve.
--
-- Matches paper/sql/14_gs_variant_impact.sql:
--   - Active peers: hindex >= 3 AND total_publications_with_citations >= 3
--     AND year_of_first_pub IS NOT NULL.
--   - 40 percentile classes j = 0..39 (thresholds theta_j = j / 40). A cited
--     work with num_citations_percentile = y falls into class
--     b = LEAST(39, FLOOR(40 * y)) and counts at every threshold j <= b.
--   - For each (year_of_first_pub, j), m_arr holds the sorted distinct counts
--     n_j observed among active peers in that cohort, cum_arr holds the
--     cumulative number of peers with n_peer <= m, and cohort_size is the total
--     number of active peers in that cohort.
--
-- Pre-aggregating to one row per (year_of_first_pub, j) (~3,000 rows total)
-- lets stats_author_pip_scores_current compute any single author's standing
-- curve g(theta_j) in O(log K) via RANGE_BUCKET(n_j - 1, m_arr) without
-- scanning or windowing across other authors at query time.
--
-- Rebuilt weekly by gs_crawler (CRAWL_MODE=materialize).

CREATE OR REPLACE TABLE `scholar-version2.gs_statistics.dist_pip_thresholds`
CLUSTER BY year_of_first_pub, j
AS
WITH
  ActivePeers AS (
    SELECT scholar_id, year_of_first_pub
    FROM `scholar-version2.gs_statistics.stats_author_current`
    WHERE hindex >= 3
      AND total_publications_with_citations >= 3
      AND year_of_first_pub IS NOT NULL
  ),
  Buckets AS (
    SELECT
      a.scholar_id,
      a.year_of_first_pub,
      LEAST(39, CAST(FLOOR(p.num_citations_percentile * 40) AS INT64)) AS b,
      COUNT(*) AS cnt
    FROM ActivePeers a
    JOIN `scholar-version2.gs_statistics.ranked_publication_current` p
      USING (scholar_id)
    GROUP BY a.scholar_id, a.year_of_first_pub, b
  ),
  PeerCounts AS (
    SELECT
      g.scholar_id,
      g.year_of_first_pub,
      g.j,
      SUM(IFNULL(b.cnt, 0)) OVER (
        PARTITION BY g.scholar_id
        ORDER BY g.j DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS n_j
    FROM (
      SELECT scholar_id, year_of_first_pub, j
      FROM ActivePeers, UNNEST(GENERATE_ARRAY(0, 39)) AS j
    ) g
    LEFT JOIN Buckets b
      ON b.scholar_id = g.scholar_id AND b.b = g.j
  ),
  CountFreqs AS (
    SELECT
      year_of_first_pub,
      j,
      n_j AS m,
      COUNT(*) AS cnt
    FROM PeerCounts
    GROUP BY year_of_first_pub, j, m
  ),
  Cumulative AS (
    SELECT
      year_of_first_pub,
      j,
      m,
      SUM(cnt) OVER (PARTITION BY year_of_first_pub, j ORDER BY m) AS cum_peers,
      SUM(cnt) OVER (PARTITION BY year_of_first_pub, j) AS cohort_size
    FROM CountFreqs
  )
SELECT
  year_of_first_pub,
  j,
  ANY_VALUE(cohort_size) AS cohort_size,
  ARRAY_AGG(m ORDER BY m) AS m_arr,
  ARRAY_AGG(cum_peers ORDER BY m) AS cum_arr
FROM Cumulative
GROUP BY year_of_first_pub, j;
