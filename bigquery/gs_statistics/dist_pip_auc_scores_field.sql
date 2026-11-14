-- PiP scores and metrics of each author's field peers, for field-relative percentiles.
--
-- One row per (field_group, year_of_first_pub): the sorted PiP scores (plus legacy
-- PiP-AUC scores and, for the author's field page, sorted h-index, citations,
-- i10-index and cited publication counts) of active authors (hindex >= 3 AND
-- total_publications >= 3, the same benchmark as dist_pip_scores) in that field
-- whose career started within +/- half_width years. Exact career-start years are
-- too thin per field (the 11 groups hold ~500-2,700 profiles each), so the window
-- widens from +/-2 (a 5-year cohort) through 3, 4, 5, 7 to 10 years until it holds
-- at least 100 peers; if even +/-10 holds fewer, it keeps +/-10 and
-- ranked_author_pip_scores_current hides percentiles of windows under 30 peers.
-- cohort_start / cohort_end are the earliest and latest career-start years among
-- the peers actually included. ranked_author_pip_scores_current reads pip_scores
-- and ranked_author_current the metric arrays; both see the same peers.
--
-- PREREQUISITE: author_fields, stats_author_current and
-- stats_author_pip_scores_current views must exist.

CREATE OR REPLACE TABLE `scholar-version2.gs_statistics.dist_pip_auc_scores_field`
CLUSTER BY field_group, year_of_first_pub
AS
WITH
  Peers AS (
    SELECT f.field_group, p.year_of_first_pub, p.pip_score, p.pip_auc_score,
           s.hindex, s.citedby, COALESCE(s.i10index, 0) AS i10index, s.total_publications_with_citations
    FROM `scholar-version2.gs_statistics.stats_author_pip_scores_current` p
    JOIN `scholar-version2.gs_statistics.stats_author_current` s USING (scholar_id)
    JOIN `scholar-version2.gs_statistics.author_fields` f USING (scholar_id)
    WHERE s.hindex >= 3 AND s.total_publications >= 3
      AND p.year_of_first_pub IS NOT NULL AND p.pip_score IS NOT NULL
  ),
  -- Every career-start year any scored author has, for every group, so authors
  -- outside the benchmark (and years with no peers) still find their row.
  Cohorts AS (
    SELECT g.field_group, y AS year_of_first_pub
    FROM (SELECT DISTINCT field_group FROM Peers) g,
         (SELECT MIN(year_of_first_pub) AS lo, MAX(year_of_first_pub) AS hi
          FROM `scholar-version2.gs_statistics.stats_author_pip_scores_current`) r,
         UNNEST(GENERATE_ARRAY(r.lo, r.hi)) AS y
  ),
  Windows AS (
    SELECT c.field_group, c.year_of_first_pub, w AS half_width,
           COUNTIF(p.year_of_first_pub BETWEEN c.year_of_first_pub - w AND c.year_of_first_pub + w) AS n
    FROM Cohorts c
    CROSS JOIN UNNEST([2, 3, 4, 5, 7, 10]) AS w
    LEFT JOIN Peers p ON p.field_group = c.field_group
    GROUP BY c.field_group, c.year_of_first_pub, w
  ),
  Chosen AS (
    -- Smallest window with >= 100 peers, else the widest.
    SELECT field_group, year_of_first_pub,
           ARRAY_AGG(half_width ORDER BY n >= 100 DESC, IF(n >= 100, half_width, -half_width) LIMIT 1)[OFFSET(0)]
             AS half_width
    FROM Windows
    GROUP BY field_group, year_of_first_pub
  )
SELECT
  c.field_group,
  c.year_of_first_pub,
  c.half_width,
  MIN(p.year_of_first_pub) AS cohort_start,
  MAX(p.year_of_first_pub) AS cohort_end,
  COUNT(p.pip_score) AS peer_count,
  ARRAY_AGG(p.pip_score IGNORE NULLS ORDER BY p.pip_score) AS pip_scores,
  ARRAY_AGG(p.pip_auc_score IGNORE NULLS ORDER BY p.pip_auc_score) AS scores,
  ARRAY_AGG(p.hindex IGNORE NULLS ORDER BY p.hindex) AS hindex_values,
  ARRAY_AGG(p.citedby IGNORE NULLS ORDER BY p.citedby) AS citedby_values,
  ARRAY_AGG(p.i10index IGNORE NULLS ORDER BY p.i10index) AS i10index_values,
  ARRAY_AGG(p.total_publications_with_citations IGNORE NULLS ORDER BY p.total_publications_with_citations)
    AS total_publications_with_citations_values
FROM Chosen c
LEFT JOIN Peers p
  ON p.field_group = c.field_group
 AND p.year_of_first_pub BETWEEN c.year_of_first_pub - c.half_width AND c.year_of_first_pub + c.half_width
GROUP BY c.field_group, c.year_of_first_pub, c.half_width;
