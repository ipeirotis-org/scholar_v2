CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_author_publication_pip_inputs_current` AS
-- Computes num_papers_percentile (the X-axis of the PiP chart) for each publication
-- via interpolation of the author's publication-count percentile.
--
-- Uses scalar subqueries against dist_author_metrics to find the floor and ceiling
-- breakpoints for interpolation. This replaces the previous cross-join approach
-- (Distances CTE) which produced ~100B+ intermediate rows.
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
  Interpolated AS (
    SELECT
      scholar_id,
      author_pub_id,
      num_citations,
      num_citations_percentile,
      publication_rank,
      -- Floor: largest breakpoint value <= publication_rank
      (SELECT MAX(d.metric_value)
       FROM `scholar-version2.statistics.dist_author_metrics` d
       WHERE d.benchmark = 'active_authors'
         AND d.metric_name = 'total_publications_with_citations'
         AND d.year_of_first_pub = rp.year_of_first_pub
         AND d.metric_value <= rp.publication_rank
      ) AS floor_value,
      (SELECT MAX(d.percentile)
       FROM `scholar-version2.statistics.dist_author_metrics` d
       WHERE d.benchmark = 'active_authors'
         AND d.metric_name = 'total_publications_with_citations'
         AND d.year_of_first_pub = rp.year_of_first_pub
         AND d.metric_value <= rp.publication_rank
      ) AS floor_pct,
      -- Ceiling: smallest breakpoint value >= publication_rank
      (SELECT MIN(d.metric_value)
       FROM `scholar-version2.statistics.dist_author_metrics` d
       WHERE d.benchmark = 'active_authors'
         AND d.metric_name = 'total_publications_with_citations'
         AND d.year_of_first_pub = rp.year_of_first_pub
         AND d.metric_value >= rp.publication_rank
      ) AS ceiling_value,
      (SELECT MIN(d.percentile)
       FROM `scholar-version2.statistics.dist_author_metrics` d
       WHERE d.benchmark = 'active_authors'
         AND d.metric_name = 'total_publications_with_citations'
         AND d.year_of_first_pub = rp.year_of_first_pub
         AND d.metric_value >= rp.publication_rank
      ) AS ceiling_pct
    FROM RankedPublications rp
  )
SELECT
  scholar_id,
  author_pub_id,
  num_citations,
  num_citations_percentile,
  publication_rank,
  CASE
    WHEN floor_pct IS NULL THEN 0.0
    WHEN ceiling_pct IS NULL THEN 1.0
    WHEN floor_value = ceiling_value THEN floor_pct
    ELSE (ceiling_pct * (publication_rank - floor_value) + floor_pct * (ceiling_value - publication_rank))
         / (ceiling_value - floor_value)
  END AS num_papers_percentile
FROM Interpolated;
