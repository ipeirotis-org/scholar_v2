CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_author_publication_pip_inputs_current` AS
-- Computes num_papers_percentile (the X-axis of the PiP chart) for each publication
-- via interpolation of the author's publication-count percentile.
--
-- Uses 'active_authors' benchmark for meaningful percentile differentiation.
--
-- S2 migration note: ranked_publication_current no longer includes scholar_id
-- (papers are identified by corpusid alone). The author dimension is brought in
-- by joining with base_author_publications.
WITH
  num_papers_percentile AS (
    -- Read the precomputed publication-count distribution from dist_author_metrics.
    -- This replaces the original DISTINCT query over all of stats_author_current,
    -- which was the main bottleneck for per-author page loads.
    SELECT
      year_of_first_pub,
      metric_value  AS total_publications_with_citations,
      percentile    AS total_publications_with_citations_percentile
    FROM `scholar-version2.statistics.dist_author_metrics`
    WHERE metric_name = 'total_publications_with_citations'
      AND benchmark = 'active_authors'
  ),
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
  Distances AS (
    SELECT
      rp.*,
      B.total_publications_with_citations_percentile,
      rp.publication_rank - B.total_publications_with_citations AS distance,
      CASE
        WHEN rp.publication_rank - B.total_publications_with_citations >= 0 THEN 'positive'
        ELSE 'negative'
      END AS distance_type
    FROM RankedPublications rp
    JOIN num_papers_percentile B ON rp.year_of_first_pub = B.year_of_first_pub
  ),
  RankedDistances AS (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY scholar_id, author_pub_id, distance_type ORDER BY ABS(distance)) AS rank_distance
    FROM Distances
  ),
  FilteredDistances AS (
    SELECT d.*
    FROM RankedDistances d
    WHERE rank_distance = 1
  ),
  AggregatedDistances AS (
    SELECT
      scholar_id,
      author_pub_id,
      MAX(CASE WHEN distance_type = 'positive' THEN total_publications_with_citations_percentile END)
        AS positive_percentile,
      MAX(CASE WHEN distance_type = 'negative' THEN total_publications_with_citations_percentile END)
        AS negative_percentile,
      MAX(CASE WHEN distance_type = 'positive' THEN ABS(distance) END)
        AS positive_distance,
      MAX(CASE WHEN distance_type = 'negative' THEN ABS(distance) END)
        AS negative_distance
    FROM FilteredDistances
    GROUP BY scholar_id, author_pub_id
  ),
  InterpolatedResults AS (
    SELECT
      fd.scholar_id,
      fd.author_pub_id,
      fd.num_citations,
      fd.num_citations_percentile,
      fd.publication_rank,
      fd.year_of_first_pub,
      fd.total_publications_with_citations,
      CASE
        WHEN ad.positive_percentile IS NULL THEN 0.0
        WHEN ad.negative_percentile IS NULL THEN 1.0
        ELSE (ad.negative_percentile * ad.positive_distance + ad.positive_percentile * ad.negative_distance)
             / (ad.positive_distance + ad.negative_distance)
      END AS interpolated_percentile
    FROM FilteredDistances fd
    JOIN AggregatedDistances ad
      ON fd.scholar_id = ad.scholar_id
     AND fd.author_pub_id = ad.author_pub_id
  )
SELECT
  scholar_id,
  author_pub_id,
  MAX(num_citations) AS num_citations,
  MAX(num_citations_percentile) AS num_citations_percentile,
  MAX(publication_rank) AS publication_rank,
  MAX(interpolated_percentile) AS num_papers_percentile
FROM InterpolatedResults
GROUP BY scholar_id, author_pub_id;
