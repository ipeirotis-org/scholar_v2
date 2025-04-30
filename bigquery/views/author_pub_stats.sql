CREATE OR REPLACE VIEW `scholar-version2.statistics.author_pub_stats` AS
WITH
  num_papers_percentile AS (
  SELECT
    DISTINCT year_of_first_pub,
    total_publications_with_citations,
    total_publications_with_citations_percentile
  FROM
    `scholar-version2.statistics.author_stats` ),
  RankedPublications AS (
  SELECT
    p.scholar_id,
    p.author_pub_id,
    p.num_citations,
    p.num_citations_percentile,
    ROW_NUMBER() OVER(PARTITION BY p.scholar_id ORDER BY p.num_citations_percentile DESC) AS publication_rank,
    a.year_of_first_pub,
    a.total_publications_with_citations
  FROM
    `scholar-version2.statistics.publication_stats` p
  JOIN
    `scholar-version2.statistics.author_stats` a
  ON
    p.scholar_id = a.scholar_id ),
  Distances AS (
  SELECT
    rp.*,
    B.total_publications_with_citations_percentile,
    rp.publication_rank - B.total_publications_with_citations AS distance,
    CASE
      WHEN rp.publication_rank - B.total_publications_with_citations >= 0 THEN 'positive'
      ELSE 'negative'
  END
    AS distance_type
  FROM
    RankedPublications rp
  JOIN
    num_papers_percentile B
  ON
    rp.year_of_first_pub = B.year_of_first_pub ),
  RankedDistances AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY scholar_id, author_pub_id, distance_type ORDER BY ABS(distance)) AS rank_distance
  FROM
    Distances ),
  FilteredDistances AS (
  SELECT
    d.*
  FROM
    RankedDistances d
  WHERE
    rank_distance = 1 ),
  AggregatedDistances AS (
  SELECT
    scholar_id,
    author_pub_id,
    MAX(CASE
        WHEN distance_type = 'positive' THEN total_publications_with_citations_percentile
    END
      ) OVER(PARTITION BY scholar_id, author_pub_id) AS positive_percentile,
    MAX(CASE
        WHEN distance_type = 'negative' THEN total_publications_with_citations_percentile
    END
      ) OVER(PARTITION BY scholar_id, author_pub_id) AS negative_percentile,
    MAX(CASE
        WHEN distance_type = 'positive' THEN ABS(distance)
    END
      ) OVER(PARTITION BY scholar_id, author_pub_id) AS positive_distance,
    MAX(CASE
        WHEN distance_type = 'negative' THEN ABS(distance)
    END
      ) OVER(PARTITION BY scholar_id, author_pub_id) AS negative_distance
  FROM
    FilteredDistances ),
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
      WHEN positive_percentile IS NULL THEN 0.0
      WHEN negative_percentile IS NULL THEN 1.0
      ELSE (negative_percentile * positive_distance + positive_percentile * negative_distance) / (positive_distance + negative_distance)
  END
    AS interpolated_percentile
  FROM
    FilteredDistances fd
  JOIN
    AggregatedDistances ad
  ON
    fd.scholar_id = ad.scholar_id
    AND fd.author_pub_id = ad.author_pub_id )
SELECT
  DISTINCT scholar_id,
  author_pub_id,
  num_citations,
  num_citations_percentile,
  publication_rank,
  interpolated_percentile AS num_papers_percentile
FROM
  InterpolatedResults
ORDER BY
  scholar_id,
  publication_rank