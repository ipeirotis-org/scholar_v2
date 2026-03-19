CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_publication_current` AS
-- Selects core publication details and resolves the citation percentile via a
-- precomputed distribution lookup (dist_publication_citations) rather than running
-- PERCENT_RANK() live. This makes per-author page loads cheap.
--
-- Lookup strategy (floor approximation):
--   MAX(percentile WHERE dist_citations <= actual_citations)
--   - Exact match when the citation count exists in the distribution.
--   - Nearest-lower approximation for newly-fetched publications whose
--     citation count has not yet been included in the distribution table.
WITH raw_pubs AS (
  SELECT
    SPLIT(JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id'), ':')[SAFE_OFFSET(0)] AS scholar_id,
    JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id') AS author_pub_id,
    CAST(JSON_EXTRACT_SCALAR(data, '$.data.bib.pub_year') AS INT64) AS pub_year,
    JSON_EXTRACT_SCALAR(data, '$.data.bib.title') AS title,
    JSON_EXTRACT_SCALAR(data, '$.data.bib.author') AS author,
    CAST(JSON_EXTRACT_SCALAR(data, '$.data.num_citations') AS INT64) AS num_citations,
    timestamp
  FROM `scholar-version2.scholar_raw_data.pub_latest`
  WHERE CAST(JSON_EXTRACT_SCALAR(data, '$.data.bib.pub_year') AS INT64) > 1950
    AND CAST(JSON_EXTRACT_SCALAR(data, '$.data.bib.pub_year') AS INT64) <= EXTRACT(YEAR FROM CURRENT_DATE())
    AND CAST(JSON_EXTRACT_SCALAR(data, '$.data.num_citations') AS INT64) > 0
),
percentile_lookup AS (
  -- For each publication, find the highest distribution percentile where
  -- the distribution citation count does not exceed the actual citation count.
  SELECT
    p.author_pub_id,
    MAX(d.num_citations_percentile) AS num_citations_percentile
  FROM raw_pubs p
  JOIN `scholar-version2.statistics.dist_publication_citations` d
    ON d.pub_year = p.pub_year
   AND d.num_citations <= p.num_citations
  GROUP BY p.author_pub_id
)
SELECT
  p.scholar_id,
  p.author_pub_id,
  p.title,
  p.author,
  p.pub_year,
  p.num_citations,
  COALESCE(l.num_citations_percentile, 0.0) AS num_citations_percentile,
  p.timestamp AS last_updated
FROM raw_pubs p
LEFT JOIN percentile_lookup l ON l.author_pub_id = p.author_pub_id;
