CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_publication_current` AS
-- Tier 1: Raw publication statistics — no percentiles, no PERCENT_RANK.
-- Extracts core publication details from the latest deduplicated publication data.
-- Percentiles are added by ranked_publication_current (Tier 3) via dist_publication_citations.
SELECT
  SPLIT(JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id'), ':')[SAFE_OFFSET(0)] AS scholar_id,
  JSON_EXTRACT_SCALAR(data, '$.data.author_pub_id') AS author_pub_id,
  CAST(JSON_EXTRACT_SCALAR(data, '$.data.bib.pub_year') AS INT64) AS pub_year,
  JSON_EXTRACT_SCALAR(data, '$.data.bib.title') AS title,
  JSON_EXTRACT_SCALAR(data, '$.data.bib.author') AS author,
  CAST(JSON_EXTRACT_SCALAR(data, '$.data.num_citations') AS INT64) AS num_citations,
  timestamp AS last_updated
FROM `scholar-version2.scholar_raw_data.pub_latest`
WHERE CAST(JSON_EXTRACT_SCALAR(data, '$.data.bib.pub_year') AS INT64) > 1950
  AND CAST(JSON_EXTRACT_SCALAR(data, '$.data.bib.pub_year') AS INT64) <= EXTRACT(YEAR FROM CURRENT_DATE())
  AND CAST(JSON_EXTRACT_SCALAR(data, '$.data.num_citations') AS INT64) > 0;
