CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_publication_current` AS
-- Tier 1: Raw publication statistics — no percentiles, no PERCENT_RANK.
-- Extracts core publication details from the latest deduplicated publication data.
-- Percentiles are added by ranked_publication_current (Tier 3) via dist_publication_citations.
SELECT
  scholar_id,
  author_pub_id,
  pub_year,
  title,
  author,
  num_citations,
  timestamp AS last_updated
FROM `scholar-version2.scholar_raw_data.pub_latest_table`
WHERE pub_year > 1950
  AND pub_year <= EXTRACT(YEAR FROM CURRENT_DATE())
  AND num_citations > 0;
