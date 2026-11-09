-- 07: Within-cohort percentiles of each author metric at each career age.
--
-- For every (benchmark, start_year, age) peer group from 06, the Hazen (mid-rank) percentile of
--   pip        step-form PiP-AUC
--   h_index    h-index from citations through the evaluation year
--   citations  total citations through the evaluation year
--   sum_hazen  sum of the author's paper percentiles (on current percentiles)
--   mean_below mean paper percentile
--   n_articles articles published by the evaluation year
-- so all scholar indicators can be compared on the same footing.
-- Hazen percentile = (peers with a lower value + half the peers with the same value) / peers
--                  = (peers + rank ascending - rank descending) / (2 * peers), which avoids partitioning by FLOAT64.
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.author_percentiles_by_age`
PARTITION BY RANGE_BUCKET(start_year, GENERATE_ARRAY(1950, 2026, 1))
CLUSTER BY benchmark, age, authorid
OPTIONS (description = 'Within-cohort Hazen percentiles of author metrics by career age. Built by paper/sql/07_author_percentiles_by_age.sql')
AS
SELECT
  benchmark, authorid, start_year, age, eval_year, n_peers,
  n_articles, n_cited, citations, h_index, prod_pct, pip, mean_below, sum_hazen,
  (n_peers + RANK() OVER (PARTITION BY benchmark, start_year, age ORDER BY pip)
     - RANK() OVER (PARTITION BY benchmark, start_year, age ORDER BY pip DESC)) / (2 * n_peers) AS pct_pip,
  (n_peers + RANK() OVER (PARTITION BY benchmark, start_year, age ORDER BY h_index)
     - RANK() OVER (PARTITION BY benchmark, start_year, age ORDER BY h_index DESC)) / (2 * n_peers) AS pct_h,
  (n_peers + RANK() OVER (PARTITION BY benchmark, start_year, age ORDER BY citations)
     - RANK() OVER (PARTITION BY benchmark, start_year, age ORDER BY citations DESC)) / (2 * n_peers) AS pct_citations,
  (n_peers + RANK() OVER (PARTITION BY benchmark, start_year, age ORDER BY sum_hazen)
     - RANK() OVER (PARTITION BY benchmark, start_year, age ORDER BY sum_hazen DESC)) / (2 * n_peers) AS pct_sum,
  (n_peers + RANK() OVER (PARTITION BY benchmark, start_year, age ORDER BY mean_below)
     - RANK() OVER (PARTITION BY benchmark, start_year, age ORDER BY mean_below DESC)) / (2 * n_peers) AS pct_mean,
  (n_peers + RANK() OVER (PARTITION BY benchmark, start_year, age ORDER BY n_articles)
     - RANK() OVER (PARTITION BY benchmark, start_year, age ORDER BY n_articles DESC)) / (2 * n_peers) AS pct_articles
FROM `scholar-version2.paper_rp.author_pip_by_age`;
