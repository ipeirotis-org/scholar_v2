-- Level 3: Raw temporal author metrics — no percentiles, no PERCENT_RANK.
-- Computes per-author per-year metrics: h_index, total_citations, i10_index, etc.
-- Depends on: intermediate_author_publication_state_temporal (L2), stats_publication_current (L1).
-- Percentiles are added by ranked_author_metrics_temporal (L5)
-- via dist_author_metrics_temporal (L4).
--
-- This view defines the temporal metrics logic. It is NOT queried directly by the app.
-- Instead, a scheduled process materializes the ranked version into a table.
CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_author_metrics_temporal_view` AS

WITH PublicationState AS (
  SELECT * FROM `scholar-version2.statistics.intermediate_author_publication_state_temporal`
),

FirstPubYear AS (
  SELECT
    ps.scholar_id,
    MIN(ps.pub_year) as year_of_first_pub
  FROM PublicationState ps
  JOIN `scholar-version2.statistics.stats_publication_current` spc
      ON ps.author_pub_id = spc.author_pub_id
  WHERE spc.num_citations > 0
  GROUP BY ps.scholar_id
),

RankedPublications AS (
  SELECT
    scholar_id,
    state_year,
    author_pub_id,
    cumulative_citations_at_state_year,
    ROW_NUMBER() OVER (PARTITION BY scholar_id, state_year ORDER BY cumulative_citations_at_state_year DESC) as h_rank
  FROM PublicationState
),

HIndexCalculated AS (
  SELECT
    scholar_id,
    state_year,
    MAX(LEAST(h_rank, cumulative_citations_at_state_year)) AS h_index
  FROM RankedPublications
  WHERE cumulative_citations_at_state_year > 0
  GROUP BY scholar_id, state_year
),

PublicationRecentCitations AS (
 SELECT
    scholar_id,
    author_pub_id,
    state_year,
    SUM(yearly_citations_at_state_year) OVER (
        PARTITION BY scholar_id, author_pub_id
        ORDER BY state_year
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS citations_last_5_years
 FROM PublicationState
),

RankedRecentPublications AS (
  SELECT
    prc.scholar_id,
    prc.state_year,
    prc.author_pub_id,
    prc.citations_last_5_years,
    ROW_NUMBER() OVER (PARTITION BY prc.scholar_id, prc.state_year ORDER BY prc.citations_last_5_years DESC) as h5y_rank
  FROM PublicationRecentCitations prc
  WHERE prc.citations_last_5_years > 0
),

HIndex5yCalculated AS (
  SELECT
    scholar_id,
    state_year,
    MAX(LEAST(h5y_rank, citations_last_5_years)) AS h_index_5y
  FROM RankedRecentPublications
  GROUP BY scholar_id, state_year
),

AggregatedMetrics AS (
  SELECT
    ps.scholar_id,
    ps.state_year,
    COUNT(DISTINCT ps.author_pub_id) AS total_publications,
    SUM(ps.cumulative_citations_at_state_year) AS total_citations,
    COUNT(DISTINCT IF(ps.cumulative_citations_at_state_year >= 10, ps.author_pub_id, NULL)) AS i10_index,
    (SELECT SUM(ps_inner.yearly_citations_at_state_year)
     FROM PublicationState ps_inner
     WHERE ps_inner.scholar_id = ps.scholar_id
       AND ps_inner.state_year BETWEEN ps.state_year - 4 AND ps.state_year
    ) AS total_recent_citations_5y,
    COUNT(DISTINCT IF(prc.citations_last_5_years >= 10, prc.author_pub_id, NULL)) AS i10_index_5y
  FROM PublicationState ps
  LEFT JOIN PublicationRecentCitations prc ON ps.scholar_id = prc.scholar_id AND ps.author_pub_id = prc.author_pub_id AND ps.state_year = prc.state_year
  GROUP BY ps.scholar_id, ps.state_year
)

SELECT
  am.scholar_id,
  am.state_year,
  fpy.year_of_first_pub,
  am.total_publications,
  am.total_citations,
  COALESCE(am.total_recent_citations_5y, 0) as total_recent_citations_5y,
  COALESCE(hic.h_index, 0) AS h_index,
  COALESCE(h5yc.h_index_5y, 0) AS h_index_5y,
  COALESCE(am.i10_index, 0) AS i10_index,
  COALESCE(am.i10_index_5y, 0) AS i10_index_5y
FROM AggregatedMetrics am
LEFT JOIN FirstPubYear fpy ON am.scholar_id = fpy.scholar_id
LEFT JOIN HIndexCalculated hic ON am.scholar_id = hic.scholar_id AND am.state_year = hic.state_year
LEFT JOIN HIndex5yCalculated h5yc ON am.scholar_id = h5yc.scholar_id AND am.state_year = h5yc.state_year;
