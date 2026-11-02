-- One-off: seed gs_data from the scholarly-era tables (Feb 2024 - Sep 2025).
-- Idempotent: skips authors that already have a legacy_scholarly snapshot.
-- Uses no SerpApi searches. Publication totals come from the author's profile
-- list (as a fresh profile crawl would); titles/venues from pub_latest_table.
INSERT INTO `scholar-version2.gs_data.author_snapshots`
SELECT
  a.scholar_id,
  a.timestamp AS fetched_at,
  'legacy_scholarly' AS source,
  a.name,
  a.affiliation,
  a.email_domain,
  ARRAY(SELECT JSON_VALUE(i) FROM UNNEST(JSON_QUERY_ARRAY(a.data, '$.data.interests')) i) AS interests,
  a.citedby,
  a.hindex,
  a.i10index,
  a.citedby5y AS citedby_since,
  a.hindex5y AS hindex_since,
  a.i10index5y AS i10index_since,
  EXTRACT(YEAR FROM a.timestamp) - 5 AS since_year,
  JSON_QUERY(a.data, '$.data.cites_per_year') AS cites_per_year,
  ARRAY(
    SELECT AS STRUCT
      JSON_VALUE(c, '$.scholar_id') AS scholar_id,
      JSON_VALUE(c, '$.name') AS name,
      JSON_VALUE(c, '$.affiliation') AS affiliation
    FROM UNNEST(a.coauthors) c
    WHERE JSON_VALUE(c, '$.scholar_id') IS NOT NULL
  ) AS coauthors,
  ARRAY_LENGTH(a.publications) AS num_publications
FROM `scholar-version2.scholar_raw_data.author_latest_table` a
WHERE a.scholar_id IS NOT NULL
  AND a.scholar_id NOT IN (
    SELECT scholar_id FROM `scholar-version2.gs_data.author_snapshots` WHERE source = 'legacy_scholarly'
  );

INSERT INTO `scholar-version2.gs_data.publication_snapshots`
SELECT
  a.scholar_id,
  JSON_VALUE(p, '$.author_pub_id') AS author_pub_id,
  a.timestamp AS fetched_at,
  'legacy_scholarly' AS source,
  d.title,
  d.author AS authors,
  JSON_VALUE(d.data, '$.data.bib.citation') AS venue,
  SAFE_CAST(JSON_VALUE(p, '$.bib.pub_year') AS INT64) AS pub_year,
  SAFE_CAST(JSON_VALUE(p, '$.num_citations') AS INT64) AS num_citations
FROM `scholar-version2.scholar_raw_data.author_latest_table` a,
  UNNEST(a.publications) p
LEFT JOIN `scholar-version2.scholar_raw_data.pub_latest_table` d
  ON d.author_pub_id = JSON_VALUE(p, '$.author_pub_id')
WHERE a.scholar_id IS NOT NULL
  AND JSON_VALUE(p, '$.author_pub_id') IS NOT NULL
  AND a.scholar_id NOT IN (
    SELECT DISTINCT scholar_id FROM `scholar-version2.gs_data.publication_snapshots` WHERE source = 'legacy_scholarly'
  );
