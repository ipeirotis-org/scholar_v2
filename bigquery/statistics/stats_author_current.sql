CREATE OR REPLACE VIEW `scholar-version2.statistics.stats_author_current` AS
WITH
  ScholarData AS (
  -- Extracts core author attributes and metrics directly from the raw Firestore export
  -- This part remains mostly the same as the original author_stats
  SELECT
    JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') AS scholar_id,
    JSON_EXTRACT_SCALAR(DATA, '$.data.name') AS name,
    JSON_EXTRACT_SCALAR(DATA, '$.data.affiliation') AS affiliation,
    JSON_EXTRACT_SCALAR(DATA, '$.data.email_domain') AS email_domain,
    CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.hindex') AS INT64) AS hindex,
    CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.hindex5y') AS INT64) AS hindex5y,
    CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.citedby') AS INT64) AS citedby,
    CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.citedby5y') AS INT64) AS citedby5y,
    CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.i10index') AS INT64) AS i10index,
    CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.i10index5y') AS INT64) AS i10index5y,
    timestamp -- Keep the timestamp from the raw author document
  FROM
    `scholar-version2.firestore_export.scholar_raw_author_raw_latest`
  WHERE
    JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') IS NOT NULL -- Ensure author ID exists
  ),
  AuthorPubsData AS (
    -- Use the new base view for author publications list
    SELECT
        scholar_id,
        author_pub_id,
        pub_year
    FROM
      `scholar-version2.statistics.base_author_publications`
    WHERE
      -- Apply consistent year filtering (using fixed year like in publication_citations)
      pub_year > 1950 AND pub_year <= EXTRACT(year FROM CURRENT_DATE())
  ),
  PublicationCounts AS (
    -- Calculate publication counts based on the extracted list
    -- Join with current publication stats to filter by citations > 0
    SELECT
        apd.scholar_id,
        COUNT(apd.author_pub_id) AS total_publications_calculated,
        COUNT(IF(ps.num_citations > 0, apd.author_pub_id, NULL)) as total_publications_with_citations_calculated
    FROM AuthorPubsData apd
    LEFT JOIN
      -- *** ASSUMES publication_stats view is renamed/available as stats_publication_current ***
      `scholar-version2.statistics.stats_publication_current` ps
      ON apd.author_pub_id = ps.author_pub_id
    GROUP BY apd.scholar_id
  ),
  FirstPubYear AS (
    -- Calculate year_of_first_pub based on the extracted list,
    -- considering only publications with citations > 0
    SELECT
        apd.scholar_id,
        MIN(apd.pub_year) AS year_of_first_pub
    FROM AuthorPubsData apd
    JOIN
      -- *** ASSUMES publication_stats view is renamed/available as stats_publication_current ***
      `scholar-version2.statistics.stats_publication_current` ps
      ON apd.author_pub_id = ps.author_pub_id
    WHERE ps.num_citations > 0 -- Filter for cited publications
    GROUP BY apd.scholar_id
  ),
  CombinedData AS (
  -- Combine base author stats with derived publication counts and first pub year
  SELECT
    sd.scholar_id,
    sd.name,
    sd.affiliation,
    sd.email_domain,
    sd.hindex,
    sd.hindex5y,
    sd.citedby,
    sd.citedby5y,
    sd.i10index,
    sd.i10index5y,
    -- Use the counts derived from base_author_publications, defaulting to 0
    COALESCE(pc.total_publications_calculated, 0) AS total_publications,
    COALESCE(pc.total_publications_with_citations_calculated, 0) AS total_publications_with_citations,
    fpy.year_of_first_pub, -- This might be NULL if author has 0 cited pubs
    sd.timestamp AS last_updated -- Timestamp from the raw author document
  FROM
    ScholarData sd
  LEFT JOIN -- Use LEFT JOINs in case author has no publications
    FirstPubYear fpy ON sd.scholar_id = fpy.scholar_id
  LEFT JOIN
    PublicationCounts pc ON sd.scholar_id = pc.scholar_id
  )
-- Final select adding the percentile calculations
SELECT
  *,
  -- Calculate percentiles, partitioning by year_of_first_pub
  -- Authors with NULL year_of_first_pub (no cited pubs) will form their own partition
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY hindex ASC) AS hindex_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY hindex5y ASC) AS hindex5y_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY citedby ASC) AS citedby_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY citedby5y ASC) AS citedby5y_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY i10index ASC) AS i10index_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY i10index5y ASC) AS i10index5y_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY total_publications ASC) AS total_publications_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY total_publications_with_citations ASC) AS total_publications_with_citations_percentile
FROM
  CombinedData;