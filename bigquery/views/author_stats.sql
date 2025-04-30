CREATE OR REPLACE VIEW `scholar-version2.statistics.author_stats` AS
WITH
  ScholarData AS (
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
    ARRAY_LENGTH(JSON_EXTRACT_ARRAY(DATA, '$.data.publications')) AS total_publications,
    (
    SELECT
      COUNT(*)
    FROM
      UNNEST(JSON_EXTRACT_ARRAY(DATA, '$.data.publications')) AS publication
    WHERE
      CAST(JSON_EXTRACT_SCALAR(publication, '$.num_citations') AS INT64) > 0
      AND CAST(JSON_EXTRACT_SCALAR(publication, '$.bib.pub_year') AS INT64) > 1950
      AND CAST(JSON_EXTRACT_SCALAR(publication, '$.bib.pub_year') AS INT64) <= EXTRACT(year
      FROM
        CURRENT_DATE()) ) AS total_publications_with_citations,
    DATA,
    timestamp
  FROM
    `scholar-version2.firestore_export.scholar_raw_author_raw_latest` ),
  PublicationsData AS (
  SELECT
    scholar_id,
    MIN(CAST(JSON_EXTRACT_SCALAR(publication, '$.bib.pub_year') AS INT64)) AS year_of_first_pub
  FROM
    ScholarData,
    UNNEST(JSON_EXTRACT_ARRAY(ScholarData.data, '$.data.publications')) publication
  WHERE
    CAST(JSON_EXTRACT_SCALAR(publication, '$.num_citations') AS INT64) > 0
    AND CAST(JSON_EXTRACT_SCALAR(publication, '$.bib.pub_year') AS INT64) > 1950
    AND CAST(JSON_EXTRACT_SCALAR(publication, '$.bib.pub_year') AS INT64) <= EXTRACT(year
    FROM
      CURRENT_DATE())
  GROUP BY
    scholar_id ),
  CombinedData AS (
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
    sd.total_publications,
    sd.total_publications_with_citations,
    pd.year_of_first_pub,
    sd.timestamp AS last_updated
  FROM
    ScholarData sd
  JOIN
    PublicationsData pd
  ON
    sd.scholar_id = pd.scholar_id )
SELECT
  *,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY hindex ASC) AS hindex_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY hindex5y ASC) AS hindex5y_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY citedby ASC) AS citedby_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY citedby5y ASC) AS citedby5y_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY i10index ASC) AS i10index_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY i10index5y ASC) AS i10index5y_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY total_publications ASC) AS total_publications_percentile,
  PERCENT_RANK() OVER(PARTITION BY year_of_first_pub ORDER BY total_publications_with_citations ASC) AS total_publications_with_citations_percentile
FROM
  CombinedData