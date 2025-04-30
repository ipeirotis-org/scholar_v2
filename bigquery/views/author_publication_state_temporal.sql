CREATE OR REPLACE VIEW `scholar-version2.statistics.author_publication_state_temporal` AS

WITH AuthorPublications AS (
  -- Extract author_id and their list of publications (author_pub_id and pub_year)
  -- Sourced from the latest Firestore export of author data
  SELECT
    JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') AS scholar_id,
    JSON_EXTRACT_SCALAR(pub, '$.author_pub_id') AS author_pub_id,
    CAST(JSON_EXTRACT_SCALAR(pub, '$.bib.pub_year') AS INT64) AS pub_year
  FROM
    `scholar-version2.firestore_export.scholar_raw_author_raw_latest`,
    UNNEST(JSON_EXTRACT_ARRAY(DATA, '$.data.publications')) AS pub
  WHERE
    -- Ensure we have the necessary IDs and year for joining and filtering
    JSON_EXTRACT_SCALAR(pub, '$.author_pub_id') IS NOT NULL
    AND JSON_EXTRACT_SCALAR(DATA, '$.data.scholar_id') IS NOT NULL
    AND CAST(JSON_EXTRACT_SCALAR(pub, '$.bib.pub_year') AS INT64) IS NOT NULL
),

YearlyPublicationCitations AS (
  -- Use the existing view that calculates yearly and cumulative citations per publication per year
  SELECT
    scholar_id, -- Included in the source view
    author_pub_id,
    pub_year,
    citation_year,      -- This represents the year 'Y' for which we want the state
    cumulative_citations,
    yearly_citations
  FROM
    `scholar-version2.statistics.publication_citations`
)

-- Combine author's publications with their yearly citation state
SELECT
  ap.scholar_id,
  ap.author_pub_id,
  ap.pub_year,
  ypc.citation_year AS state_year, -- Renaming for clarity (this is the year 'Y' the state represents)
  ypc.cumulative_citations AS cumulative_citations_at_state_year,
  ypc.yearly_citations AS yearly_citations_at_state_year
FROM
  AuthorPublications ap
JOIN
  YearlyPublicationCitations ypc ON ap.author_pub_id = ypc.author_pub_id AND ap.scholar_id = ypc.scholar_id
-- The join ensures we only get citation data for publications associated with the author.
-- The publication_citations view inherently ensures citation_year >= pub_year,
-- so we only get states for years at or after publication.

ORDER BY
  ap.scholar_id,
  ap.author_pub_id,
  state_year;