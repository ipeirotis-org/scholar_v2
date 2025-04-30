CREATE OR REPLACE VIEW `scholar-version2.statistics.intermediate_author_publication_state_temporal` AS

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
  -- *** CORRECTED HERE: Use the view that contains the necessary temporal columns ***
  SELECT
    scholar_id,
    author_pub_id,
    pub_year,
    citation_year,          -- Exists in stats_publication_citations_temporal
    cumulative_citations,   -- Exists in stats_publication_citations_temporal
    yearly_citations        -- Exists in stats_publication_citations_temporal
  FROM
    `scholar-version2.statistics.stats_publication_citations_temporal` -- Correct source view
)

-- Combine author's publications with their yearly citation state
SELECT
  ap.scholar_id,
  ap.author_pub_id,
  ap.pub_year,
  ypc.citation_year AS state_year, -- Renaming for clarity
  ypc.cumulative_citations AS cumulative_citations_at_state_year,
  ypc.yearly_citations AS yearly_citations_at_state_year
FROM
  AuthorPublications ap
JOIN
  YearlyPublicationCitations ypc ON ap.author_pub_id = ypc.author_pub_id AND ap.scholar_id = ypc.scholar_id
ORDER BY
  ap.scholar_id,
  ap.author_pub_id,
  state_year;