-- Field groups for authors whose interests match no rule, from their co-authors.
--
-- Takes the group most of an author's profiled co-authors have (links in either
-- direction: A lists B on their profile, or B lists A), from
-- author_interest_fields, if at least 2 co-authors have a group and the top group
-- holds more than half of them. Only authors without an interest-based group get
-- a row. A table because it reads every profile (~6 s as a view); rebuilt with
-- the dist_* tables by gs_crawler CRAWL_MODE=materialize.
--
-- PREREQUISITE: author_interest_fields view must exist.

CREATE OR REPLACE TABLE `scholar-version2.gs_statistics.author_coauthor_fields`
AS
WITH
  Links AS (
    SELECT DISTINCT scholar_id, other_id FROM (
      SELECT a.scholar_id, c.scholar_id AS other_id
      FROM `scholar-version2.gs_data.authors_latest` a, UNNEST(a.coauthors) c
      UNION ALL
      SELECT c.scholar_id, a.scholar_id
      FROM `scholar-version2.gs_data.authors_latest` a, UNNEST(a.coauthors) c
    )
    WHERE scholar_id != other_id
  ),
  CoauthorVotes AS (
    SELECT l.scholar_id, f.field_group, COUNT(*) AS votes
    FROM Links l
    JOIN `scholar-version2.gs_statistics.author_interest_fields` f ON f.scholar_id = l.other_id
    GROUP BY l.scholar_id, f.field_group
  ),
  Best AS (
    SELECT scholar_id,
           ARRAY_AGG(STRUCT(field_group, votes) ORDER BY votes DESC, field_group LIMIT 1)[OFFSET(0)] AS top,
           SUM(votes) AS total
    FROM CoauthorVotes
    GROUP BY scholar_id
  )
SELECT t.scholar_id, t.top.field_group AS field_group, t.top.votes AS coauthor_votes, t.total AS coauthors_with_field
FROM Best t
JOIN `scholar-version2.gs_data.authors_latest` a USING (scholar_id)
LEFT JOIN `scholar-version2.gs_statistics.author_interest_fields` i USING (scholar_id)
WHERE i.scholar_id IS NULL
  AND t.total >= 2 AND t.top.votes * 2 > t.total;
