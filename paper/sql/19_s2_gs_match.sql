-- 19: Match Google Scholar profiles to S2 authors through their publications, and compare the two sources.
--
-- 1. GS works and S2 papers match on normalized title (lowercase, accents and punctuation removed, at least 25
--    characters so generic titles cannot match) and publication year within one year.
-- 2. For each GS profile, the S2 counterpart is the S2 author, among those whose surname matches the profile's, who
--    holds the most matched works. S2 IDs that hold at least 2 of the person's matched works and have the same surname
--    count as fragments of the same person (S2 disambiguation splits): n_s2_ids. n_s2_ids_initial also requires the
--    same first initial, which excludes most co-authors who merely share the surname.
-- Output `gs_s2_match`: one row per GS profile with a counterpart. `gs_s2_comparison`: the counterpart's S2 scores
-- at evaluation year 2025 (career age <= 30, i.e. S2 start year >= 1995) next to the GS profile's.
CREATE OR REPLACE TABLE `scholar-version2.paper_rp.gs_s2_match`
OPTIONS (description = 'GS profile -> S2 author matches via publication titles. Built by paper/sql/19_s2_gs_match.sql')
AS
WITH
  gs_authors AS (
    SELECT a.scholar_id, l.name,
      LOWER(REGEXP_EXTRACT(REGEXP_REPLACE(NORMALIZE(l.name, NFKD), r'\p{M}', ''), r'([A-Za-z\-]+)\s*$')) AS surname,
      LOWER(SUBSTR(REGEXP_REPLACE(NORMALIZE(l.name, NFKD), r'[^A-Za-z]', ''), 1, 1)) AS initial
    FROM `scholar-version2.gs_statistics.stats_author_current` AS a
    JOIN `scholar-version2.gs_data.authors_latest` AS l USING (scholar_id)
  ),
  gs_pubs AS (
    SELECT p.scholar_id, p.author_pub_id, p.pub_year,
      REGEXP_REPLACE(LOWER(REGEXP_REPLACE(NORMALIZE(p.title, NFKD), r'\p{M}', '')), r'[^a-z0-9]', '') AS nt
    FROM `scholar-version2.gs_data.publications_latest` AS p
    JOIN gs_authors USING (scholar_id)
    WHERE p.title IS NOT NULL AND p.pub_year IS NOT NULL
  ),
  gs_eligible AS (SELECT * FROM gs_pubs WHERE LENGTH(nt) >= 25),
  s2_papers AS (
    SELECT corpusid, year,
      REGEXP_REPLACE(LOWER(REGEXP_REPLACE(NORMALIZE(title, NFKD), r'\p{M}', '')), r'[^a-z0-9]', '') AS nt
    FROM `scholar-version2.s2_data.papers`
    WHERE title IS NOT NULL AND year BETWEEN 1950 AND 2026
  ),
  matched AS (
    SELECT DISTINCT g.scholar_id, g.author_pub_id, s.corpusid
    FROM gs_eligible AS g
    JOIN s2_papers AS s ON s.nt = g.nt AND ABS(s.year - g.pub_year) <= 1
  ),
  s2_names AS (
    SELECT authorid, name,
      LOWER(REGEXP_EXTRACT(REGEXP_REPLACE(NORMALIZE(name, NFKD), r'\p{M}', ''), r'([A-Za-z\-]+)\s*$')) AS surname,
      LOWER(SUBSTR(REGEXP_REPLACE(NORMALIZE(name, NFKD), r'[^A-Za-z]', ''), 1, 1)) AS initial
    FROM `scholar-version2.s2_data.authors`
  ),
  candidates AS (
    SELECT m.scholar_id, b.authorid, COUNT(DISTINCT m.author_pub_id) AS n_matched
    FROM matched AS m
    JOIN `scholar-version2.s2_data.author_paper_bridge` AS b ON b.corpusid = m.corpusid
    GROUP BY m.scholar_id, b.authorid
  ),
  same_name AS (
    SELECT c.*, n.name AS s2_name, n.initial = g.initial AS same_initial
    FROM candidates AS c
    JOIN s2_names AS n ON n.authorid = c.authorid
    JOIN gs_authors AS g ON g.scholar_id = c.scholar_id
    WHERE n.surname = g.surname
  ),
  per_profile AS (
    SELECT scholar_id,
      ARRAY_AGG(STRUCT(authorid, n_matched, s2_name) ORDER BY n_matched DESC, authorid LIMIT 1)[OFFSET(0)] AS best,
      COUNTIF(n_matched >= 2) AS n_s2_ids,
      COUNTIF(n_matched >= 2 AND same_initial) AS n_s2_ids_initial,
      SUM(n_matched) AS matched_over_ids
    FROM same_name
    GROUP BY scholar_id
  ),
  counts AS (
    SELECT g.scholar_id, COUNT(*) AS n_works, COUNTIF(LENGTH(g.nt) >= 25) AS n_eligible,
      COUNT(DISTINCT m.author_pub_id) AS n_found_in_s2
    FROM gs_pubs AS g
    LEFT JOIN (SELECT DISTINCT scholar_id, author_pub_id FROM matched) AS m USING (scholar_id, author_pub_id)
    GROUP BY g.scholar_id
  )
SELECT
  a.scholar_id, a.name AS gs_name, c.n_works, c.n_eligible, c.n_found_in_s2,
  p.best.authorid AS s2_authorid, p.best.s2_name AS s2_name, p.best.n_matched AS n_matched_best,
  p.n_s2_ids, p.n_s2_ids_initial, p.matched_over_ids
FROM gs_authors AS a
JOIN counts AS c USING (scholar_id)
JOIN per_profile AS p USING (scholar_id);

CREATE OR REPLACE TABLE `scholar-version2.paper_rp.gs_s2_comparison`
OPTIONS (description = 'GS profile scores next to the matched S2 author scores (eval year 2025). Built by paper/sql/19_s2_gs_match.sql')
AS
SELECT
  m.*,
  gs.year_of_first_pub AS gs_start, gs.hindex AS gs_h, gs.citedby AS gs_citations,
  gs.total_publications_with_citations AS gs_cited_works,
  gv.sym_tau50 AS gs_sym_tau50, gv.pip_live AS gs_pip, gv.cohort AS gs_cohort,
  s.start_year AS s2_start, s.age AS s2_age, s.n_articles AS s2_articles, s.h_index AS s2_h, s.citations AS s2_citations,
  s.sym_tau50 AS s2_sym_tau50, s.pip AS s2_pip
FROM `scholar-version2.paper_rp.gs_s2_match` AS m
JOIN `scholar-version2.gs_statistics.stats_author_current` AS gs USING (scholar_id)
LEFT JOIN `scholar-version2.paper_rp.gs_variant_scores` AS gv USING (scholar_id)
LEFT JOIN `scholar-version2.paper_rp.sym_by_age` AS s
  ON s.authorid = m.s2_authorid AND s.start_year + s.age = 2025;
