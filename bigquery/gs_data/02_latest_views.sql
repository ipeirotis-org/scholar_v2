-- Profiles Google Scholar no longer serves (deleted or made private by their owner):
-- the latest crawl failed with "profile not found" after the latest snapshot
-- (2026-11-04: Takashi Taniguchi, Paul S. Weiss, Steven Bellovin and others; their
-- old IDs return 404 on scholar.google.com). They are left out of authors_latest,
-- so they leave the statistics, the percentile population, the search index and the
-- refresh schedule, and the cache layer stops paying for crawls of them. A later
-- successful crawl writes a newer snapshot, which brings the profile back.
CREATE OR REPLACE VIEW `scholar-version2.gs_data.retired_profiles` AS
SELECT f.scholar_id, MAX(f.failed_at) AS retired_at
FROM `scholar-version2.gs_data.crawl_failures` f
LEFT JOIN (
  SELECT scholar_id, MAX(fetched_at) AS last_fetched
  FROM `scholar-version2.gs_data.author_snapshots`
  GROUP BY scholar_id
) s USING (scholar_id)
WHERE STARTS_WITH(f.error, 'profile not found')
GROUP BY f.scholar_id
HAVING MAX(f.failed_at) > IFNULL(ANY_VALUE(s.last_fetched), TIMESTAMP '1970-01-01');

-- Latest snapshot per author (retired profiles excluded), and the publication list
-- of that snapshot.
CREATE OR REPLACE VIEW `scholar-version2.gs_data.authors_latest` AS
SELECT * EXCEPT (rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY scholar_id ORDER BY fetched_at DESC) AS rn
  FROM `scholar-version2.gs_data.author_snapshots`
)
WHERE rn = 1
  AND scholar_id NOT IN (SELECT scholar_id FROM `scholar-version2.gs_data.retired_profiles`);

-- Publications are written with the same fetched_at as their author snapshot,
-- so joining on (scholar_id, fetched_at) yields exactly the latest list.
CREATE OR REPLACE VIEW `scholar-version2.gs_data.publications_latest` AS
SELECT p.*
FROM `scholar-version2.gs_data.publication_snapshots` p
JOIN `scholar-version2.gs_data.authors_latest` a
  ON p.scholar_id = a.scholar_id AND p.fetched_at = a.fetched_at;
