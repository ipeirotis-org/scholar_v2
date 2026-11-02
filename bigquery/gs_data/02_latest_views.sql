-- Latest snapshot per author, and the publication list of that snapshot.
CREATE OR REPLACE VIEW `scholar-version2.gs_data.authors_latest` AS
SELECT * EXCEPT (rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY scholar_id ORDER BY fetched_at DESC) AS rn
  FROM `scholar-version2.gs_data.author_snapshots`
)
WHERE rn = 1;

-- Publications are written with the same fetched_at as their author snapshot,
-- so joining on (scholar_id, fetched_at) yields exactly the latest list.
CREATE OR REPLACE VIEW `scholar-version2.gs_data.publications_latest` AS
SELECT p.*
FROM `scholar-version2.gs_data.publication_snapshots` p
JOIN `scholar-version2.gs_data.authors_latest` a
  ON p.scholar_id = a.scholar_id AND p.fetched_at = a.fetched_at;
