CREATE OR REPLACE VIEW `scholar-version2.gs_statistics.author_fields` AS
-- Level 2: each profiled author's field group and where it came from.
-- field_source 'interests': author_interest_fields (live, from the GS interests);
-- 'coauthors': author_coauthor_fields (rebuilt weekly) for authors whose interests
-- match no rule. Authors with neither get no row.
SELECT scholar_id, field_group, 'interests' AS field_source
FROM `scholar-version2.gs_statistics.author_interest_fields`
UNION ALL
SELECT c.scholar_id, c.field_group, 'coauthors' AS field_source
FROM `scholar-version2.gs_statistics.author_coauthor_fields` c
LEFT JOIN `scholar-version2.gs_statistics.author_interest_fields` i USING (scholar_id)
WHERE i.scholar_id IS NULL;
