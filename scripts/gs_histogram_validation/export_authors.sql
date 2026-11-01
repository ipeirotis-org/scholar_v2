WITH auth AS (SELECT JSON_EXTRACT_SCALAR(data, "$.data.scholar_id") sid, timestamp ts, data FROM scholar_raw_data.author),
keep AS (SELECT sid FROM auth GROUP BY sid HAVING COUNT(*) >= 2 AND TIMESTAMP_DIFF(MAX(ts), MIN(ts), DAY) > 400)
SELECT sid, FORMAT_TIMESTAMP("%Y-%m-%dT%H:%M:%S", ts) ts,
  JSON_EXTRACT(data, "$.data.cites_per_year") cpy,
  JSON_EXTRACT_SCALAR(data, "$.data.citedby") citedby,
  TO_JSON_STRING(ARRAY(SELECT AS STRUCT JSON_EXTRACT_SCALAR(p, "$.author_pub_id") pid, SAFE_CAST(JSON_EXTRACT_SCALAR(p, "$.num_citations") AS INT64) nc,
     SAFE_CAST(JSON_EXTRACT_SCALAR(p, "$.bib.pub_year") AS INT64) yr
     FROM UNNEST(JSON_EXTRACT_ARRAY(data, "$.data.publications")) p)) pubs
FROM auth WHERE sid IN (SELECT sid FROM keep)
