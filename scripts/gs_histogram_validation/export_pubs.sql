WITH auth AS (SELECT JSON_EXTRACT_SCALAR(data, "$.data.scholar_id") sid, timestamp ts FROM scholar_raw_data.author),
keep AS (SELECT sid FROM auth GROUP BY sid HAVING COUNT(*) >= 2 AND TIMESTAMP_DIFF(MAX(ts), MIN(ts), DAY) > 400),
s AS (SELECT JSON_EXTRACT_SCALAR(data, "$.data.author_pub_id") pid, timestamp ts,
        SAFE_CAST(JSON_EXTRACT_SCALAR(data, "$.data.num_citations") AS INT64) nc,
        JSON_EXTRACT(data, "$.data.cites_per_year") cpy
      FROM scholar_raw_data.pub)
SELECT SPLIT(pid, ":")[SAFE_OFFSET(0)] sid, pid, FORMAT_TIMESTAMP("%Y-%m-%dT%H:%M:%S", ts) ts, nc, cpy
FROM s WHERE SPLIT(pid, ":")[SAFE_OFFSET(0)] IN (SELECT sid FROM keep)
