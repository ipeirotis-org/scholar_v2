#!/usr/bin/env bash
# Exports the per-author inputs of analyze_honors.py and analyze_gs_honors.py from BigQuery to honors/data/.
# The two files hold one row per author (no names or IDs) and are not committed; the aggregate results are.
# Usage: paper/honors/export_honors_samples.sh    DATASET=<name> to read another dataset (default paper_rp)
#   honors_sample.csv     from honors_base (22): every honoree, every author of the two notable groups, and a 10%
#                         sample of all active authors (`in_sample`: authors whose ID hash ends in 0, so the sample
#                         is the same on every run and in every dataset)
#   gs_honors_sample.csv  from gs_honors_base (24): every active Google Scholar profile in a cohort of at least 30
set -euo pipefail
cd "$(dirname "$0")/data"
DATASET="${DATASET:-paper_rp}"
q() { bq query --use_legacy_sql=false --format=csv --max_rows=2000000 --quiet "$1" | grep -v -E "WARNING|Waiting"; }
q "SELECT age, CAST(in_wikidata AS INT64) AS in_wikidata, CAST(in_gs_faculty AS INT64) AS in_gs_faculty,
     CAST(MOD(ABS(FARM_FINGERPRINT(CAST(authorid AS STRING))), 10) = 0 AS INT64) AS in_sample,
     CAST(honored AS INT64) AS honored, CAST(honored_dated AS INT64) AS honored_dated,
     p_pip, p_pip_original, p_h, p_citations, p_articles, p_top10, p_i3, p_mean_pct,
     p_sym0, p_sym25, p_sym60, p_sym75, p_sym90
   FROM \`scholar-version2.$DATASET.honors_base\`
   WHERE honored OR in_wikidata OR in_gs_faculty OR MOD(ABS(FARM_FINGERPRINT(CAST(authorid AS STRING))), 10) = 0
   ORDER BY age, start_year, authorid" > honors_sample.csv
q "SELECT cohort, p_pip, p_sym75, p_pip_original, p_h, p_citations, p_i10, p_articles, p_top10, p_i3, p_mean_pct,
     CAST(honored AS INT64) AS honored, CAST(enwiki AS INT64) AS enwiki, views, CAST(in_wikidata AS INT64) AS in_wikidata
   FROM \`scholar-version2.$DATASET.gs_honors_base\` ORDER BY cohort, scholar_id" > gs_honors_sample.csv
wc -l honors_sample.csv gs_honors_sample.csv
