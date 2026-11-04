#!/usr/bin/env bash
# Builds paper_rp.author_pip_by_age from 06_author_pip_by_age.sql in chunks of career-start years.
# Usage: paper/sql/run_06.sh [--dry-run]
#   DATASET=paper_rp_noself  write to and read from another dataset (default paper_rp)
#   ONLY_BENCHMARK=active    build one peer rule only
# Two peer rules (benchmarks), each in six chunks of career-start years. Peers are defined within
# (benchmark, start_year, age), so chunking by start year does not change any result.
set -euo pipefail
cd "$(dirname "$0")"
DRY=""; [[ "${1:-}" == "--dry-run" ]] && DRY="--dry_run"
BQ=(bq query --use_legacy_sql=false --quiet)
CHUNKS=("1955 1969" "1970 1979" "1980 1989" "1990 1999" "2000 2009" "2010 2024")
BENCHMARKS=("active 3" "min3 0")
DATASET="${DATASET:-paper_rp}"
ONLY_BENCHMARK="${ONLY_BENCHMARK:-}"
first=1
for bench in "${BENCHMARKS[@]}"; do
read -r bname minh <<<"$bench"
[[ -n "$ONLY_BENCHMARK" && "$bname" != "$ONLY_BENCHMARK" ]] && continue
for chunk in "${CHUNKS[@]}"; do
  read -r lo hi <<<"$chunk"
  if [[ $first == 1 ]]; then
    prefix="CREATE OR REPLACE TABLE \`scholar-version2.paper_rp.author_pip_by_age\`
PARTITION BY RANGE_BUCKET(start_year, GENERATE_ARRAY(1950, 2026, 1))
CLUSTER BY benchmark, age, authorid
OPTIONS (description = 'Author metrics and step-form PiP-AUC at career ages 1-30, peers as of the evaluation year. Built by paper/sql/run_06.sh')
AS"
  else
    prefix="INSERT INTO \`scholar-version2.paper_rp.author_pip_by_age\`"
  fi
  sql=$(sed -e "s/{LO}/$lo/; s/{HI}/$hi/; s/{MIN_H}/$minh/g; s/{BENCHMARK}/$bname/g" -e "/^{MODE_PREFIX}$/{
r /dev/stdin
d
}" 06_author_pip_by_age.sql <<<"$prefix" | sed "s/\`scholar-version2\.paper_rp\./\`scholar-version2.${DATASET}./g")
  echo "== $DATASET $bname: start years $lo-$hi"
  "${BQ[@]}" $DRY <<<"$sql"
  [[ -n $DRY ]] && exit 0   # the table may not exist yet, so only the first chunk can be dry-run
  first=0
done
done
