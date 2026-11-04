#!/usr/bin/env bash
# Builds paper_rp.sym_by_age from 15_sym_by_age.sql in chunks of career-start years (peers are defined within
# (start_year, age), so chunking does not change any result).
# Usage: paper/sql/run_15.sh [--dry-run]    DATASET=paper_rp_noself to use another dataset
set -euo pipefail
cd "$(dirname "$0")"
DRY=""; [[ "${1:-}" == "--dry-run" ]] && DRY="--dry_run"
BQ=(bq query --use_legacy_sql=false --quiet)
DATASET="${DATASET:-paper_rp}"
CHUNKS=("1955 1969" "1970 1979" "1980 1989" "1990 1999" "2000 2009" "2010 2024")
first=1
for chunk in "${CHUNKS[@]}"; do
  read -r lo hi <<<"$chunk"
  if [[ $first == 1 ]]; then
    prefix="CREATE OR REPLACE TABLE \`scholar-version2.paper_rp.sym_by_age\`
PARTITION BY RANGE_BUCKET(start_year, GENERATE_ARRAY(1950, 2026, 1))
CLUSTER BY age, authorid
OPTIONS (description = 'Symmetric PiP (tau 0, 0.25, 0.5) at career ages 1-30 for active peers. Built by paper/sql/run_15.sh')
AS"
  else
    prefix="INSERT INTO \`scholar-version2.paper_rp.sym_by_age\`"
  fi
  sql=$(sed -e "s/{LO}/$lo/g; s/{HI}/$hi/g" -e "/^{MODE_PREFIX}$/{
r /dev/stdin
d
}" 15_sym_by_age.sql <<<"$prefix" | sed "s/\`scholar-version2\.paper_rp\./\`scholar-version2.${DATASET}./g")
  echo "== $DATASET start years $lo-$hi $(date -u +%H:%M:%S)"
  "${BQ[@]}" $DRY <<<"$sql"
  [[ -n $DRY ]] && exit 0
  first=0
done
