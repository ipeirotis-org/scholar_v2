#!/usr/bin/env bash
# Builds a table of PiP scores from 25_pip_exact.sql in chunks of career-start years (peers are defined within
# (start_year, age), so chunking does not change any result).
# Usage: paper/sql/run_25.sh <run> [--dry-run]
#   grid   active authors at career age 10, start years 1960-2015, grids K = 40, 200, 1000 and exact (0),
#          tau = 0.5, 0.75, 0.9                                  -> paper_rp.pip_exact_grid (read by 26)
#   min3   authors with at least 3 articles (no h-index rule), start years 1975-2005, ages 3-10 and 15, the
#          production grid K = 40, tau = 0.5                     -> paper_rp.pip_exact_min3 (read by 27)
set -euo pipefail
cd "$(dirname "$0")"
RUN="${1:?usage: run_25.sh grid|min3 [--dry-run]}"
DRY=""; [[ "${2:-}" == "--dry-run" ]] && DRY="--dry_run"
BQ=(bq query --use_legacy_sql=false --quiet)
case "$RUN" in
  grid) OUT=pip_exact_grid; BENCHMARK=active; AGES="10"; GRIDS="40, 200, 1000, 0"; TAUS="0.5, 0.75, 0.9"
        CHUNKS=("1960 1984" "1985 1999" "2000 2007" "2008 2015") ;;
  min3) OUT=pip_exact_min3; BENCHMARK=min3; AGES="3, 4, 5, 6, 7, 8, 9, 10, 15"; GRIDS="40"; TAUS="0.5"
        CHUNKS=("1975 1982" "1983 1989" "1990 1995" "1996 2000" "2001 2005") ;;
  *) echo "unknown run: $RUN"; exit 1 ;;
esac
first=1
for chunk in "${CHUNKS[@]}"; do
  read -r lo hi <<<"$chunk"
  if [[ $first == 1 ]]; then
    prefix="CREATE OR REPLACE TABLE \`scholar-version2.paper_rp.${OUT}\`
PARTITION BY RANGE_BUCKET(start_year, GENERATE_ARRAY(1950, 2026, 1))
CLUSTER BY k_grid, age, authorid
OPTIONS (description = 'PiP from order statistics: benchmark ${BENCHMARK}, ages ${AGES}, grids ${GRIDS}, tau ${TAUS}. Built by paper/sql/run_25.sh ${RUN}')
AS"
  else
    prefix="INSERT INTO \`scholar-version2.paper_rp.${OUT}\`"
  fi
  sql=$(sed -e "s/{LO}/$lo/g; s/{HI}/$hi/g; s/{BENCHMARK}/$BENCHMARK/g; s/{AGES}/$AGES/g; s/{GRIDS}/$GRIDS/g" \
            -e "s/{TAUS}/$TAUS/g" -e "/^{MODE_PREFIX}$/{
r /dev/stdin
d
}" 25_pip_exact.sql <<<"$prefix")
  echo "== $OUT ($BENCHMARK) start years $lo-$hi $(date -u +%H:%M:%S)"
  "${BQ[@]}" $DRY <<<"$sql"
  [[ -n $DRY ]] && exit 0   # the table may not exist yet, so only the first chunk can be dry-run
  first=0
done
