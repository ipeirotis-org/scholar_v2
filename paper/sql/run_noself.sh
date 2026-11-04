#!/usr/bin/env bash
# Self-citation robustness: rerun the analysis pipeline on citations without self-citations.
#
# Builds paper_rp_noself.paper_citations_by_year (00), then replays steps 01, 02, 04, 05, 06, 07, 11, 12 and 13
# unchanged except that every `scholar-version2.paper_rp...` name points to paper_rp_noself, and step 01 reads the
# self-free citation table instead of s2_data.paper_citations_by_year. Results land in the same table names inside
# paper_rp_noself, so the main and the self-free results can be compared one to one.
# Usage: paper/sql/run_noself.sh [first_step]   (e.g. "05" to resume)
set -euo pipefail
cd "$(dirname "$0")"
BQ=(bq query --use_legacy_sql=false --quiet)
START="${1:-00}"
to_noself() {
  sed -e 's/`scholar-version2\.paper_rp\([.`]\)/`scholar-version2.paper_rp_noself\1/g' \
      -e 's/`scholar-version2\.s2_data\.paper_citations_by_year`/`scholar-version2.paper_rp_noself.paper_citations_by_year`/g'
}
run() {  # $1 = step label, stdin = SQL; stops the whole run if BigQuery reports an error
  echo "== $1 $(date -u +%H:%M:%S)"
  local out
  if ! out=$("${BQ[@]}" 2>&1); then echo "$out"; echo "FAILED at step $1"; exit 1; fi
  echo "$out" | grep -v -E "WARNING|Waiting" || true
}
for step in 00 01 02 04 05 06 07 11 12 13; do
  [[ "$step" < "$START" ]] && continue
  case "$step" in
    00) run "$step" < 00_noself_citations.sql ;;
    06) DATASET=paper_rp_noself ONLY_BENCHMARK=active ./run_06.sh ;;
    *)  f=$(ls ${step}_*.sql); to_noself < "$f" | run "$step $f" ;;
  esac
done
echo "== done $(date -u +%H:%M:%S)"
