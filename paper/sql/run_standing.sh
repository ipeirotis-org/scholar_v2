#!/usr/bin/env bash
# Replays every step that depends on the peer standing g (the tie rule of PiP): 12-18, the comparison of 19, and
# 20-28. Steps 00-11 do not depend on it. Results land in the same table names.
# Usage: paper/sql/run_standing.sh [first_step]   (e.g. "20" to resume; steps are listed in STEPS below)
#   DATASET=<name>  read from and write to another dataset (default paper_rp). The dataset must already hold the
#                   tables of steps 00-11 and 19 that these steps read: author_pip_by_age, author_articles,
#                   article_percentiles, citation_percentile_lookup, prediction_base, survivorship_base, gs_s2_match
#                   and the wikidata_* tables (table clones are enough).
# Steps 14, the comparison of 19 and 24 read the live Google Scholar tables (gs_statistics), so their results move
# with the profile data.
set -euo pipefail
cd "$(dirname "$0")"
export DATASET="${DATASET:-paper_rp}"
BQ=(bq query --use_legacy_sql=false --quiet)
START="${1:-12}"
STEPS=(12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 27b 28)
to_dataset() { sed "s/\`scholar-version2\.paper_rp\./\`scholar-version2.${DATASET}./g"; }
run() {  # $1 = step label, stdin = SQL; stops the whole run if BigQuery reports an error
  echo "== $DATASET $1 $(date -u +%H:%M:%S)"
  local out
  if ! out=$("${BQ[@]}" 2>&1); then echo "$out"; echo "FAILED at step $1"; exit 1; fi
  echo "$out" | grep -v -E "WARNING|Waiting" || true
}
for step in "${STEPS[@]}"; do
  [[ "$step" < "$START" ]] && continue
  case "$step" in
    15) ./run_15.sh ;;
    # the match of GS profiles to S2 authors does not depend on the standing; only the comparison is replayed
    19) sed -n '/^CREATE OR REPLACE TABLE `scholar-version2\.paper_rp\.gs_s2_comparison`/,$p' 19_s2_gs_match.sql \
          | to_dataset | run "19 gs_s2_comparison" ;;
    21) ./run_21.sh active ;;
    25) ./run_25.sh grid; ./run_25.sh min3 ;;   # read by 26 and 27
    27b) ./run_21.sh min3 ;;                    # early detection under the three-article rule; reads 27's table
    *)  f=$(ls ${step}_*.sql); to_dataset < "$f" | run "$step $f" ;;
  esac
done
echo "== done $(date -u +%H:%M:%S)"
