#!/usr/bin/env bash
# Runs 21_early_detection.sql for one eligibility rule.
# Usage: paper/sql/run_21.sh active|min3 [--dry-run]
#   active  peers with three articles and an h-index of three (sym_by_age from 15) -> early_detection_base / _results
#   min3    peers with three articles, no h-index condition (sym_by_age_min3 from 27) -> ..._min3
set -euo pipefail
cd "$(dirname "$0")"
RULE="${1:?usage: run_21.sh active|min3 [--dry-run]}"
DRY=""; [[ "${2:-}" == "--dry-run" ]] && DRY="--dry_run"
case "$RULE" in
  active) SYM_TABLE=sym_by_age; SUFFIX="" ;;
  min3) SYM_TABLE=sym_by_age_min3; SUFFIX="_min3" ;;
  *) echo "unknown rule: $RULE"; exit 1 ;;
esac
echo "== early detection, rule $RULE $(date -u +%H:%M:%S)"
sed -e "s/{SYM_TABLE}/$SYM_TABLE/g; s/{SUFFIX}/$SUFFIX/g" 21_early_detection.sql \
  | bq query --use_legacy_sql=false --quiet $DRY
