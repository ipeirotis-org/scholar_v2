#!/usr/bin/env bash
# Exports the result tables written by the steps that depend on the peer standing (sql/run_standing.sh: 12-28, and
# steps 12-13 of sql/run_noself.sh) from BigQuery to paper/results/<dir>/*.csv, rows sorted by their key columns.
# Usage: paper/results/export_results.sh
#   DATASET=<name>         main dataset to read (default paper_rp); files go to results/paper_rp/
#   DATASET_NOSELF=<name>  self-citation-free dataset (default paper_rp_noself); files go to results/paper_rp_noself/
# Arrays are written as JSON strings. The tables of steps 01-11 (paper_percentile_stability, author_stability,
# survivorship_*, prediction_results) do not depend on the standing and are not re-exported here.
set -euo pipefail
cd "$(dirname "$0")"
DATASET="${DATASET:-paper_rp}"
DATASET_NOSELF="${DATASET_NOSELF:-paper_rp_noself}"
q() { bq query --use_legacy_sql=false --format=csv --max_rows=1000000 --quiet "$1" | grep -v -E "WARNING|Waiting"; }
dump() {  # $1 = dataset, $2 = output dir, $3 = table, $4 = ORDER BY list, $5 = optional REPLACE clause for arrays
  q "SELECT * ${5:-} FROM \`scholar-version2.$1.$3\` ORDER BY $4" > "$2/$3.csv"
  echo "$2/$3.csv $(($(wc -l < "$2/$3.csv") - 1)) rows"
}
M=paper_rp
dump "$DATASET" $M variant_prediction "scenario, setting, variant"
dump "$DATASET" $M variant_profile "variant"
dump "$DATASET" $M gs_variant_comparison "variant" "REPLACE (TO_JSON_STRING(quantiles) AS quantiles)"
dump "$DATASET" $M sym_stability "benchmark, start_year, metric, t1, t2"
dump "$DATASET" $M sym_survivorship_auc "cohort, metric, outcome"
dump "$DATASET" $M sym_decomposition "age" "REPLACE (TO_JSON_STRING(q_deciles) AS q_deciles)"
dump "$DATASET" $M field_prediction "main_field, predictor"
dump "$DATASET" $M field_profile "main_field"
dump "$DATASET" $M field_normalized_prediction "predictor, outcome"
dump "$DATASET" $M baseline_prediction "scenario, setting, predictor"
dump "$DATASET" $M baseline_prediction_by_cohort "setting, start_year, predictor"
dump "$DATASET" $M early_detection_results "pop_age, t, predictor"
dump "$DATASET" $M early_detection_results_min3 "pop_age, t, predictor"
dump "$DATASET" $M tau_holdout "setting, sample, predictor"
dump "$DATASET" $M grid_cohorts "tau_pct, k_grid, start_year"
dump "$DATASET" $M grid_convergence "tau_pct, k_grid"
dump "$DATASET" $M grid_prediction "tau_pct, k_grid"
dump "$DATASET" $M grid_validation "tau_pct"
dump "$DATASET" $M eligibility_excluded "age, main_field"
dump "$DATASET" $M eligibility_prediction "rule, age, predictor"
dump "$DATASET" $M tie_rule_comparison "rule, tau_pct"
N=paper_rp_noself
dump "$DATASET_NOSELF" $N variant_prediction "scenario, setting, variant"
dump "$DATASET_NOSELF" $N variant_profile "variant"
