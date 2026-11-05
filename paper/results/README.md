# Result tables

These CSV files are exports of the result tables that the queries in `../sql/` write to BigQuery. They hold the
aggregates behind the numbers in the manuscript: one row per setting, start year, field, age pair or predictor, never
one row per author or article. They were exported on 5 November 2026.

The per-author and per-article tables (about 312 GB in `paper_rp` and 215 GB in `paper_rp_noself`) are not copied
here. They stay in the BigQuery datasets `scholar-version2.paper_rp` and `scholar-version2.paper_rp_noself`, which
anyone signed in to Google Cloud can query; queries are billed to the reader's own project. The per-article Google
Scholar profile data are not redistributed; the Google Scholar tables in `paper_rp` hold per-profile scores only.

Columns that hold arrays or records in BigQuery are written as JSON strings.

## `paper_rp/` (main analysis)

| Table | Built by | Contents |
|---|---|---|
| `paper_percentile_stability` | `03_paper_percentile_stability.sql` | Correlation of paper percentiles between ages, by field and publication year |
| `author_stability` | `08_author_stability.sql` | Correlation of scholar percentiles between career ages, by benchmark, start year and indicator |
| `survivorship_auc`, `survivorship_deciles` | `09_survivorship.sql` | ROC AUC of age-5 percentiles for later publishing; publishing rates by decile |
| `prediction_results` | `11_prediction.sql` | Spearman and AUC of raw and percentile predictors for future work |
| `variant_prediction`, `variant_profile` | `13_variant_benchmarks.sql` | Prediction benchmarks for PiP variants; what each variant tracks at career age 10 |
| `gs_variant_comparison` | `14_gs_variant_impact.sql` | Google Scholar profiles: the website's score against the PiP variants |
| `sym_stability`, `sym_survivorship_auc`, `sym_decomposition` | `16_sym_stability_survivorship.sql` | PiP stability between career ages; survival AUC; variance split of log PiP |
| `field_prediction`, `field_profile` | `17_fields.sql` | Prediction within each main field; score levels by field at career age 10 |
| `field_normalized_prediction` | `18_field_normalized.sql` | Pooled against field-normalized PiP on pooled and field-normalized outcomes |
| `baseline_prediction`, `baseline_prediction_by_cohort` | `20_percentile_baselines.sql` | PiP against percentile-based indicators, pooled and per start year |
| `early_detection_results`, `early_detection_results_min3` | `21_early_detection.sql` | Early detection by population, measurement age and predictor (main and three-article rules) |
| `tau_holdout` | `23_tau_holdout.sql` | Prediction by choice (1975-1990) and test (1991 and later) cohorts |
| `grid_cohorts`, `grid_prediction`, `grid_convergence`, `grid_validation` | `26_grid_convergence.sql` | The 40-class grid against the exact integral |
| `eligibility_excluded`, `eligibility_prediction` | `27_eligibility.sql` | Exclusions by the h-index condition; prediction under the main and three-article rules |

## `paper_rp_noself/` (without self-citations)

`prediction_results`, `variant_prediction` and `variant_profile`, built by `sql/run_noself.sh`, which replays the
main queries with every self-citation removed.
