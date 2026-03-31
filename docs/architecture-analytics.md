# Component 2: Analytics

> Part of [System Architecture](ARCHITECTURE.md)

**Purpose:** Compute all metrics, percentiles, and scores from S2 data in BigQuery.

**Input:** BigQuery base tables in `s2_data` dataset (papers, citations, authors, derived tables).

**Output:** BigQuery materialized tables in `statistics` dataset.

## What it computes

1. **Publication metrics:** Citation counts and citation percentiles per publication, partitioned by publication year
2. **Author metrics:** h-index, citations, i10-index (and 5-year variants), total publications — with percentiles by career-stage cohort (year of first publication)
3. **PiP-AUC scores:** Paper-in-Percentile Area Under Curve via trapezoidal integration, with percentile ranking
4. **Temporal metrics:** Historical evolution of author h-index, citations, i10-index over time
5. **Coauthor network:** Coauthor graph extracted from author data

## Three-tier architecture

```
Tier 1 — Raw Statistics (no percentiles, no PERCENT_RANK):
  base_author_publications            — author → publication list
  stats_publication_current           — num_citations, metadata
  stats_publication_citations_temporal — yearly/cumulative citations
  stats_author_current                — hindex, citedby, i10index, total_publications
  stats_author_metrics_temporal_view  — per-author per-year metrics
  stats_author_pip_scores_current     — pip_auc_score (no percentile)
  stats_author_pip_scores_temporal_view — temporal pip_auc_score (no percentile)
  coauthor_network, coauthors_to_add
  intermediate_author_publication_state_temporal
  stats_author_publication_pip_inputs_current — PiP chart X/Y coordinates

Tier 2 — Distribution Tables (ONLY place PERCENT_RANK runs):
  dist_publication_citations          — (pub_year, num_citations) → percentile
  dist_publication_citations_temporal — (pub_year, citation_year, age) → 4 percentiles
  dist_author_metrics                 — (cohort, metric, value) → percentile (8 metrics)
  dist_author_metrics_temporal        — (cohort, state_year, metric, value) → percentile (7 metrics)
  dist_pip_auc_scores                 — (cohort, pip_auc_score) → percentile
  dist_pip_auc_scores_temporal        — (cohort, state_year, pip_auc_score) → percentile

Tier 3 — Ranked (cheap JOINs of Tier 1 + Tier 2):
  ranked_publication_current            — adds num_citations_percentile
  ranked_publication_citations_temporal  — adds 4 percentile columns
  ranked_author_current                 — adds 8 percentile columns
  ranked_author_metrics_temporal        — adds 7 percentile columns
  ranked_author_pip_scores_current      — adds pip_auc_score_percentile
  ranked_author_pip_scores_temporal     — adds pip_auc_score_percentile
```

## Materialization strategy

> Full details: [architecture-analytics-details.md](architecture-analytics-details.md)

**16 tables are materialized monthly** during S2 dataset ingestion by `dataset_ingestion/materialize_tables.py`. Only tables that are directly queried by the app, used as inputs by downstream materializations, or needed for percentile lookups (dist tables) are materialized. Intermediate views (e.g., `intermediate_author_publication_state_temporal`) are left as views — their output is consumed inline by downstream materialized tables.

Stats and ranked views get `_table` suffixed counterparts (e.g., `stats_author_current_table`); distribution tables are materialized in-place as `dist_*` (no `_table` suffix). The Cache Layer's `USE_MATERIALIZED_TABLES` config flag (default: `false`) controls whether queries hit materialized `_table` versions or live views; must be set via env var in production.

**16 tables materialized across 6 levels:**

| Level | Tables | Count |
|-------|--------|-------|
| 1 | stats_author_current, dist_publication_citations, dist_author_metrics | 3 |
| 2 | stats_publication_citations_temporal, dist_publication_citations_temporal | 2 |
| 3 | stats_author_metrics_temporal, stats_author_publication_pip_inputs_current, ranked_author_current, ranked_publication_citations_temporal | 4 |
| 4 | stats_author_pip_scores_current, dist_pip_auc_scores, dist_author_metrics_temporal | 3 |
| 5 | ranked_author_pip_scores_current, ranked_author_metrics_temporal, stats_author_pip_scores_temporal | 3 |
| 6 | dist_pip_auc_scores_temporal | 1 |

**Skipped (intermediate views consumed inline by downstream tables):**
- `base_author_publications`, `stats_publication_current` (L1)
- `intermediate_author_publication_state_temporal` (L2)
- `ranked_author_pip_scores_temporal` (L7 — not queried by app)

**Fallback materialization:** GitHub Actions workflow `bigquery-materialize-all.yml` runs on the 1st of each month at 08:00 UTC as a safety net, skipping if materialization already succeeded for the latest release.

## Benchmark populations

Author-level distribution tables include two benchmarks:
- `all_authors` — full S2 population (~99.5M)
- `active_authors` — hindex ≥ 3 AND total_publications ≥ 3 (meaningful differentiation)

Ranked tables default to `active_authors` for user-facing percentiles.

## Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery base tables (`s2_data.papers`, `s2_data.citations`, `s2_data.authors`, derived tables) | |
| **Writes** | | BigQuery views and materialized tables (`statistics.*`) |

## Implementation

| File | Role |
|---|---|
| `bigquery/statistics/*.sql` | All view and table definitions (22 SQL files) |
| `bigquery/coauthor_network/*.sql` | Coauthor graph views |
| `dataset_ingestion/materialize_tables.py` | Selective DAG materialization (6 levels, 17 tables, called during ingestion) |
| `.github/workflows/bigquery-views.yml` | CI/CD: deploy views in dependency order (on SQL file changes) |
| `.github/workflows/bigquery-materialize-all.yml` | Fallback: monthly safety-net materialization |
