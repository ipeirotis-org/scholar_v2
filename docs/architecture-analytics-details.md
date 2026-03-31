# Analytics Framework: BigQuery Views, Materialization, and Cost

This document describes the full analytics computation pipeline — how Semantic Scholar data in BigQuery becomes the percentiles, PiP-AUC scores, and temporal metrics served by the web app.

## Design Principles

### 1. Three roles: Stats → Distributions → Ranked

Every metric family follows the same pattern:

- **Stats (`stats_*`):** Compute actual metric values only. No percentiles, no `PERCENT_RANK()`.
- **Distributions (`dist_*`):** The **only** place `PERCENT_RANK()` runs. Small materialized tables mapping (partition_key, metric_value) → percentile.
- **Ranked (`ranked_*`):** Cheap JOINs of stats + distributions. Add percentile columns via floor lookups. The app queries these.

However, these roles do **not** form three independent tiers. The PiP pipeline creates cross-role dependencies: `stats_author_publication_pip_inputs_current` reads from `ranked_publication_current` and `dist_author_metrics`. This means the real dependency graph has **7 topological levels** (1–7).

### 2. Materialize everything

All views are materialized into tables during monthly ingestion. S2 data is static between bulk loads — live views waste compute on every query. The Cache Layer's `USE_MATERIALIZED_TABLES` config flag controls whether queries hit materialized `_table` versions or live views. Currently defaults to `false` in code; must be set via env var in production after materialization completes.

### 3. Refresh frequency matches data change rate

S2 releases weekly diffs; we ingest monthly. 16 analytics tables are materialized in one pass after each ingestion cycle.

---

## View Dependency DAG

The complete dependency graph, organized by topological level. Every node depends **only** on nodes at lower levels. All levels are materialized into tables during monthly ingestion.

```
Level 1: Foundation (raw S2 data only)
  ┌─────────────────────────────────────────────┐
  │  TABLES (materialized)                       │
  │    base_author_publications_table            │
  │    stats_publication_current_table           │
  │    stats_author_current_table                │
  │    dist_publication_citations                │
  │    dist_author_metrics                       │
  └─────────────────────────────────────────────┘
                       │
                       ▼
Level 2: Temporal Foundation + First Ranked
  ┌──────────────────────────────────────────────────────────────┐
  │  TABLES (materialized)                                       │
  │    stats_publication_citations_temporal_table                 │
  │    ranked_publication_current_table                           │
  │    intermediate_author_publication_state_temporal_table       │
  │    dist_publication_citations_temporal                        │
  └──────────────────────────────────────────────────────────────┘
                       │
                       ▼
Level 3: Temporal Stats + PiP Inputs + More Ranked
  ┌──────────────────────────────────────────────────────────────┐
  │  TABLES (materialized)                                       │
  │    stats_author_metrics_temporal_table                        │
  │    stats_author_publication_pip_inputs_current_table          │
  │    ranked_author_current_table                                │
  │    ranked_publication_citations_temporal_table                 │
  └──────────────────────────────────────────────────────────────┘
                       │
                       ▼
Level 4: PiP Scores + Higher Distributions
  ┌──────────────────────────────────────────────────────────────┐
  │  TABLES (materialized)                                       │
  │    stats_author_pip_scores_current_table                      │
  │    dist_pip_auc_scores                                        │
  │    dist_author_metrics_temporal                               │
  └──────────────────────────────────────────────────────────────┘
                       │
                       ▼
Level 5: Ranked PiP + Temporal Ranked + Temporal PiP Stats
  ┌──────────────────────────────────────────────────────────────┐
  │  TABLES (materialized)                                       │
  │    ranked_author_pip_scores_current_table                     │
  │    ranked_author_metrics_temporal_table                       │
  │    stats_author_pip_scores_temporal_table                     │
  └──────────────────────────────────────────────────────────────┘
                       │
                       ▼
Level 6: Temporal PiP Distribution
  ┌──────────────────────────────────────────────────────────────┐
  │  TABLES (materialized)                                       │
  │    dist_pip_auc_scores_temporal                               │
  └──────────────────────────────────────────────────────────────┘
                       │
                       ▼
Level 7: Temporal PiP Ranked
  ┌──────────────────────────────────────────────────────────────┐
  │  TABLES (materialized)                                       │
  │    ranked_author_pip_scores_temporal_table                    │
  └──────────────────────────────────────────────────────────────┘
```

### Why 7 levels?

The depth comes from two sources:

1. **PiP inputs read ranked data.** `stats_author_publication_pip_inputs_current` needs `ranked_publication_current` (which itself needs `dist_publication_citations`). This pushes the PiP pipeline deeper than basic stats.

2. **The temporal PiP pipeline stacks three stats→dist→ranked cycles.** Publications temporal → author temporal → PiP temporal, each cycle adding 2 levels.

---

## Pipelines

The DAG decomposes into six pipelines that share data across levels.

### Pipeline A: Publications (current)

```
L1  stats_publication_current  ──► L1  dist_publication_citations
                                         │
                                         ▼
                                   L2  ranked_publication_current
```

### Pipeline B: Authors (current)

```
L1  base_author_publications ─┐
L1  stats_publication_current ─┤──► L1  stats_author_current ──► L1  dist_author_metrics
                               │                                       │
                               │                                       ▼
                               │                                 L3  ranked_author_current
```

### Pipeline C: Publications (temporal)

```
L2  stats_publication_citations_temporal ──► L2  dist_publication_citations_temporal
                                                   │
                                                   ▼
                                             L3  ranked_publication_citations_temporal
```

### Pipeline D: Authors (temporal)

```
L2  stats_publication_citations_temporal ─┐
L2  intermediate_*  ──────────────────────┤──► L3  stats_author_metrics_temporal
                                          │                │
                                          │                ▼
                                          └──► L4  dist_author_metrics_temporal
                                                       │
                                                       ▼
                                               L5  ranked_author_metrics_temporal
```

### Pipeline E: PiP (current)

```
L2  ranked_publication_current  ─┐
L1  dist_author_metrics ─────────┤──► L3  stats_author_publication_pip_inputs_current
L1  stats_author_current ────────┤               │
                                 │               ▼
                                 │     L4  stats_author_pip_scores_current
                                 │     L4  dist_pip_auc_scores
                                 │               │
                                 │               ▼
                                 └───► L5  ranked_author_pip_scores_current
```

### Pipeline F: PiP (temporal)

```
L2  intermediate_*  ─────────────────┐
L2  dist_publication_citations_temp ─┤
L3  stats_author_metrics_temp  ──────┤──► L5  stats_author_pip_scores_temporal
L4  dist_author_metrics_temporal ────┘               │
                                                     ▼
                                             L6  dist_pip_auc_scores_temporal
                                                     │
                                                     ▼
                                             L7  ranked_author_pip_scores_temporal
```

---

## View Details

### Level 1: Foundation

#### base_author_publications
Extracts (authorid, corpusid, pub_year) from S2 data. Used by stats_author_current.

#### stats_publication_current
Per-publication: `corpusid`, `author_pub_id`, `pub_year`, `title`, `num_citations`, `last_updated`.

#### stats_author_current
Author summary: h-index, citations, i10-index, publication counts, year_of_first_pub. Depends on `base_author_publications` and `stats_publication_current`.

#### dist_publication_citations
Maps `(pub_year, num_citations)` → percentile via `PERCENT_RANK()`. Reads from S2 papers data.

#### dist_author_metrics
Maps `(year_of_first_pub, metric_name, metric_value)` → percentile for 8 metrics. Reads from S2 authors + papers data.

### Level 2: Temporal Foundation + First Ranked

#### stats_publication_citations_temporal
Full citation timeline per publication: yearly and cumulative counts by citation_year and age.

#### ranked_publication_current
Joins `stats_publication_current` + `dist_publication_citations`. Adds `num_citations_percentile`.

#### intermediate_author_publication_state_temporal
For each (author, publication, year): cumulative and yearly citations at that point in time. Shared by both temporal pipelines (D and F).

#### dist_publication_citations_temporal
Maps temporal citation data → percentiles with 4 metrics × 2 partitioning schemes.

### Level 3: Temporal Stats + PiP Inputs

#### stats_author_metrics_temporal
Per-author per-year metrics: h_index, total_citations, i10_index, etc.

#### stats_author_publication_pip_inputs_current
Computes (X, Y) coordinates for each point on the PiP chart. Y-axis = `num_citations_percentile` from `ranked_publication_current`. X-axis = `num_papers_percentile` interpolated from `dist_author_metrics`.

#### ranked_author_current
Joins `stats_author_current` + `dist_author_metrics`. Adds 8 percentile columns.

#### ranked_publication_citations_temporal
Joins `stats_publication_citations_temporal` + `dist_publication_citations_temporal`. Adds 4 percentile columns.

### Level 4: PiP Scores + Higher Distributions

#### stats_author_pip_scores_current
PiP-AUC score via trapezoidal integration of PiP inputs. No percentile.

#### dist_pip_auc_scores
Maps `(year_of_first_pub, pip_auc_score)` → percentile.

#### dist_author_metrics_temporal
Maps `(year_of_first_pub, state_year, metric_name, metric_value)` → percentile for 7 temporal metrics.

### Level 5: Ranked PiP + Temporal Ranked + Temporal PiP

#### ranked_author_pip_scores_current
Joins `stats_author_pip_scores_current` + `dist_pip_auc_scores`. Adds `pip_auc_score_percentile`.

#### ranked_author_metrics_temporal
Joins `stats_author_metrics_temporal` + `dist_author_metrics_temporal`. Adds 7 percentile columns.

#### stats_author_pip_scores_temporal
The most expensive computation. For each (author, year), computes PiP-AUC using temporal citation percentiles and temporal publication-count percentiles.

### Level 6: Temporal PiP Distribution

#### dist_pip_auc_scores_temporal
Maps `(year_of_first_pub, state_year, pip_auc_score)` → percentile.

### Level 7: Temporal PiP Ranked

#### ranked_author_pip_scores_temporal
Joins `stats_author_pip_scores_temporal` + `dist_pip_auc_scores_temporal`. Adds `pip_auc_score_percentile`.

---

## Materialization Schedule

### Current schedule

| What | Schedule | How | Rationale |
|------|----------|-----|-----------|
| 16 analytics tables | Monthly (1st of month) | `dataset_ingestion/materialize_tables.py` (in Cloud Run Job) | S2 data is static between loads; no reason to re-compute |
| Safety-net materialization | Monthly (1st, 08:00 UTC) | `bigquery-materialize-all.yml` (GitHub Actions) | Catches failures in the ingestion job's materialization |
| Views deployment | On SQL file change | `bigquery-views.yml` (GitHub Actions) | Views kept for dev/debugging |

### Materialization order

Must respect the DAG — higher-level tables depend on lower-level tables:

```
Level 1 (independent):
  base_author_publications_table, stats_publication_current_table,
  stats_author_current_table, dist_publication_citations, dist_author_metrics

Level 2 (depends on Level 1):
  stats_publication_citations_temporal_table, ranked_publication_current_table,
  intermediate_author_publication_state_temporal_table,
  dist_publication_citations_temporal

Level 3 (depends on Levels 1-2):
  stats_author_metrics_temporal_table,
  stats_author_publication_pip_inputs_current_table,
  ranked_author_current_table, ranked_publication_citations_temporal_table

Level 4 (depends on Levels 1-3):
  stats_author_pip_scores_current_table,
  dist_pip_auc_scores, dist_author_metrics_temporal

Level 5 (depends on Levels 1-4):
  ranked_author_pip_scores_current_table,
  ranked_author_metrics_temporal_table,
  stats_author_pip_scores_temporal_table

Level 6 (depends on Level 5):
  dist_pip_auc_scores_temporal

Level 7: NOT MATERIALIZED (view only, not queried by app)
  ranked_author_pip_scores_temporal (view)
```

### Table substitutions during materialization

SQL files reference views so they work standalone. During pipeline execution, `materialize_tables.py` substitutes view references with `_table` references so each level reads from previously materialized output:

| View reference | Substituted with |
|----------------|-----------------|
| `statistics.stats_author_metrics_temporal_view` | `statistics.stats_author_metrics_temporal_table` |
| `statistics.stats_author_pip_scores_current` | `statistics.stats_author_pip_scores_current_table` |
| `statistics.stats_author_pip_scores_temporal_view` | `statistics.stats_author_pip_scores_temporal_table` |

---

## Query Patterns

### Per-author profile page (cheap, cached)

```
User visits /results?author_id=XYZ
  → Check Firestore cache (hit: ~50ms, done)
  → Cache miss: enqueue to Cache Layer priority queue
      → Cache Layer queries materialized tables for ONE author:
          ranked_author_current_table WHERE scholar_id = 'XYZ'
          stats_author_publication_pip_inputs_current_table WHERE ...
          ranked_author_pip_scores_current_table WHERE scholar_id = 'XYZ'
      → Cache results in Firestore
  → Frontend returns loading page → auto-refresh
  → Total: ~2-3s uncached, ~50-100ms cached
```

### All-authors ranking / CSV export

```
User visits /download or all-authors page
  → Cache Layer queries ranked_author_current_table (materialized, all rows)
  → JOIN ranked_author_pip_scores_current_table
  → Return ~99M rows (all S2 authors with stats)
```

---

## SQL File Reference

| File | Level | Purpose |
|------|-------|---------|
| `base_author_publications.sql` | 1 | Author → publication list extraction |
| `stats_publication_current.sql` | 1 | Per-pub: num_citations, metadata |
| `stats_author_current.sql` | 1 | Per-author: hindex, citedby, etc. |
| `dist_publication_citations.sql` | 1 | Citation percentile by pub_year |
| `dist_author_metrics.sql` | 1 | Author metric percentiles by cohort |
| `stats_publication_citations_temporal.sql` | 2 | Per-pub per-year: yearly/cumulative citations |
| `ranked_publication_current.sql` | 2 | Pub stats + citation percentile |
| `intermediate_author_publication_state_temporal.sql` | 2 | Per-pub per-year citation state |
| `dist_publication_citations_temporal.sql` | 2 | Temporal citation percentiles |
| `stats_author_metrics_temporal.sql` | 3 | Per-author per-year metrics |
| `stats_author_publication_pip_inputs_current.sql` | 3 | PiP chart coordinates |
| `ranked_author_current.sql` | 3 | Author stats + 8 percentiles |
| `ranked_publication_citations_temporal.sql` | 3 | Temporal pub stats + 4 percentiles |
| `stats_author_pip_scores_current.sql` | 4 | PiP-AUC score (no percentile) |
| `dist_pip_auc_scores.sql` | 4 | PiP-AUC percentile by cohort |
| `dist_author_metrics_temporal.sql` | 4 | Temporal author metric percentiles |
| `ranked_author_pip_scores_current.sql` | 5 | PiP-AUC + percentile |
| `ranked_author_metrics_temporal.sql` | 5 | Temporal author stats + 7 percentiles |
| `stats_author_pip_scores_temporal.sql` | 5 | Temporal PiP-AUC (no percentile) |
| `dist_pip_auc_scores_temporal.sql` | 6 | Temporal PiP-AUC percentiles |
| `ranked_author_pip_scores_temporal.sql` | 7 | Temporal PiP-AUC + percentile |
| `coauthor_network.sql` | 1 | Coauthor graph |
| `coauthors_to_add.sql` | 2 | Uncrawled coauthors |

All statistics files live under `bigquery/statistics/` except coauthor views in `bigquery/coauthor_network/`.

---

## Deployment

### View deployment (on code change)

Workflow: `.github/workflows/bigquery-views.yml`

Triggers on push to `bigquery/**/*.sql` on main. Deploys views in DAG order (Level 1 → 7). Views are kept for development and debugging. Whether app queries use materialized tables or live views depends on the `USE_MATERIALIZED_TABLES` config flag (default: `false`).

### Full materialization (monthly, during ingestion)

Primary: `dataset_ingestion/materialize_tables.py` (called at the end of the Cloud Run Job)

Materializes 16 tables in topological order (Levels 1–6). Each level's tables are created as `CREATE OR REPLACE TABLE ... CLUSTER BY ... AS SELECT * FROM <view>`. Intermediate views are left as views — their output is consumed inline by downstream materialized tables.

### Fallback materialization (monthly, safety net)

Workflow: `.github/workflows/bigquery-materialize-all.yml`

Runs on the 1st of each month at 08:00 UTC. Checks `release_log` to skip if materialization already succeeded. Can also be triggered manually via `workflow_dispatch`.

---

_Last updated: 2026-03-30_
