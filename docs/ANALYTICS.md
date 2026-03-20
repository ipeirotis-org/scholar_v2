# Analytics Framework: BigQuery Views, Materialization, and Cost

This document describes the full analytics computation pipeline — how raw Scholar data in BigQuery becomes the percentiles, PiP-AUC scores, and temporal metrics served by the web app.

## Design Principles

### 1. Three roles: Stats → Distributions → Ranked

Every metric family follows the same pattern:

- **Stats (`stats_*`):** Compute actual metric values only. No percentiles, no `PERCENT_RANK()`.
- **Distributions (`dist_*`):** The **only** place `PERCENT_RANK()` runs. Small materialized tables mapping (partition_key, metric_value) → percentile. Refreshed quarterly.
- **Ranked (`ranked_*`):** Cheap JOINs of stats + distributions. Add percentile columns via floor lookups. The app queries these.

However, these roles do **not** form three independent tiers. The PiP pipeline creates cross-role dependencies: `stats_author_publication_pip_inputs_current` reads from `ranked_publication_current` and `dist_author_metrics`. This means the real dependency graph has **8 topological levels** (0–7), not 3.

### 2. Materialize only what's needed for bulk operations

Per-author page loads query the ranked views directly (1–3s, cached in Firestore). Only the all-authors ranking page and CSV export need pre-materialized snapshot tables.

### 3. Refresh frequency should match data change rate

Author data changes at most monthly (when re-crawled). Distribution tables capture population shape, which shifts slowly. Quarterly refresh is sufficient.

---

## View Dependency DAG

The complete dependency graph, organized by topological level. Every node depends **only** on nodes at lower levels — no forward pointers. Materialized tables are marked with `[TABLE]`.

```
Level 0: Raw Data
  ┌─────────────────────────────────────────────┐
  │  scholar_raw_data.author_latest              │
  │  scholar_raw_data.pub_latest                 │
  └─────────────────────────────────────────────┘
                       │
                       ▼
Level 1: Foundation (raw data only)
  ┌─────────────────────────────────────────────┐
  │  VIEWS                                       │
  │    base_author_publications     ← author     │
  │    stats_publication_current    ← pub        │
  │    stats_pub_citations_temporal ← pub        │
  │    coauthor_network             ← author     │
  │                                              │
  │  TABLES (materialized quarterly)             │
  │    dist_publication_citations   ← pub        │
  │    dist_author_metrics          ← author+pub │
  └─────────────────────────────────────────────┘
                       │
                       ▼
Level 2: Derived Stats + First Ranked + First Temporal Dist
  ┌──────────────────────────────────────────────────────────────┐
  │  VIEWS                                                       │
  │    stats_author_current                                      │
  │      ← base_author_publications + stats_publication_current  │
  │    intermediate_author_publication_state_temporal             │
  │      ← stats_publication_citations_temporal                  │
  │    coauthors_to_add                                          │
  │      ← coauthor_network                                      │
  │    ranked_publication_current                                 │
  │      ← stats_publication_current + dist_publication_citations│
  │                                                              │
  │  TABLES (materialized quarterly)                             │
  │    dist_publication_citations_temporal                        │
  │      ← stats_publication_citations_temporal                  │
  └──────────────────────────────────────────────────────────────┘
                       │
                       ▼
Level 3: Temporal Stats + PiP Inputs + More Ranked
  ┌──────────────────────────────────────────────────────────────┐
  │  VIEWS                                                       │
  │    stats_author_metrics_temporal_view                         │
  │      ← intermediate_* + stats_publication_current            │
  │    stats_author_publication_pip_inputs_current                │
  │      ← ranked_publication_current + dist_author_metrics      │
  │        + stats_author_current                                │
  │    ranked_author_current                                      │
  │      ← stats_author_current + dist_author_metrics            │
  │    ranked_publication_citations_temporal                       │
  │      ← stats_pub_citations_temporal                          │
  │        + dist_publication_citations_temporal                  │
  └──────────────────────────────────────────────────────────────┘
                       │
                       ▼
Level 4: PiP Scores + PiP Dist + Temporal Author Dist
  ┌──────────────────────────────────────────────────────────────┐
  │  VIEWS                                                       │
  │    stats_author_pip_scores_current                            │
  │      ← stats_author_publication_pip_inputs_current           │
  │        + stats_author_current                                │
  │                                                              │
  │  TABLES (materialized quarterly)                             │
  │    dist_pip_auc_scores                                        │
  │      ← stats_author_publication_pip_inputs_current           │
  │        + stats_author_current                                │
  │    dist_author_metrics_temporal                               │
  │      ← stats_author_metrics_temporal_view                    │
  └──────────────────────────────────────────────────────────────┘
                       │
                       ▼
Level 5: PiP Ranked + Temporal Ranked + Temporal PiP Stats
  ┌──────────────────────────────────────────────────────────────┐
  │  VIEWS                                                       │
  │    ranked_author_pip_scores_current                           │
  │      ← stats_author_pip_scores_current + dist_pip_auc_scores │
  │    ranked_author_metrics_temporal                              │
  │      ← stats_author_metrics_temporal_view                    │
  │        + dist_author_metrics_temporal                         │
  │    stats_author_pip_scores_temporal_view                       │
  │      ← intermediate_* + dist_publication_citations_temporal  │
  │        + stats_author_metrics_temporal_view                  │
  │        + dist_author_metrics_temporal                         │
  └──────────────────────────────────────────────────────────────┘
                       │
                       ▼
Level 6: Temporal PiP Distribution
  ┌──────────────────────────────────────────────────────────────┐
  │  TABLES (materialized quarterly)                             │
  │    dist_pip_auc_scores_temporal                               │
  │      ← stats_author_pip_scores_temporal_view                 │
  └──────────────────────────────────────────────────────────────┘
                       │
                       ▼
Level 7: Temporal PiP Ranked
  ┌──────────────────────────────────────────────────────────────┐
  │  VIEWS                                                       │
  │    ranked_author_pip_scores_temporal                           │
  │      ← stats_author_pip_scores_temporal_view                 │
  │        + dist_pip_auc_scores_temporal                         │
  └──────────────────────────────────────────────────────────────┘
```

### Why 8 levels instead of 3?

The original "3-tier" framing (stats → dist → ranked) describes the **role** of each view, not its **position** in the DAG. The depth comes from two sources:

1. **PiP inputs read ranked data.** `stats_author_publication_pip_inputs_current` needs `ranked_publication_current` (which itself needs `dist_publication_citations`). This pushes the PiP pipeline deeper than basic stats.

2. **The temporal PiP pipeline stacks three stats→dist→ranked cycles.** Publications temporal → author temporal → PiP temporal, each cycle adding 2 levels.

### Snapshot Tables (daily materialization)

These are `CREATE OR REPLACE TABLE ... AS SELECT * FROM <ranked_view>`, used only for all-authors ranking pages and CSV export:

| Snapshot Table | Source View | Level |
|---|---|---|
| `ranked_author_current_table` | `ranked_author_current` | 3 |
| `ranked_author_pip_scores_current_table` | `ranked_author_pip_scores_current` | 5 |
| `ranked_author_metrics_temporal_table` | `ranked_author_metrics_temporal` | 5 |
| `ranked_author_pip_scores_temporal_table` | `ranked_author_pip_scores_temporal` | 7 |

---

## Pipelines

The DAG decomposes into six pipelines that share data across levels.

### Pipeline A: Publications (current)

```
L1  stats_publication_current  ──► L1  dist_publication_citations [TABLE]
                                         │
                                         ▼
                                   L2  ranked_publication_current
```

### Pipeline B: Authors (current)

```
L1  base_author_publications ─┐
L1  stats_publication_current ─┤──► L2  stats_author_current ──► L1  dist_author_metrics [TABLE]
                               │                                       │
                               │                                       ▼
                               │                                 L3  ranked_author_current
```

### Pipeline C: Publications (temporal)

```
L1  stats_publication_citations_temporal ──► L2  dist_publication_citations_temporal [TABLE]
                                                   │
                                                   ▼
                                             L3  ranked_publication_citations_temporal
```

### Pipeline D: Authors (temporal)

```
L1  stats_publication_citations_temporal ─┐
L0  author_latest ────────────────────────┤──► L2  intermediate_author_publication_state_temporal
                                          │                │
L1  stats_publication_current ────────────┤                ▼
                                          └──► L3  stats_author_metrics_temporal_view
                                                       │
                                                       ▼
                                               L4  dist_author_metrics_temporal [TABLE]
                                                       │
                                                       ▼
                                               L5  ranked_author_metrics_temporal
```

### Pipeline E: PiP (current)

```
L2  ranked_publication_current  ─┐
L1  dist_author_metrics ─────────┤──► L3  stats_author_publication_pip_inputs_current
L2  stats_author_current ────────┤               │
                                 │               ▼
                                 │     L4  stats_author_pip_scores_current
                                 │     L4  dist_pip_auc_scores [TABLE]
                                 │               │
                                 │               ▼
                                 └───► L5  ranked_author_pip_scores_current
```

### Pipeline F: PiP (temporal)

```
L2  intermediate_*  ─────────────────┐
L2  dist_publication_citations_temp ─┤
L3  stats_author_metrics_temp_view ──┤──► L5  stats_author_pip_scores_temporal_view
L4  dist_author_metrics_temporal ────┘               │
                                                     ▼
                                             L6  dist_pip_auc_scores_temporal [TABLE]
                                                     │
                                                     ▼
                                             L7  ranked_author_pip_scores_temporal
```

---

## View Details

### Level 1: Foundation

#### base_author_publications
Extracts (scholar_id, author_pub_id, pub_year) from author JSON. Used by stats_author_current.

#### stats_publication_current
Per-publication: `scholar_id`, `author_pub_id`, `pub_year`, `title`, `author`, `num_citations`, `last_updated`.

#### stats_publication_citations_temporal
Full citation timeline per publication: yearly and cumulative counts by citation_year and age.

#### coauthor_network
Extracts the coauthor graph from author JSON.

#### dist_publication_citations `[TABLE, quarterly]`
Maps `(pub_year, num_citations)` → percentile via `PERCENT_RANK()`. Reads directly from `pub_latest`.

#### dist_author_metrics `[TABLE, quarterly]`
Maps `(year_of_first_pub, metric_name, metric_value)` → percentile for 8 metrics. Reads directly from `author_latest` + `pub_latest`.

### Level 2: Derived Stats

#### stats_author_current
Author summary: h-index, citations, i10-index, publication counts, year_of_first_pub. Depends on `base_author_publications` and `stats_publication_current`.

#### intermediate_author_publication_state_temporal
For each (author, publication, year): cumulative and yearly citations at that point in time. Shared by both temporal pipelines (D and F).

#### coauthors_to_add
Filters `coauthor_network` to find coauthors not yet in the database.

#### ranked_publication_current
Joins `stats_publication_current` + `dist_publication_citations`. Adds `num_citations_percentile`.

#### dist_publication_citations_temporal `[TABLE, quarterly]`
Maps temporal citation data → percentiles with 4 metrics × 2 partitioning schemes. Reads from `stats_publication_citations_temporal`.

### Level 3: Temporal Stats + PiP Inputs

#### stats_author_metrics_temporal_view
Per-author per-year metrics: h_index, total_citations, i10_index, etc. Computed from the intermediate temporal view.

#### stats_author_publication_pip_inputs_current
Computes (X, Y) coordinates for each point on the PiP chart. Y-axis = `num_citations_percentile` from `ranked_publication_current`. X-axis = `num_papers_percentile` interpolated from `dist_author_metrics`.

#### ranked_author_current
Joins `stats_author_current` + `dist_author_metrics`. Adds 8 percentile columns.

#### ranked_publication_citations_temporal
Joins `stats_publication_citations_temporal` + `dist_publication_citations_temporal`. Adds 4 percentile columns.

### Level 4: PiP Scores + Higher Distributions

#### stats_author_pip_scores_current
PiP-AUC score via trapezoidal integration of PiP inputs. No percentile.

#### dist_pip_auc_scores `[TABLE, quarterly]`
Maps `(year_of_first_pub, pip_auc_score)` → percentile. Recomputes the full PiP pipeline internally.

#### dist_author_metrics_temporal `[TABLE, quarterly]`
Maps `(year_of_first_pub, state_year, metric_name, metric_value)` → percentile for 7 temporal metrics.

### Level 5: PiP Ranked + Temporal Ranked + Temporal PiP

#### ranked_author_pip_scores_current
Joins `stats_author_pip_scores_current` + `dist_pip_auc_scores`. Adds `pip_auc_score_percentile`.

#### ranked_author_metrics_temporal
Joins `stats_author_metrics_temporal_view` + `dist_author_metrics_temporal`. Adds 7 percentile columns.

#### stats_author_pip_scores_temporal_view
The most expensive view. For each (author, year), computes PiP-AUC using temporal citation percentiles and temporal publication-count percentiles. Always materialized, never queried live.

### Level 6: Temporal PiP Distribution

#### dist_pip_auc_scores_temporal `[TABLE, quarterly]`
Maps `(year_of_first_pub, state_year, pip_auc_score)` → percentile.

### Level 7: Temporal PiP Ranked

#### ranked_author_pip_scores_temporal
Joins `stats_author_pip_scores_temporal_view` + `dist_pip_auc_scores_temporal`. Adds `pip_auc_score_percentile`.

---

## Materialization Schedule and Cost Rationale

### Current schedule

| What | Schedule | Workflow | Rationale |
|------|----------|----------|-----------|
| Distribution tables (`dist_*`, 6 tables) | Quarterly (Jan 1, Apr 1, Jul 1, Oct 1 at 04:00 UTC) | `bigquery-materialize-distributions.yml` | Population percentiles shift slowly; expensive to compute |
| Snapshot tables (4 tables) | Daily at 06:00 UTC | `bigquery-materialize.yml` | Needed for all-authors ranking/export; per-author pages use views |

### Distribution table materialization order

Must respect the DAG — higher-level dist tables depend on lower-level views which themselves depend on lower-level dist tables:

```
Step 1 (independent, Level 1):
  dist_publication_citations          ← pub_latest
  dist_author_metrics                 ← author_latest + pub_latest

Step 2 (Level 2, depends on Level 1 views):
  dist_publication_citations_temporal ← stats_publication_citations_temporal

Step 3 (Level 4, depends on Level 2-3 views + Step 1 tables):
  dist_author_metrics_temporal        ← stats_author_metrics_temporal_view
  dist_pip_auc_scores                 ← pip_inputs + stats_author_current

Step 4 (Level 6, depends on Level 5 views + Step 2-3 tables):
  dist_pip_auc_scores_temporal        ← stats_author_pip_scores_temporal_view
```

### Why quarterly for distribution tables

Distribution tables capture the *shape* of the population. This shape changes slowly because:

- Adding a few hundred authors to a pool of 15,000+ barely shifts percentile boundaries
- Citation counts for older papers change slowly
- New publication years accumulate papers gradually throughout the year

Recomputing quarterly is sufficient. The error from a 3-month-old distribution table is negligible.

### Why daily snapshot materialization may be excessive

The snapshot tables exist only for the all-authors ranking page and CSV export. Individual author data changes only when re-crawled (monthly). Daily materialization recomputes all rows to update ~200.

**Alternatives to consider:**
- **Weekly materialization** — 7x cost reduction
- **Event-driven materialization** — trigger after `batch_load_gcs_to_bq` completes
- **Incremental updates** — MERGE only changed authors

Per-author profile pages are unaffected — they always query views directly and cache in Firestore.

---

## Query Patterns

### Per-author profile page (cheap, real-time)

```
User visits /results?author_id=XYZ
  → Check Firestore cache (hit: ~50ms, done)
  → Cache miss: query these ranked views for ONE author:
      ranked_author_current WHERE scholar_id = 'XYZ'              (~500ms)
      stats_author_publication_pip_inputs_current WHERE ...        (~800ms)
      ranked_author_pip_scores_current WHERE scholar_id = 'XYZ'   (~500ms)
  → Cache results in Firestore
  → Generate matplotlib charts (~300ms)
  → Total: ~2-3s uncached, ~350ms cached
```

These queries are cheap because:
- They filter to a single `scholar_id` (tiny result set)
- Percentile lookups use the small distribution tables (no full-table scans)
- BigQuery can pushdown the WHERE clause through the view chain

### All-authors ranking page (needs materialized tables)

```
User visits /download or all-authors page
  → Query ranked_author_current_table (materialized, all rows)
  → JOIN ranked_author_pip_scores_current_table
  → Return ~15,000 rows
```

---

## SQL File Reference

| File | Level | Type | Purpose |
|------|-------|------|---------|
| `base_author_publications.sql` | 1 | View | Author → publication list extraction |
| `stats_publication_current.sql` | 1 | View | Per-pub: num_citations, metadata |
| `stats_publication_citations_temporal.sql` | 1 | View | Per-pub per-year: yearly/cumulative citations |
| `coauthor_network.sql` | 1 | View | Coauthor graph |
| `dist_publication_citations.sql` | 1 | Table (quarterly) | Citation percentile by pub_year |
| `dist_author_metrics.sql` | 1 | Table (quarterly) | Author metric percentiles by cohort |
| `stats_author_current.sql` | 2 | View | Per-author: hindex, citedby, etc. |
| `intermediate_author_publication_state_temporal.sql` | 2 | View | Per-pub per-year citation state |
| `coauthors_to_add.sql` | 2 | View | Uncrawled coauthors |
| `ranked_publication_current.sql` | 2 | View | Pub stats + citation percentile |
| `dist_publication_citations_temporal.sql` | 2 | Table (quarterly) | Temporal citation percentiles |
| `stats_author_metrics_temporal.sql` | 3 | View | Per-author per-year metrics |
| `stats_author_publication_pip_inputs_current.sql` | 3 | View | PiP chart coordinates |
| `ranked_author_current.sql` | 3 | View | Author stats + 8 percentiles |
| `ranked_publication_citations_temporal.sql` | 3 | View | Temporal pub stats + 4 percentiles |
| `stats_author_pip_scores_current.sql` | 4 | View | PiP-AUC score (no percentile) |
| `dist_pip_auc_scores.sql` | 4 | Table (quarterly) | PiP-AUC percentile by cohort |
| `dist_author_metrics_temporal.sql` | 4 | Table (quarterly) | Temporal author metric percentiles |
| `ranked_author_pip_scores_current.sql` | 5 | View | PiP-AUC + percentile |
| `ranked_author_metrics_temporal.sql` | 5 | View | Temporal author stats + 7 percentiles |
| `stats_author_pip_scores_temporal.sql` | 5 | View | Temporal PiP-AUC (no percentile) |
| `dist_pip_auc_scores_temporal.sql` | 6 | Table (quarterly) | Temporal PiP-AUC percentiles |
| `ranked_author_pip_scores_temporal.sql` | 7 | View | Temporal PiP-AUC + percentile |
| `materialize_stats.sql` | — | Script | Full materialization (dist + snapshots) |

All statistics files live under `bigquery/statistics/` except coauthor views in `bigquery/coauthor_network/`.

---

## Deployment

### View deployment (on code change)

Workflow: `.github/workflows/bigquery-views.yml`

Triggers on push to `bigquery/**/*.sql` on main. Deploys views in DAG order (Level 1 → 7). Does **not** refresh distribution or snapshot tables — dist tables must already exist.

### Distribution table refresh (quarterly)

Workflow: `.github/workflows/bigquery-materialize-distributions.yml`

Materializes 6 distribution tables in dependency order:

1. `dist_publication_citations` + `dist_author_metrics` (Level 1, independent)
2. `dist_publication_citations_temporal` (Level 2)
3. `dist_author_metrics_temporal` + `dist_pip_auc_scores` (Level 4, independent of each other)
4. `dist_pip_auc_scores_temporal` (Level 6)

### Snapshot table refresh (daily)

Workflow: `.github/workflows/bigquery-materialize.yml`

Runs `CREATE OR REPLACE TABLE ... AS SELECT * FROM <ranked_view>` for:
1. `ranked_author_current_table` (Level 3, clustered by scholar_id, year_of_first_pub)
2. `ranked_author_pip_scores_current_table` (Level 5, clustered by scholar_id)
3. `ranked_author_metrics_temporal_table` (Level 5, clustered by scholar_id, state_year)
4. `ranked_author_pip_scores_temporal_table` (Level 7, clustered by scholar_id, state_year)

---

_Last updated: 2026-03-20_
