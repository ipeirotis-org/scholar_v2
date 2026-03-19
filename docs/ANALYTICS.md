# Analytics Framework: BigQuery Views, Materialization, and Cost

This document describes the full analytics computation pipeline — how raw Scholar data in BigQuery becomes the percentiles, PiP-AUC scores, and temporal metrics served by the web app.

## Design Principles

### 1. Three-tier architecture: Stats → Distributions → Ranked

The pipeline is split into three clean tiers:

- **Tier 1 — Raw statistics (`stats_*`):** Compute actual metric values only. No percentiles, no `PERCENT_RANK()`. These are the core "facts" — citation counts, h-index values, PiP-AUC scores.
- **Tier 2 — Distribution tables (`dist_*`):** The **only** place `PERCENT_RANK()` runs. Small lookup tables mapping (partition_key, metric_value) → percentile. Expensive to compute but compact in output (DISTINCT collapses tied values). Refreshed quarterly.
- **Tier 3 — Ranked views (`ranked_*`):** Cheap JOINs of Tier 1 + Tier 2. Add percentile columns to raw stats via floor lookups. These are what the app queries.

This means a single author's profile page never triggers a full-table scan — it joins one author's data against the small distribution tables.

### 2. Materialize only what's needed for bulk operations

Per-author page loads query the ranked views directly (1-3s, cached in Firestore). Only the all-authors ranking page and CSV export need pre-materialized snapshot tables.

### 3. Refresh frequency should match data change rate

Author data changes at most monthly (when re-crawled). Daily materialization of snapshot tables recomputes 15,000+ author rows when typically fewer than 100 have changed. Distribution tables change even more slowly — the shape of the citation percentile curve barely shifts week-to-week.

---

## View Dependency DAG

```
Raw tables
  scholar_raw_data.author  →  author_latest (dedup view)
  scholar_raw_data.pub     →  pub_latest (dedup view)
      │
      ▼
Tier 1: Raw Statistics (no percentiles, no PERCENT_RANK)
  base_author_publications            — author → publication list
  stats_publication_current           — num_citations, metadata
  stats_publication_citations_temporal — yearly_citations, cumulative_citations
  coauthor_network                    — coauthor graph
  stats_author_current                — hindex, citedby, i10index, total_publications
  stats_author_metrics_temporal_view  — per-author per-year: h_index, total_citations, etc.
  intermediate_author_publication_state_temporal
  stats_author_pip_scores_current     — pip_auc_score (no percentile)
  stats_author_pip_scores_temporal_view — pip_auc_score per year (no percentile)
      │
      ▼
Tier 2: Distribution Tables (materialized quarterly — the ONLY place PERCENT_RANK runs)
  ┌──────────────────────────────────────────────────────────────────────────────┐
  │ dist_publication_citations          (pub_year) → num_citations → percentile │
  │ dist_publication_citations_temporal  (pub_year, citation_year, age)          │
  │                                     → yearly_citations, cumulative → pctile │
  │ dist_author_metrics                 (year_of_first_pub) → 8 metrics → pctile│
  │ dist_author_metrics_temporal        (year_of_first_pub, state_year)          │
  │                                     → 7 metrics → percentile                │
  │ dist_pip_auc_scores                 (year_of_first_pub) → pip_auc → pctile  │
  │ dist_pip_auc_scores_temporal        (year_of_first_pub, state_year)          │
  │                                     → pip_auc → percentile                  │
  └──────────────────────────────────────────────────────────────────────────────┘
      │
      ▼
Tier 3: Ranked Views (cheap JOINs of Tier 1 + Tier 2)
  ranked_publication_current            — adds num_citations_percentile
  ranked_publication_citations_temporal  — adds 4 percentile columns
  ranked_author_current                 — adds 8 percentile columns
  ranked_author_metrics_temporal        — adds 7 percentile columns
  ranked_author_pip_scores_current      — adds pip_auc_score_percentile
  ranked_author_pip_scores_temporal     — adds pip_auc_score_percentile

PiP Pipeline (sits between tiers, uses ranked + dist for inputs):
  stats_author_publication_pip_inputs_current  — PiP chart X/Y coordinates
  coauthors_to_add                             — coauthors not yet in the database
```

---

## Tier 1: Raw Statistics

These views compute actual metric values only. No percentile columns.

### stats_publication_current

**Purpose:** Extract core publication details from the latest deduplicated data.

**Output:** `scholar_id`, `author_pub_id`, `pub_year`, `title`, `author`, `num_citations`, `last_updated`

### stats_publication_citations_temporal

**Purpose:** Full citation timeline per publication — yearly and cumulative counts.

**Output:** `scholar_id`, `author_pub_id`, `pub_year`, `age`, `citation_year`, `yearly_citations`, `cumulative_citations`

### stats_author_current

**Purpose:** Author summary metrics (no percentiles).

**Output:** `scholar_id`, `name`, `affiliation`, `email_domain`, h-index/citations/i10 (current + 5y), `total_publications`, `total_publications_with_citations`, `year_of_first_pub`, `last_updated`

### stats_author_metrics_temporal_view

**Purpose:** How an author's metrics evolved over time. Raw values only.

**Output per (author, year):** `total_publications`, `total_citations`, `total_recent_citations_5y`, `h_index`, `h_index_5y`, `i10_index`, `i10_index_5y`

### stats_author_pip_scores_current

**Purpose:** PiP-AUC score via trapezoidal integration. No percentile.

**Output:** `scholar_id`, `year_of_first_pub`, `pip_auc_score`

### stats_author_pip_scores_temporal_view

**Purpose:** PiP-AUC at each point in time. Uses each publication's `cumulative_citations` as of `state_year` to compute the PiP chart at that moment.

**Output:** `scholar_id`, `state_year`, `year_of_first_pub`, `pip_auc_score`

**Cost:** This is the most expensive view — it runs the full PiP pipeline for every author × every year. Always materialized, never queried live.

---

## Tier 2: Distribution Tables

These are the **only** place `PERCENT_RANK()` runs. Everything downstream uses floor lookups.

### dist_publication_citations

**What:** Maps (pub_year, num_citations) → citation percentile.

**How:** `PERCENT_RANK() OVER (PARTITION BY pub_year ORDER BY num_citations)` across all publications. `SELECT DISTINCT` collapses tied values.

**Used by:** `ranked_publication_current` (Tier 3)

### dist_publication_citations_temporal

**What:** Maps temporal citation data to percentiles with two partitioning schemes:
- By (pub_year, citation_year): yearly_citations → percentile, cumulative_citations → percentile
- By (age): yearly_citations → percentile, cumulative_citations → percentile

**Format:** Normalized `(pub_year, citation_year, age, metric_name, metric_value, percentile)`

**Used by:** `ranked_publication_citations_temporal` (Tier 3), `stats_author_pip_scores_temporal_view` (Tier 1)

### dist_author_metrics

**What:** Maps (year_of_first_pub, metric_name, metric_value) → percentile for 8 author metrics.

**Metrics:** `hindex`, `hindex5y`, `citedby`, `citedby5y`, `i10index`, `i10index5y`, `total_publications`, `total_publications_with_citations`

**Used by:** `ranked_author_current` (Tier 3), `stats_author_publication_pip_inputs_current` (PiP X-axis interpolation)

### dist_author_metrics_temporal

**What:** Maps (year_of_first_pub, state_year, metric_name, metric_value) → percentile for 7 temporal author metrics.

**Format:** Same normalized structure as `dist_author_metrics`, with added `state_year` partition key.

**Used by:** `ranked_author_metrics_temporal` (Tier 3), `stats_author_pip_scores_temporal_view` (Tier 1)

### dist_pip_auc_scores

**What:** Maps (year_of_first_pub, pip_auc_score) → PiP-AUC percentile.

**Depends on:** `dist_publication_citations` and `dist_author_metrics` (the underlying PiP views read from them).

**Used by:** `ranked_author_pip_scores_current` (Tier 3)

### dist_pip_auc_scores_temporal

**What:** Maps (year_of_first_pub, state_year, pip_auc_score) → PiP-AUC percentile.

**Depends on:** `dist_publication_citations_temporal` and `dist_author_metrics_temporal`.

**Used by:** `ranked_author_pip_scores_temporal` (Tier 3)

---

## Tier 3: Ranked Views

These add percentile columns by JOINing Tier 1 stats against Tier 2 distribution tables. The app queries these.

### ranked_publication_current

**Joins:** `stats_publication_current` + `dist_publication_citations`

**Adds:** `num_citations_percentile`

### ranked_publication_citations_temporal

**Joins:** `stats_publication_citations_temporal` + `dist_publication_citations_temporal`

**Adds:** `perc_pub_year_yearly_citations`, `perc_pub_year_cumulative_citations`, `perc_age_yearly_citations`, `perc_age_cumulative_citations`

### ranked_author_current

**Joins:** `stats_author_current` + `dist_author_metrics`

**Adds:** 8 percentile columns (one per metric)

### ranked_author_metrics_temporal

**Joins:** `stats_author_metrics_temporal_view` + `dist_author_metrics_temporal`

**Adds:** 7 percentile columns (one per temporal metric)

### ranked_author_pip_scores_current

**Joins:** `stats_author_pip_scores_current` + `dist_pip_auc_scores`

**Adds:** `pip_auc_score_percentile`

### ranked_author_pip_scores_temporal

**Joins:** `stats_author_pip_scores_temporal_view` + `dist_pip_auc_scores_temporal`

**Adds:** `pip_auc_score_percentile`

---

## PiP Pipeline

### stats_author_publication_pip_inputs_current

**Purpose:** Compute the (X, Y) coordinates for each point on an author's PiP chart.

**Y-axis:** `num_citations_percentile` — from `ranked_publication_current`

**X-axis:** `num_papers_percentile` — interpolated from `dist_author_metrics` (the `total_publications_with_citations` metric). This is a 6-CTE pipeline:

1. **RankedPublications:** Rank author's papers by citation percentile (descending)
2. **Distances:** Look up the num_papers_percentile for the author's paper count and for paper count ± 1
3. **RankedDistances:** Order the distance values
4. **FilteredDistances:** Pick the two bracketing entries from the distribution
5. **AggregatedDistances:** Compute interpolation bounds
6. **InterpolatedResults:** Linear interpolation to get the exact X-axis position

---

## Temporal Views

### stats_author_metrics_temporal_view → ranked_author_metrics_temporal_table

**Purpose:** How an author's h-index, citations, and i10-index evolved over time (year by year).

**How:** For each (author, year) pair, computes what the author's metrics *would have been* at that point in time — using only citations accumulated up to that year. This requires rolling calculations across the full publication × citation-year matrix.

**Cost:** This is expensive because it materializes O(authors × years × publications) intermediate state.

### stats_author_pip_scores_temporal_view → ranked_author_pip_scores_temporal_table

**Purpose:** How an author's PiP-AUC score evolved over their career.

**How:** For each (author, year), takes all publications with pub_year ≤ state_year, looks up each publication's cumulative_citations as of that year via `dist_publication_citations_temporal`, counts papers and looks up `num_papers_percentile` via `dist_author_metrics_temporal`, then runs trapezoidal integration.

**Cost:** The most expensive computation in the system. Always materialized.

### intermediate_author_publication_state_temporal

**Purpose:** Intermediate join table used by temporal views. For each (author, publication, year), tracks cumulative citation count at that point in time.

---

## Coauthor Views

### coauthor_network

**Purpose:** Extract the coauthor graph from author JSON.

### coauthors_to_add

**Purpose:** Filter `coauthor_network` to find coauthors not yet in the database.

---

## Materialization Schedule and Cost Rationale

### Current schedule

| What | Schedule | Workflow | Estimated BQ cost |
|------|----------|----------|-------------------|
| Distribution tables (`dist_*`) | Quarterly (Jan 1, Apr 1, Jul 1, Oct 1 at 04:00 UTC) | `bigquery-materialize-distributions.yml` | High per run (full PERCENT_RANK), but only 4x/year |
| Snapshot tables (`ranked_*_table`) | Daily at 06:00 UTC | `bigquery-materialize.yml` | Moderate (SELECT * from views for all authors) |

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

| File | Tier | Type | Purpose |
|------|------|------|---------|
| `base_author_publications.sql` | 1 | View | Author → publication list extraction |
| `stats_publication_current.sql` | 1 | View | Per-pub: num_citations, metadata |
| `stats_publication_citations_temporal.sql` | 1 | View | Per-pub per-year: yearly/cumulative citations |
| `stats_author_current.sql` | 1 | View | Per-author: hindex, citedby, etc. (no percentiles) |
| `stats_author_metrics_temporal.sql` | 1 | View | Per-author per-year metrics (no percentiles) |
| `stats_author_pip_scores_current.sql` | 1 | View | PiP-AUC score (no percentile) |
| `stats_author_pip_scores_temporal.sql` | 1 | View | Temporal PiP-AUC (no percentile) |
| `intermediate_author_publication_state_temporal.sql` | 1 | View | Intermediate temporal state |
| `stats_author_publication_pip_inputs_current.sql` | 1+ | View | PiP chart coordinates |
| `dist_publication_citations.sql` | 2 | Table (quarterly) | Citation percentile by pub_year |
| `dist_publication_citations_temporal.sql` | 2 | Table (quarterly) | Temporal citation percentiles |
| `dist_author_metrics.sql` | 2 | Table (quarterly) | Author metric percentiles by cohort |
| `dist_author_metrics_temporal.sql` | 2 | Table (quarterly) | Temporal author metric percentiles |
| `dist_pip_auc_scores.sql` | 2 | Table (quarterly) | PiP-AUC percentile by cohort |
| `dist_pip_auc_scores_temporal.sql` | 2 | Table (quarterly) | Temporal PiP-AUC percentiles |
| `ranked_publication_current.sql` | 3 | View | Pub stats + citation percentile |
| `ranked_publication_citations_temporal.sql` | 3 | View | Temporal pub stats + 4 percentiles |
| `ranked_author_current.sql` | 3 | View | Author stats + 8 percentiles |
| `ranked_author_metrics_temporal.sql` | 3 | View | Temporal author stats + 7 percentiles |
| `ranked_author_pip_scores_current.sql` | 3 | View | PiP-AUC + percentile |
| `ranked_author_pip_scores_temporal.sql` | 3 | View | Temporal PiP-AUC + percentile |
| `coauthor_network.sql` | 1 | View | Coauthor graph |
| `coauthors_to_add.sql` | 1 | View | Uncrawled coauthors |
| `materialize_stats.sql` | — | Script | Full materialization (dist + snapshots) |

All statistics files live under `bigquery/statistics/` except coauthor views in `bigquery/coauthor_network/`.

---

## Deployment

### View deployment (on code change)

Workflow: `.github/workflows/bigquery-views.yml`

Triggers on push to `bigquery/**/*.sql` on main. Deploys views in tier order (1 → 3) to respect dependencies. Does **not** refresh distribution or snapshot tables.

### Distribution table refresh (quarterly)

Workflow: `.github/workflows/bigquery-materialize-distributions.yml`

Runs 6 distribution table materializations in dependency order:
1. `dist_publication_citations` + `dist_author_metrics` (independent)
2. `dist_publication_citations_temporal` + `dist_author_metrics_temporal` (independent)
3. `dist_pip_auc_scores` (depends on 1)
4. `dist_pip_auc_scores_temporal` (depends on 2)

### Snapshot table refresh (daily)

Workflow: `.github/workflows/bigquery-materialize.yml`

Runs `CREATE OR REPLACE TABLE ... AS SELECT * FROM <ranked_view>` for:
1. `ranked_author_current_table` (clustered by scholar_id, year_of_first_pub)
2. `ranked_author_pip_scores_current_table` (clustered by scholar_id)
3. `ranked_author_metrics_temporal_table` (clustered by scholar_id, state_year)
4. `ranked_author_pip_scores_temporal_table` (clustered by scholar_id, state_year)

---

_Last updated: 2026-03-19_
