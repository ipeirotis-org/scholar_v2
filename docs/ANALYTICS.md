# Analytics Framework: BigQuery Views, Materialization, and Cost

This document describes the full analytics computation pipeline — how raw Scholar data in BigQuery becomes the percentiles, PiP-AUC scores, and temporal metrics served by the web app.

## Design Principles

### 1. Separate population statistics from individual lookups

Computing a percentile requires knowing the full distribution. But *looking up* a percentile for one author only requires the precomputed distribution. We exploit this by splitting the work:

- **Distribution tables (`dist_*`):** Expensive to compute (full-table `PERCENT_RANK()`), but small in output (distinct values only) and slow to change (population percentiles shift meaningfully only when many new authors/papers are added).
- **Views (`stats_*`):** Cheap per-author queries that do floor lookups against the distribution tables. No `PERCENT_RANK()` at query time.

This means a single author's profile page never triggers a full-table scan — it joins one author's data against the small distribution tables.

### 2. Materialize only what's needed for bulk operations

Per-author page loads query the views directly (1-3s, cached in Firestore). Only the all-authors ranking page and CSV export need pre-materialized snapshot tables.

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
Tier 0: Distribution Tables (materialized quarterly)
  ┌─────────────────────────────────────────────────┐
  │ dist_publication_citations                      │
  │   (pub_year, num_citations) → percentile        │
  │                                                 │
  │ dist_author_metrics                             │
  │   (cohort, metric_name, metric_value)           │
  │   → percentile for 8 metrics                   │
  │                                                 │
  │ dist_pip_auc_scores                             │
  │   (cohort, pip_auc_score) → percentile          │
  └─────────────────────────────────────────────────┘
      │
      ▼
Tier 1: Base Views (no view-to-view dependencies)
  base_author_publications         — author → publication list (UNNEST from JSON)
  stats_publication_current        — publication citation percentiles (floor lookup into dist_publication_citations)
  stats_publication_citations_temporal — citation timeline per publication (yearly + cumulative)
  coauthor_network                 — coauthor graph (UNNEST from JSON)
      │
      ▼
Tier 2: Author-Level Views
  stats_author_current             — author summary metrics + 8 percentiles (floor lookup into dist_author_metrics)
  coauthors_to_add                 — coauthors not yet in the database
  intermediate_author_publication_state_temporal — per-author-per-year publication state
      │
      ▼
Tier 3: PiP-AUC Inputs + Temporal
  stats_author_publication_pip_inputs_current — PiP chart X/Y coordinates (6-CTE interpolation pipeline)
  stats_author_metrics_temporal_view          — temporal h-index, citations, i10 evolution
      │
      ▼
Tier 4: Final Scores
  stats_author_pip_scores_current  — PiP-AUC score (trapezoidal integration) + percentile from dist_pip_auc_scores
```

---

## Distribution Tables (Tier 0)

These are the **only** place `PERCENT_RANK()` runs. Everything downstream uses floor lookups.

### dist_publication_citations

**What:** Maps (pub_year, num_citations) → citation percentile.

**How:** `PERCENT_RANK() OVER (PARTITION BY pub_year ORDER BY num_citations)` across all publications. `SELECT DISTINCT` collapses tied values that share the same rank, keeping the table small.

**Used by:** `stats_publication_current` (Tier 1) — to assign each publication its citation percentile without scanning all publications.

**Example lookup:**
```sql
-- "A paper with 45 citations published in 2018 is at what percentile?"
SELECT MAX(percentile)
FROM dist_publication_citations
WHERE pub_year = 2018 AND num_citations <= 45
```

### dist_author_metrics

**What:** Maps (year_of_first_pub, metric_name, metric_value) → percentile for 8 author metrics.

**Metrics covered:**
- `hindex`, `hindex5y`
- `citedby`, `citedby5y`
- `i10index`, `i10index5y`
- `total_publications`, `total_publications_with_citations`

**How:** Same `PERCENT_RANK()` + `DISTINCT` pattern, partitioned by `(year_of_first_pub, metric_name)`. This makes percentiles **age-aware** — a 5-year-old researcher is compared only to other 5-year-old researchers.

**Used by:**
- `stats_author_current` (Tier 2) — author metric percentiles
- `stats_author_publication_pip_inputs_current` (Tier 3) — num_papers_percentile interpolation for PiP chart X-axis

### dist_pip_auc_scores

**What:** Maps (year_of_first_pub, pip_auc_score) → PiP-AUC percentile.

**Depends on:** `dist_publication_citations` and `dist_author_metrics` (the underlying PiP-AUC view reads from them).

**Used by:** `stats_author_pip_scores_current` (Tier 4) — to rank an author's PiP-AUC score against their cohort.

---

## Stats Views (Tiers 1-4)

### Tier 1: stats_publication_current

**Purpose:** Assign each publication its citation percentile.

**Key technique:** Floor lookup into `dist_publication_citations`:
```sql
SELECT MAX(d.percentile)
FROM dist_publication_citations d
WHERE d.pub_year = p.pub_year AND d.num_citations <= p.num_citations
```
This is O(log n) per publication via the distribution table, not O(n) via live `PERCENT_RANK()`.

**Output columns:** `author_pub_id`, `scholar_id`, `pub_year`, `num_citations`, `num_citations_percentile`

### Tier 1: stats_publication_citations_temporal

**Purpose:** Full citation timeline per publication — yearly citation counts and cumulative totals, with percentiles.

**Key technique:** Generates a year series for each publication (from pub_year to current year), LEFT JOINs actual citations, fills gaps with 0. Computes both yearly and cumulative citation percentiles.

### Tier 2: stats_author_current

**Purpose:** Author summary metrics with age-aware percentiles.

**Output:** h-index, citations, i10-index (current + 5-year), total publications — each with its percentile from `dist_author_metrics`. Also includes name, affiliation, email domain, year of first publication.

**Key technique:** Same floor lookup pattern against `dist_author_metrics`.

### Tier 3: stats_author_publication_pip_inputs_current

**Purpose:** Compute the (X, Y) coordinates for each point on an author's PiP chart.

**Y-axis:** `num_citations_percentile` — from `stats_publication_current`

**X-axis:** `num_papers_percentile` — interpolated from `dist_author_metrics` (the `total_publications` metric). This is a 6-CTE pipeline:

1. **RankedPublications:** Rank author's papers by citation percentile (descending)
2. **Distances:** Look up the num_papers_percentile for the author's paper count and for paper count ± 1
3. **RankedDistances:** Order the distance values
4. **FilteredDistances:** Pick the two bracketing entries from the distribution
5. **AggregatedDistances:** Compute interpolation bounds
6. **InterpolatedResults:** Linear interpolation to get the exact X-axis position

### Tier 4: stats_author_pip_scores_current

**Purpose:** Compute the PiP-AUC score and its percentile.

**How:** Sorts publications by citation percentile (descending), uses `LAG()` to get consecutive (X, Y) pairs, computes trapezoidal areas, sums them. Then does a floor lookup into `dist_pip_auc_scores` for the percentile.

**Output:** `scholar_id`, `pip_auc`, `pip_auc_percentile`, `num_papers`, `year_of_first_pub`

---

## Temporal Views

### stats_author_metrics_temporal_view → stats_author_metrics_temporal (table)

**Purpose:** How an author's h-index, citations, and i10-index evolved over time (year by year).

**How:** For each (author, year) pair, computes what the author's metrics *would have been* at that point in time — using only citations accumulated up to that year. This requires rolling calculations across the full publication × citation-year matrix.

**Cost:** This is the most expensive computation because it materializes O(authors × years × publications) intermediate state. For a single author it's manageable (~50 years × ~100 papers), but for all 15,000+ authors it's substantial.

### intermediate_author_publication_state_temporal

**Purpose:** Intermediate join table used by the temporal view. For each (author, publication, year), tracks the cumulative citation count and citation percentile at that point in time.

---

## Coauthor Views

### coauthor_network

**Purpose:** Extract the coauthor graph from author JSON. Each author's profile contains a list of coauthors with their names, affiliations, and Scholar IDs.

### coauthors_to_add

**Purpose:** Filter `coauthor_network` to find coauthors not yet in the database — candidates for the Refresh & Expand service to crawl.

---

## Materialization Schedule and Cost Rationale

### Current schedule

| What | Schedule | Workflow | Estimated BQ cost |
|------|----------|----------|-------------------|
| Distribution tables (`dist_*`) | Quarterly (Jan 1, Apr 1, Jul 1, Oct 1 at 04:00 UTC) | `bigquery-materialize-distributions.yml` | High per run (full PERCENT_RANK), but only 4x/year |
| Snapshot tables (`*_table`) | Daily at 06:00 UTC | `bigquery-materialize.yml` | Moderate (SELECT * from views for all authors) |

### Why quarterly for distribution tables

Distribution tables capture the *shape* of the population — "what percentile is 100 citations for a 2018 paper?" This shape changes slowly because:

- Adding a few hundred authors to a pool of 15,000+ barely shifts percentile boundaries
- Citation counts for older papers change slowly (a 2015 paper gains maybe 5-10 citations/year)
- New publication years accumulate papers gradually throughout the year

Recomputing quarterly is sufficient. The error from a 3-month-old distribution table is negligible — if your paper was at the 85th percentile in January, it's almost certainly between the 84th and 86th in March.

### Why daily snapshot materialization may be excessive

The snapshot tables (`stats_author_current_table`, `stats_author_pip_scores_current_table`, `stats_author_metrics_temporal`) exist only for:

1. **All-authors ranking page** — needs every author in one query
2. **CSV export** — bulk download of all author stats

Individual author data changes only when that author is re-crawled, which happens at most monthly (90-day staleness threshold). On any given day, fewer than ~200 of 15,000+ authors have new data. Daily materialization recomputes all 15,000+ rows to update ~200.

**Alternatives to consider:**
- **Weekly materialization** — 7x cost reduction, data is at most 7 days stale for bulk exports
- **Event-driven materialization** — trigger after `batch_load_gcs_to_bq` completes, so snapshots update only when new data actually arrives
- **Incremental updates** — MERGE only changed authors into the snapshot tables (requires tracking which authors were updated)

The per-author profile pages are unaffected by this choice — they always query views directly and cache in Firestore.

---

## Query Patterns

### Per-author profile page (cheap, real-time)

```
User visits /results?author_id=XYZ
  → Check Firestore cache (hit: ~50ms, done)
  → Cache miss: query these views for ONE author:
      stats_author_current WHERE scholar_id = 'XYZ'           (~500ms)
      stats_publication_current WHERE scholar_id = 'XYZ'       (~500ms)
      stats_author_publication_pip_inputs_current WHERE ...     (~800ms)
      stats_author_pip_scores_current WHERE scholar_id = 'XYZ' (~500ms)
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
  → Query stats_author_current_table (materialized, all rows)
  → JOIN stats_author_pip_scores_current_table
  → Return ~15,000 rows
  → Generate CSV or render HTML table
```

This cannot use views efficiently because it needs *all* authors — the view would have to compute metrics for every author on every request.

---

## SQL File Reference

| File | Tier | Type | Purpose |
|------|------|------|---------|
| `dist_publication_citations.sql` | 0 | Table (quarterly) | Citation percentile distribution by pub_year |
| `dist_author_metrics.sql` | 0 | Table (quarterly) | Author metric percentile distribution by cohort |
| `dist_pip_auc_scores.sql` | 0 | Table (quarterly) | PiP-AUC percentile distribution by cohort |
| `base_author_publications.sql` | 1 | View | Author → publication list extraction |
| `stats_publication_current.sql` | 1 | View | Per-publication citation percentile |
| `stats_publication_citations_temporal.sql` | 1 | View | Citation timeline per publication |
| `coauthor_network.sql` | 1 | View | Coauthor graph |
| `stats_author_current.sql` | 2 | View | Author metrics + percentiles |
| `coauthors_to_add.sql` | 2 | View | Uncrawled coauthors |
| `intermediate_author_publication_state_temporal.sql` | 2 | View | Per-author-per-year publication state |
| `stats_author_publication_pip_inputs_current.sql` | 3 | View | PiP chart coordinates |
| `stats_author_metrics_temporal.sql` | 3 | View → Table | Temporal metrics (materialized daily) |
| `stats_author_pip_scores_current.sql` | 4 | View | PiP-AUC score + percentile |
| `materialize_stats.sql` | — | Script | Snapshot materialization queries |

All files live under `bigquery/statistics/` except coauthor views which are in `bigquery/coauthor_network/`.

---

## Deployment

### View deployment (on code change)

Workflow: `.github/workflows/bigquery-views.yml`

Triggers on push to `bigquery/**/*.sql` on main. Deploys views in tier order (0 → 4) to respect dependencies. Does **not** refresh distribution or snapshot tables.

### Distribution table refresh (quarterly)

Workflow: `.github/workflows/bigquery-materialize-distributions.yml`

Runs `dist_publication_citations.sql` and `dist_author_metrics.sql` (independent, could be parallel), then `dist_pip_auc_scores.sql` (depends on the first two). Can be triggered manually via `workflow_dispatch`.

### Snapshot table refresh (daily)

Workflow: `.github/workflows/bigquery-materialize.yml`

Runs `CREATE OR REPLACE TABLE ... AS SELECT * FROM <view>` for:
1. `stats_author_current_table` (clustered by scholar_id, year_of_first_pub)
2. `stats_author_pip_scores_current_table` (clustered by scholar_id)
3. `stats_author_metrics_temporal` (clustered by scholar_id, state_year)

---

_Last updated: 2026-03-19_
