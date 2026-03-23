# Component 3: Analytics

> Part of [System Architecture](ARCHITECTURE.md)

**Purpose:** Compute all metrics, percentiles, and scores from raw BigQuery data.

**Input:** BigQuery raw tables via `_latest` deduplication views.

**Output:** BigQuery views and materialized tables with computed metrics.

## What it computes

1. **Publication metrics:** Citation counts and citation percentiles per publication, partitioned by publication year
2. **Author metrics:** h-index, citations, i10-index (and 5-year variants), total publications — with percentiles by career-stage cohort (year of first publication)
3. **PiP-AUC scores:** Paper-in-Percentile Area Under Curve via trapezoidal integration, with percentile ranking
4. **Temporal metrics:** Historical evolution of author h-index, citations, i10-index over time
5. **Coauthor network:** Coauthor graph extracted from author profiles

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

Tier 2 — Distribution Tables (materialized quarterly, ONLY place PERCENT_RANK runs):
  dist_publication_citations          — (pub_year, num_citations) → percentile
  dist_publication_citations_temporal — (pub_year, citation_year, age) → 4 percentiles
  dist_author_metrics                 — (cohort, metric, value) → percentile (8 metrics)
  dist_author_metrics_temporal        — (cohort, state_year, metric, value) → percentile (7 metrics)
  dist_pip_auc_scores                 — (cohort, pip_auc_score) → percentile
  dist_pip_auc_scores_temporal        — (cohort, state_year, pip_auc_score) → percentile

Tier 3 — Ranked Views (cheap JOINs of Tier 1 + Tier 2):
  ranked_publication_current            — adds num_citations_percentile
  ranked_publication_citations_temporal  — adds 4 percentile columns
  ranked_author_current                 — adds 8 percentile columns
  ranked_author_metrics_temporal        — adds 7 percentile columns
  ranked_author_pip_scores_current      — adds pip_auc_score_percentile
  ranked_author_pip_scores_temporal     — adds pip_auc_score_percentile
```

## Materialization strategy

> Full details: [ANALYTICS.md](ANALYTICS.md)

There are two materialization schedules with different cost profiles:

**Quarterly — distribution tables** (`bigquery-materialize-distributions.yml`, 04:00 UTC, Jan/Apr/Jul/Oct):

| Table | What it computes |
|---|---|
| `dist_publication_citations` | PERCENT_RANK by pub_year |
| `dist_publication_citations_temporal` | PERCENT_RANK for 4 temporal citation metrics |
| `dist_author_metrics` | PERCENT_RANK by cohort for 8 metrics |
| `dist_author_metrics_temporal` | PERCENT_RANK by cohort+year for 7 metrics |
| `dist_pip_auc_scores` | PiP-AUC percentiles (depends on dist 1+3) |
| `dist_pip_auc_scores_temporal` | Temporal PiP-AUC percentiles (depends on dist 2+4) |

These are the **only** place `PERCENT_RANK()` runs. Output is small (DISTINCT values only) and the population shape changes slowly — recomputing quarterly introduces negligible error.

**Daily — snapshot tables** (`bigquery-materialize.yml`, 06:00 UTC):

| Table | Source |
|---|---|
| `ranked_author_current_table` | `ranked_author_current` view |
| `ranked_author_pip_scores_current_table` | `ranked_author_pip_scores_current` view |
| `ranked_author_metrics_temporal_table` | `ranked_author_metrics_temporal` view |
| `ranked_author_pip_scores_temporal_table` | `ranked_author_pip_scores_temporal` view |

These exist only for the all-authors ranking page and CSV export. **Per-author profile pages query the ranked views directly** (cheap via distribution table lookups, cached in Firestore).

**Cost note:** Individual author data changes at most monthly (90-day re-crawl threshold). Daily snapshot materialization recomputes ~15,000 rows when typically fewer than 200 have changed. This is an area where event-driven or weekly materialization could reduce cost without meaningful staleness impact.

## Boundaries

| | Source | Target |
|---|---|---|
| **Reads** | BigQuery raw tables (`scholar_raw_data.author`, `scholar_raw_data.pub`) via `_latest` views | |
| **Writes** | | BigQuery views and materialized tables |

## Implementation

| File | Role |
|---|---|
| `bigquery/statistics/*.sql` | All view and table definitions |
| `bigquery/coauthor_network/*.sql` | Coauthor graph views |
| `.github/workflows/bigquery-views.yml` | CI/CD: deploy views in dependency order |
| `.github/workflows/bigquery-materialize.yml` | CI/CD: daily materialization |
