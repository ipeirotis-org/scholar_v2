# Component 5: Refresh & Expand (Not Implemented)

> This component was planned but never implemented. Data freshness is now managed by the [Dataset Ingestion](architecture-ingestion.md) pipeline's monthly S2 bulk dataset updates.

The original design called for a separate service to orchestrate data freshness by:
- Identifying stale authors and enqueuing re-crawl tasks
- Expanding the database via coauthor graph analysis
- Handling user-triggered refresh requests

With the migration to Semantic Scholar bulk datasets, data freshness is handled differently:
- **Monthly ingestion** from S2 (diff-based) keeps all 200M+ papers current
- **Cache Layer** handles cache invalidation after ingestion
- **No per-author crawling** is needed since S2 provides the full dataset

See [ARCHITECTURE.md](ARCHITECTURE.md) for the current system design.
