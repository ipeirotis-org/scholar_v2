# Crawler (Retired)

> This component has been retired. Data is now sourced from **Semantic Scholar bulk datasets** via the [Dataset Ingestion](architecture-ingestion.md) pipeline.

The original crawler used Google Scholar via the `scholarly` library to fetch author and publication data. It was replaced when the system migrated to Semantic Scholar's bulk dataset API, which provides comprehensive coverage of 200M+ papers and 102M authors without rate-limiting concerns.

See [architecture-ingestion.md](architecture-ingestion.md) for the current data ingestion architecture.
