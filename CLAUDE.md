# CLAUDE.md — ipeirotis-org/scholar_v2

## Purpose

Scholar Analytics v2 — a distributed system for analyzing Google Scholar data with percentile-based, age-aware research metrics. Implements the **PiP-AUC (Paper-in-Percentile Area Under Curve)** scoring methodology. Serves an interactive Flask web app for visualizing author and publication analytics.

## Architecture

```
scholar_v2/
├── app/                          # Flask web application
│   ├── main.py                   # Routes: /, /results, /download, /api/*
│   ├── scholar.py                # Google Scholar integration (scholarly lib)
│   ├── data_analysis.py          # BigQuery queries + Firestore cache
│   ├── visualization.py          # Matplotlib plot generation
│   ├── refresh.py                # Data refresh orchestration
│   ├── queue_handler.py          # Task queue interface
│   ├── coauthor_service.py       # Co-author discovery
│   ├── templates/                # Jinja2 HTML templates (9 files)
│   └── static/                   # CSS, JS assets
├── functions/                    # Google Cloud Functions
│   ├── fetch_author/             # Fetch author from Scholar → GCS
│   ├── fetch_publication/        # Fetch publication details → GCS
│   ├── find_scholar_id_from_name/# Search authors by name
│   └── batch_load_gcs_to_bq/    # Load GCS JSON → BigQuery
├── shared/                       # Shared code (app + functions)
│   ├── config.py                 # GCP project, queues, regions
│   ├── services/                 # Firestore, BigQuery, GCS, Cloud Tasks
│   └── repositories/             # Author + Publication CRUD
├── bigquery/                     # SQL views for analytics
│   ├── statistics/               # 7 SQL views (PiP-AUC, percentiles, temporal)
│   └── coauthor_network/         # Co-author graph analysis
├── scripts/                      # One-off scripts
├── .github/workflows/            # CI/CD (Cloud Run + multi-region Functions)
└── Dockerfile                    # Container for Cloud Run
```

## Data Flow

1. **Fetch**: User enters Scholar ID → task queued → Cloud Function fetches via `scholarly` → JSON saved to GCS
2. **Load**: `batch_load_gcs_to_bq` converts JSON → NDJSON → BigQuery tables
3. **Analyze**: SQL views compute percentiles, PiP-AUC scores, temporal metrics
4. **Serve**: Flask app queries BigQuery → caches in Firestore → renders matplotlib plots

## Tech Stack

- **Flask** — web framework on Google Cloud Run
- **scholarly** — Google Scholar scraping
- **Google Cloud Platform**: Cloud Run, Cloud Functions (4 functions x 9 regions), Firestore, BigQuery, Cloud Storage, Cloud Tasks, Secret Manager
- **matplotlib** — visualization (base64 PNG embedded in HTML)
- **pandas / numpy** — data manipulation
- **Docker** — containerization

## Key Infrastructure

- **GCP Project**: `scholar-version2`
- **BigQuery Dataset**: `scholar_raw_data` (tables: `author`, `pub`)
- **GCS Bucket**: `scholar_data_share` (prefixes: `authors_json/`, `publications_json/`)
- **Cloud Tasks Queues**: `process-authors`, `process-pubs` (location: `northamerica-northeast1`)
- **Region Rotation**: Functions deploy across 9 US regions; requests rotate hourly to avoid Scholar rate-limiting

## Known Issues

1. **Temporal stats disabled** — code in `main.py:157-173` is commented out; temporal plot functions exist but are unreachable from web UI
2. **Firestore saves disabled in Cloud Functions** — `save_author()` and `save_publication()` are commented out; data only goes to GCS now (depends on BigQuery ETL)
3. **Task queue status queries don't work** — `get_number_of_tasks_in_queue()` returns None; comments suggest Firestore-based status tracking instead
4. **Hardcoded project/collection names** — no env var overrides in all places
5. **No retry on GCS upload failure** — returns None silently

## CI/CD

- **main.yml**: Push to `main` → build Docker → deploy Cloud Run
- **function.yml**: Matrix deployment of 4 functions across 9 regions

## TODO.md

This repo's TODO.md feeds into the `Research: Scholar Analytics` section of the main tasks repo (`ipeirotis/tasks`).
