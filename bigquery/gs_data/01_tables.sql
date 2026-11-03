-- gs_data: Google Scholar profile snapshots (profiles only; no per-paper histograms).
-- Append-only. Writers: gs_crawler (source='serpapi') and 03_seed_from_legacy.sql
-- (source='legacy_scholarly'). Schemas must match gs_crawler/writer.py.
CREATE SCHEMA IF NOT EXISTS `scholar-version2.gs_data` OPTIONS (location = 'US');

CREATE TABLE IF NOT EXISTS `scholar-version2.gs_data.author_snapshots` (
  scholar_id STRING NOT NULL,
  fetched_at TIMESTAMP NOT NULL,
  source STRING NOT NULL,
  name STRING,
  affiliation STRING,
  email_domain STRING,
  interests ARRAY<STRING>,
  citedby INT64,
  hindex INT64,
  i10index INT64,
  citedby_since INT64,   -- GS "Since YYYY" column (last ~5 years)
  hindex_since INT64,
  i10index_since INT64,
  since_year INT64,
  cites_per_year STRING, -- JSON {"year": citations} from the profile chart
  coauthors ARRAY<STRUCT<scholar_id STRING, name STRING, affiliation STRING>>,
  num_publications INT64
)
PARTITION BY DATE(fetched_at)
CLUSTER BY scholar_id;

CREATE TABLE IF NOT EXISTS `scholar-version2.gs_data.publication_snapshots` (
  scholar_id STRING NOT NULL,
  author_pub_id STRING NOT NULL,  -- GS citation_id, "SCHOLARID:PUBID"
  fetched_at TIMESTAMP NOT NULL,  -- equals the author snapshot's fetched_at
  source STRING NOT NULL,
  title STRING,
  authors STRING,
  venue STRING,
  pub_year INT64,
  num_citations INT64
)
PARTITION BY DATE(fetched_at)
CLUSTER BY scholar_id, author_pub_id;

-- On-demand requests (cache misses); gs_crawler refresh crawls these first.
CREATE TABLE IF NOT EXISTS `scholar-version2.gs_data.refresh_queue` (
  scholar_id STRING NOT NULL,
  requested_at TIMESTAMP NOT NULL,
  source STRING
);

-- GS author IDs from Wikidata (P1960) for name search of unprofiled authors.
-- Replaced in full by gs_crawler CRAWL_MODE=wikidata (schema in gs_crawler/wikidata.py).
CREATE TABLE IF NOT EXISTS `scholar-version2.gs_data.wikidata_authors` (
  scholar_id STRING NOT NULL,
  wikidata_id STRING NOT NULL,
  name STRING NOT NULL,
  description STRING,   -- Wikidata English description, shown under the name in search
  sitelinks INT64,      -- number of Wikimedia sites with an article about the person
  enwiki_title STRING,
  enwiki_views_60d INT64,  -- English Wikipedia pageviews, last 60 days (ingestion priority)
  fetched_at TIMESTAMP NOT NULL,
  died BOOL,            -- has a date of death (P570): never ingested
  is_scholarly BOOL,    -- researcher/engineer/academic/university teacher/physician occupation
  is_politician BOOL    -- politician occupation: never ingested
);
-- Migration for tables created before the flags existed (gs_crawler/scheduler.py runs it too).
ALTER TABLE `scholar-version2.gs_data.wikidata_authors`
  ADD COLUMN IF NOT EXISTS died BOOL,
  ADD COLUMN IF NOT EXISTS is_scholarly BOOL,
  ADD COLUMN IF NOT EXISTS is_politician BOOL;

-- Permanent crawl failures (no profile behind the ID). The refresh scheduler skips
-- an ID for FAILURE_COOLDOWN_DAYS after its last failure. Written by gs_crawler.
CREATE TABLE IF NOT EXISTS `scholar-version2.gs_data.crawl_failures` (
  scholar_id STRING NOT NULL,
  failed_at TIMESTAMP NOT NULL,
  error STRING
);
