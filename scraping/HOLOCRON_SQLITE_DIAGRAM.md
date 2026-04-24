# Holocron SQLite Diagram

This documents the app-facing SQLite file produced by the sidecar scraper:

- `scraping/out/holocron.sqlite`

That database is rebuilt on each run of `scraping/run.py`.

## Overview

The Holocron scraper writes two tables:

- `hits`: raw marketplace listing hits from phase 1
- `companies`: Crustdata enrichment rows from phase 2

The relationship is logical rather than enforced by a foreign key:

- `hits.supplier_name`
- `companies.raw_supplier_name`

One company row is intended to represent one unique non-null supplier name found in `hits`.

## Diagram

```text
scraping/out/holocron.sqlite
|
+-- hits
\-- companies
```

```mermaid
flowchart LR
    H[hits] -. supplier_name .-> C[companies.raw_supplier_name]
```

## Table: `hits`

Phase 1 output. One row per marketplace listing match.

```text
hits
|
+-- id INTEGER PK AUTOINCREMENT
+-- source_site TEXT NOT NULL
+-- query_type TEXT NOT NULL
+-- query TEXT NOT NULL
+-- listing_url TEXT NOT NULL
+-- listing_title TEXT
+-- supplier_name TEXT
+-- supplier_country TEXT
+-- price TEXT
+-- quantity TEXT
+-- snippet TEXT
+-- scraped_at TEXT NOT NULL
\-- raw_html_path TEXT
```

### Purpose of key columns

- `source_site`: which marketplace produced the hit, such as `tradeford` or `chemnet`
- `query_type`: `keyword` or `cas`
- `query`: the exact term searched
- `listing_url`: listing or supplier page URL
- `supplier_name`: raw supplier string scraped from the page
- `raw_html_path`: relative path to the saved sample HTML page when available

### Indexes

- `idx_hits_site` on `source_site`
- `idx_hits_query` on `query`

## Table: `companies`

Phase 2 output. One row per unique supplier name considered for enrichment.

```text
companies
|
+-- id INTEGER PK AUTOINCREMENT
+-- raw_supplier_name TEXT NOT NULL UNIQUE
+-- crustdata_status TEXT NOT NULL
+-- company_name TEXT
+-- company_website TEXT
+-- linkedin_url TEXT
+-- hq_country TEXT
+-- hq_city TEXT
+-- industry TEXT
+-- employee_count INTEGER
+-- description TEXT
+-- raw_response_json TEXT
\-- enriched_at TEXT NOT NULL
```

### Purpose of key columns

- `raw_supplier_name`: the supplier name exactly as deduplicated from `hits`
- `crustdata_status`: `matched`, `not_found`, `error`, or `budget_skipped`
- `raw_response_json`: stored top-match payload for later inspection
- `company_name` through `description`: normalized company metadata from Crustdata

### Indexes

- `idx_companies_country` on `hq_country`

## Relationship Model

This DB does not use foreign keys. The join is name-based:

```sql
SELECT
  h.source_site,
  h.query,
  h.listing_title,
  h.supplier_name,
  c.crustdata_status,
  c.company_name,
  c.hq_country,
  c.linkedin_url
FROM hits h
LEFT JOIN companies c
  ON h.supplier_name = c.raw_supplier_name;
```

That means:

- rows in `hits` may have `supplier_name = NULL`
- multiple `hits` rows can map to one `companies` row
- company matching quality depends on the raw supplier string being stable

## End-to-End Data Flow

```text
search query
  -> scrape listing hit
  -> write row to hits
  -> collect unique non-null supplier_name values
  -> enrich each supplier with Crustdata
  -> write one row per supplier to companies
```

## App Mental Model

If the app wants to look live while using this dataset, the most useful pattern is:

1. Read `hits` as the event stream of discovered listings.
2. Read `companies` as delayed enrichment data attached to supplier names.
3. Join them in the UI by `supplier_name = raw_supplier_name`.

That gives the user a believable two-step workflow:

1. listing discovered
2. supplier enriched

## Representative Queries

Count hits by source:

```sql
SELECT source_site, COUNT(*)
FROM hits
GROUP BY source_site
ORDER BY COUNT(*) DESC;
```

Count enrichment status:

```sql
SELECT crustdata_status, COUNT(*)
FROM companies
GROUP BY crustdata_status;
```

Show hits with enrichment:

```sql
SELECT
  h.source_site,
  h.query_type,
  h.query,
  h.listing_title,
  h.supplier_name,
  c.company_name,
  c.hq_country,
  c.industry
FROM hits h
LEFT JOIN companies c
  ON h.supplier_name = c.raw_supplier_name
ORDER BY h.scraped_at DESC
LIMIT 50;
```

## CSV Parity

The SQLite tables mirror the CSVs written beside them:

- `scraping/out/hits.csv` mirrors `hits`
- `scraping/out/companies.csv` mirrors `companies`

So the app can consume either:

- SQLite for structured querying
- CSV for simpler import/demo wiring
