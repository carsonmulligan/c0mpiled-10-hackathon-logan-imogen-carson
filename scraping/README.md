# Holocron Scraper

This sidecar scraper collects marketplace listings for a fixed set of precursor-chemical keywords and CAS numbers, then enriches unique supplier names with Crustdata company search results. It writes reproducible CSV and SQLite artifacts under `scraping/out/` and stores raw HTML samples for successful result pages under `scraping/samples/`.

## Install

```bash
cd scraping
pip install -r requirements.txt
```

## Run

```bash
python run.py
python run.py --site tradeford
python run.py --query Piperidone
python run.py --dry-run
python run.py --no-enrich
python run.py --enrich-only
```

## Outputs

- `out/hits.csv`: raw listing hits from marketplace searches
- `out/companies.csv`: one enrichment row per unique non-null supplier name
- `out/holocron.sqlite`: SQLite database with `hits` and `companies`
- `out/skipped.log`: skipped-site and query-level failures with reasons
- `samples/<site>/<query>.html`: saved first result page for successful hit queries

## Known skipped sites

Some targets may skip when they return 403, declare `Disallow: /` in `robots.txt`, need JavaScript rendering, or the runtime environment has no outbound network access. In those cases the run still completes and records the reason in `out/skipped.log`.
