# Holocron Scraper — Agent Build Brief

You are being dispatched to **build and run** a Python sidecar scraper for the Holocron hackathon project. Read this entire file before writing code. When in doubt, favor the hackathon-speed path: fewer moving parts, visible output, graceful skips over heroic fixes.

---

## 1. Mission

Produce a **dataset of supplier listings** from five B2B chemical marketplaces that mention a fixed list of precursor-chemical keywords and CAS numbers, then **enrich each unique supplier through the Crustdata Company API**. Output goes to **CSV + SQLite** under `scraping/out/`, plus raw HTML **samples** under `scraping/samples/`. The Rails app will later read these artifacts via dummy wiring — you do **not** need to integrate with Rails, trigger live runs from the app, or touch any Ruby code.

**Two-phase pipeline:**
1. **Phase 1 — Scrape** marketplaces for keyword/CAS hits → produces raw listings with supplier names.
2. **Phase 2 — Enrich** each unique supplier name via Crustdata Company Search → produces structured company records (industry, country, LinkedIn, employee count, etc.) joined back to listings.

---

## 2. Context (Why this exists)

Holocron is a Rails 8 hackathon demo for **Francis (State Dept / ONDCP-directed)** who researches how drug precursor chemicals move through global commerce. She needs evidence that analysts can pull live marketplace signals — not just stare at seeded data. Your output is what makes the demo credible: real listings, real supplier names, real URLs, harvested today.

This is a **defensive / law-enforcement research** use case. Act accordingly: be polite, don't hammer, don't try to bypass CAPTCHAs, don't log in anywhere, don't scrape anything behind authentication.

---

## 3. Targets

From `scraping/targets.txt`:

| Slug | Base URL | Notes |
|---|---|---|
| `tradeford` | http://www.tradeford.com/ | Search: `?q=<term>` on `/search/` likely |
| `chinaexporter` | http://www.chinaexporter.com/ | Inspect site for search endpoint |
| `ecrobot` | https://www.ecrobot.com/ | May be JS-heavy; if so, skip gracefully |
| `chemnet` | https://www.chemnet.com/ | Typically server-rendered search |
| `indiamart_export` | https://export.indiamart.com/ | May require JS; skip if you can't get HTML |

You must **discover the actual search URL for each site** by fetching the homepage and inspecting form actions / links. Don't guess URLs if the obvious ones 404.

---

## 4. Queries

Load from `scraping/targets.txt` (already in repo). For reference:

**Keywords (3):** `Piperidone`, `Nitazene`, `Xylazine`

**CAS numbers (14):** `99918-43-1`, `125541-22-2`, `19099-93-5`, `103-63-9`, `28578-16-7`, `437-38-7`, `990-73-8`, `1443-48-3`, `21409-26-7`, `39742-60-4`, `1235-83-6`, `59708-52-0`, `56030-54-7`, `71195-58-9`

Total queries: 17 × 5 sites = **85 searches**.

---

## 5. Scope — Light

This is the dial. **Do not exceed it** without asking.

- **Top 10 listings per search.** No pagination.
- **Skip sites that 403, WAF-block, or require JS you can't get to render** — log them to `scraping/out/skipped.log` with reason, move on.
- **One retry on transient network errors.** No elaborate retry loops.
- Budget: the full run should complete in **~10–15 minutes wall-clock**.
- Expected total hit rows: ~50–300 (many CAS searches will legitimately return zero — that's fine).

---

## 6. Stack

Keep it boring:

- **Python 3.11+**
- `httpx` (sync client, `follow_redirects=True`)
- `selectolax` (fast HTML parsing — faster than BeautifulSoup, same ergonomics)
- `tenacity` (one-retry decorator)
- `sqlite3` (stdlib)
- `csv` (stdlib)

**Do not use Playwright, Selenium, or any headless browser.** If a site needs JS, skip it with a `skipped.log` entry. Hackathon scope.

Manage deps with a `requirements.txt` in `scraping/`. No Poetry, no Pipenv.

---

## 7. Directory Layout

Create exactly this structure under `scraping/`:

```
scraping/
├── AGENT_BRIEF.md          ← this file, already exists
├── targets.txt             ← already exists, read from it
├── README.md               ← you write: short "how to run" doc
├── requirements.txt        ← you write
├── run.py                  ← CLI entry point (orchestrates phase 1 + 2)
├── scrapers/
│   ├── __init__.py
│   ├── base.py             ← shared BaseScraper class
│   ├── tradeford.py
│   ├── chinaexporter.py
│   ├── ecrobot.py
│   ├── chemnet.py
│   └── indiamart_export.py
├── enrichment/
│   ├── __init__.py
│   └── crustdata.py        ← phase 2: company enrichment via Crustdata API
├── out/
│   ├── hits.csv            ← generated (phase 1)
│   ├── companies.csv       ← generated (phase 2, one row per unique supplier)
│   ├── holocron.sqlite     ← generated (both phases write here)
│   └── skipped.log         ← generated
└── samples/
    └── <site_slug>/
        └── <query>.html    ← raw HTML of one result page per successful query
```

**Reference docs in the repo (read these before coding):**
- `crustdata-api/company-identification-api-docs.md` — full Company API reference (endpoints, filter operators, examples). This is your source of truth for phase 2.
- `crustdata-api/all-docs.md` — broader API surface; use only if Company Search alone is insufficient.

---

## 8. Per-Site Scraper Contract

Every site module exposes one class:

```python
class TradefordScraper(BaseScraper):
    slug = "tradeford"
    base_url = "http://www.tradeford.com/"

    def search(self, query: str) -> list[Listing]:
        """Return up to 10 Listing rows for this query. Raise SkipSite to abort this site."""
```

`BaseScraper` provides:
- Shared `httpx.Client` with reasonable User-Agent (`Holocron-Research-Bot/0.1 (+hackathon)`)
- `fetch(url)` with 1-retry + 1.5s sleep between calls (be polite)
- `save_sample(query, html)` → writes to `samples/<slug>/<query>.html`
- `SkipSite(Exception)` — raise this to skip the site entirely with a reason

Each `search()` implementation:
1. Builds the search URL for the site
2. Fetches HTML (or raises SkipSite if 403/JS-blocked)
3. Saves the first result page to `samples/<slug>/<query>.html` if any hits found
4. Parses up to 10 listings into `Listing` objects
5. Returns the list

**Don't over-engineer.** If a site's listing card has 8 fields but you can only cleanly extract 3, extract 3. Leave the rest `None`.

---

## 9. Data Model

```python
@dataclass
class Listing:
    source_site: str          # slug, e.g. "tradeford"
    query_type: str           # "keyword" or "cas"
    query: str                # the actual search term
    listing_url: str          # absolute URL to the listing / supplier page
    listing_title: str        # product or listing title
    supplier_name: str | None
    supplier_country: str | None
    price: str | None         # keep as string; sites use weird formats
    quantity: str | None      # same
    snippet: str | None       # ~200 chars of text around the match
    scraped_at: str           # ISO 8601 UTC
    raw_html_path: str | None # relative path to sample file, if saved
```

CSV column order = dataclass field order.

SQLite schema mirrors the dataclass. Single table `hits`:

```sql
CREATE TABLE IF NOT EXISTS hits (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_site TEXT NOT NULL,
  query_type TEXT NOT NULL,
  query TEXT NOT NULL,
  listing_url TEXT NOT NULL,
  listing_title TEXT,
  supplier_name TEXT,
  supplier_country TEXT,
  price TEXT,
  quantity TEXT,
  snippet TEXT,
  scraped_at TEXT NOT NULL,
  raw_html_path TEXT
);
CREATE INDEX IF NOT EXISTS idx_hits_site ON hits(source_site);
CREATE INDEX IF NOT EXISTS idx_hits_query ON hits(query);
```

Write to **both** CSV and SQLite in the same run. Overwrite on re-run (idempotent).

---

## 10. Phase 2 — Crustdata Enrichment

After phase 1 produces `hits.csv`, enrich each **unique non-null `supplier_name`** through the Crustdata Company Search API.

**Auth:** API key is `CRUSTDATA_API_KEY` in the project root `.env` file (already populated). Load it with `python-dotenv` or `os.getenv`. **Never hardcode the key. Never write it to logs, samples, or CSV.**

**Endpoint:** `POST https://api.crustdata.com/screener/companydb/search`
Header: `Authorization: Token $CRUSTDATA_API_KEY`

**Strategy (read `crustdata-api/company-identification-api-docs.md` for filter syntax):**

For each unique supplier name from phase 1:
1. POST a search with `{"filters": {"filter_type": "company_name", "type": "=", "value": "<supplier_name>"}, "limit": 3}`.
2. If results: take the top match. Capture: `company_name`, `company_website`, `linkedin_url`, `hq_country`, `hq_city`, `industry`, `employee_count`, `description`.
3. If zero results: record the lookup as `crustdata_status="not_found"` — don't retry with looser filters in phase 2 v1.
4. Write one row per unique supplier to `companies.csv` and a `companies` table in SQLite.

**Credit budget:**
Crustdata charges credits per result-returning call. Phase 1 will surface maybe 30–100 unique supplier names — that's a small spend. **Hard cap: 200 enrichment calls per run.** If phase 1 produces more unique suppliers than that, deduplicate and take the first 200 (log the rest as `crustdata_status="budget_skipped"`).

**Companies table schema:**

```sql
CREATE TABLE IF NOT EXISTS companies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  raw_supplier_name TEXT NOT NULL UNIQUE,
  crustdata_status TEXT NOT NULL,    -- 'matched' | 'not_found' | 'error' | 'budget_skipped'
  company_name TEXT,
  company_website TEXT,
  linkedin_url TEXT,
  hq_country TEXT,
  hq_city TEXT,
  industry TEXT,
  employee_count INTEGER,
  description TEXT,
  raw_response_json TEXT,            -- store the full top-match JSON for later inspection
  enriched_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_companies_country ON companies(hq_country);
```

**Politeness toward the API:** 0.3s sleep between calls, one retry on 5xx, fail-soft on 4xx (record `crustdata_status="error"`, log, continue).

**Skip phase 2 if:** `CRUSTDATA_API_KEY` is missing, or `--no-enrich` flag passed. The run should still produce `hits.csv` successfully without enrichment.

---

## 11. CLI (`run.py`)

Support:

```
python run.py                        # run all sites × all queries, then enrich
python run.py --site tradeford       # one site (still enriches new suppliers found)
python run.py --query Piperidone     # one query across all sites
python run.py --dry-run              # print planned searches, don't fetch
python run.py --no-enrich            # skip phase 2
python run.py --enrich-only          # skip phase 1, re-enrich from existing hits.csv
```

Logging: plain `logging` stdlib, level INFO to stderr. Print a final summary: hits per site, skipped sites, unique suppliers enriched, Crustdata matches vs not_found, total runtime.

---

## 12. Politeness

- **1.5s sleep between requests** to the same host.
- Single User-Agent: `Holocron-Research-Bot/0.1 (+hackathon)`. Don't rotate or spoof browsers.
- Respect `robots.txt` only loosely — this is law-enforcement-adjacent research, but still: if a site has `Disallow: /`, skip it.
- No cookies, no logins, no CAPTCHAs.

---

## 13. Error Handling

- Transient network error → retry once, then log and skip that query.
- Site returns 403 on homepage → raise SkipSite, log to `skipped.log`, move to next site.
- Parse error on a single listing → log warning, skip that listing, keep going.
- **Never crash the whole run** because one site/query failed. The script must always produce `hits.csv` and `holocron.sqlite`, even if they're partial.

---

## 14. Dummy Wiring (what you do NOT do)

The Rails app already has a `Scraper` model, `ScrapesController#run` stub, and seeded dummy `Source` records. **Leave all of that alone.** You are not wiring Rails → Python. The Rails side will later be pointed at `scraping/out/hits.csv` via a rake task that someone else writes.

Do **not**:
- Edit any Ruby files
- Add Python→Rails bridges, API endpoints, or webhooks
- Modify `app/`, `config/`, `db/` anywhere

---

## 15. README.md (what you write)

One page, under 80 lines. Covers:
1. What this is (one paragraph)
2. Install: `cd scraping && pip install -r requirements.txt`
3. Run: the four `run.py` invocations above
4. Outputs: where files land
5. Known skipped sites: if you hit JS walls, list them here

---

## 16. Success Criteria

When you're done:

1. `python run.py` completes in under 25 minutes without uncaught exceptions.
2. `scraping/out/hits.csv` has ≥ 20 rows spanning ≥ 2 sites.
3. `scraping/out/companies.csv` has one row per unique non-null `supplier_name` from `hits.csv`, with `crustdata_status` set on every row.
4. `scraping/out/holocron.sqlite` exists; `SELECT COUNT(*) FROM hits` matches `hits.csv` row count, and `SELECT COUNT(*) FROM companies` matches `companies.csv`.
5. `scraping/samples/` contains at least one HTML file per site that didn't skip.
6. `scraping/out/skipped.log` explains why any skipped site was skipped.
7. `scraping/README.md` accurately describes how to reproduce your run, and explicitly mentions `--no-enrich` and `--enrich-only` modes.
8. No Crustdata API key (or any secret) appears anywhere in the repo outside `.env`.

If you can't hit #2 (too many sites block), produce what you can, document it clearly in `skipped.log` and `README.md`, and stop. Don't escalate to Playwright or heavier tooling without asking.

---

## 17. Before You Finish

Run this sanity check from the repo root:

```bash
cd scraping
python run.py
ls -la out/ samples/
sqlite3 out/holocron.sqlite "SELECT source_site, COUNT(*) FROM hits GROUP BY source_site;"
sqlite3 out/holocron.sqlite "SELECT crustdata_status, COUNT(*) FROM companies GROUP BY crustdata_status;"
head -3 out/hits.csv
head -3 out/companies.csv
```

Paste that output into your final message so the orchestrator can verify.
