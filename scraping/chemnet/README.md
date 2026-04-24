# Precursor Chemical Dataset (ChemNet+)

A multi-source dataset on **drug precursor chemicals, controlled substances, and dual-use chemical export controls**, built for a government-hackathon project that maps illicit-supply-chain signal against legal/regulatory ground truth.

Scraped from ~15 public sources in parallel on 2026-04-24. Each source lives under `sources/<name>/` with its own data, schema, metadata card, and samples so an LLM (or a human) can pick it up cold.

## Quick start for LLMs / RAG pipelines

- **Single-file everything**: `chemnet.sqlite3` — one database, one table per source + a unified `compounds_union` view.
- **Per-source JSONL**: `sources/<name>/data/*.jsonl` — one compound per line, good for streaming into an embedding pipeline.
- **Per-source CSV**: `sources/<name>/data/*.csv` — for pandas / duckdb / BigQuery.
- **Schema**: `sources/<name>/schema/*.md` — human-readable column descriptions.
- **Metadata**: `sources/<name>/metadata.yaml` — URL(s), fetch date, row counts, authority, license.

Every file has provenance. Every row has a `source_url`.

## Source index

| Tag              | Source                                       | Authority                | Type             |
|------------------|----------------------------------------------|--------------------------|------------------|
| `dea`            | 21 CFR 1310 + 1308 + Orange Book             | US DEA                   | Regulatory       |
| `incb`           | Red / Yellow / Green lists                   | UN INCB                  | Regulatory (intl)|
| `pubchem`        | PubChem classifications & properties         | NIH PubChem              | Reference        |
| `wikipedia`      | Lists of controlled / precursor chemicals    | Wikipedia                | Reference        |
| `export_controls`| Australia Group / OPCW / BIS CCL             | Multiple (gov)           | Regulatory       |
| `eu_uk`          | EU Reg 273/2004 + 111/2005 + UK MDA          | EU / UK.gov              | Regulatory       |
| `chemnet_com`    | chemnet.com B2B supplier listings            | chemnet.com              | Marketplace      |
| `chem_b2b`       | LookChem, ChemicalBook, Guidechem, Molbase   | B2B directories          | Marketplace      |
| `chem_china`     | Made-In-China, echemi, hxchem, wap.china.cn, ecasb | B2B directories    | Marketplace      |
| `chem_intl`      | Alibaba, IndiaMart, supplierlist, lobasources| B2B directories          | Marketplace      |

See `MANIFEST.json` for machine-readable index.

## Dataset intent

Three kinds of rows in one joined dataset:

1. **Ground truth**: what's a precursor, what's scheduled, what's on a control list — from DEA / INCB / OPCW / Australia Group / EU / UK.
2. **Reference chemistry**: CAS numbers, IUPAC names, molecular formulas, SMILES, InChI — from PubChem and Wikipedia.
3. **Signal**: who's selling what, from where — from public B2B directories.

The hackathon hypothesis: overlay (3) on (1)+(2) to find suppliers listing scheduled precursors that regulators should care about. All data is public.

## Directory layout

```
data/chemnet/
├── README.md                 # (this file)
├── MANIFEST.json             # machine-readable source index + row counts
├── SCHEMA.md                 # unified schema reference
├── chemnet.sqlite3           # master merged DB
├── chemnet_all.csv           # master merged CSV (one row per compound+source)
├── chemnet_compounds.jsonl   # one compound per line, merged
│
├── sources/                  # LLM-friendly per-source dir
│   ├── dea/
│   │   ├── data/             # CSVs + JSONL
│   │   ├── schema/           # column descriptions
│   │   ├── metadata.yaml     # URLs, dates, counts, provenance
│   │   ├── samples/          # 10 sample rows
│   │   └── README.md
│   ├── incb/
│   ├── pubchem/
│   ├── wikipedia/
│   ├── export_controls/
│   ├── eu_uk/
│   ├── chemnet_com/
│   ├── chem_b2b/
│   ├── chem_china/
│   └── chem_intl/
│
├── scrape_*.py               # per-source scrapers (reproducible)
├── merge.py                  # builds chemnet.sqlite3 from per-source dbs
├── organize_for_llm.py       # builds sources/<name>/ from dbs + csvs
│
├── csv/                      # raw flat CSVs per source (pre-organization)
├── dbs/                      # per-source SQLite files (pre-merge)
├── raw/                      # raw HTML/PDF/JSON (gitignored; regenerate)
└── logs/                     # scrape logs (gitignored)
```

## Reproducibility

Every scraper is pure Python3 + `requests` + `bs4`. To rebuild from scratch:

```bash
cd data/chemnet
python3 scrape_dea.py
python3 scrape_incb.py
python3 scrape_pubchem.py
python3 scrape_wikipedia.py
python3 scrape_export_controls.py
python3 scrape_eu_uk.py
python3 scrape_chemnet_com.py
python3 scrape_chem_b2b.py
python3 scrape_chem_china.py
python3 scrape_chem_intl.py

python3 merge.py               # merges into chemnet.sqlite3
python3 organize_for_llm.py    # rebuilds sources/ directory
```

Raw fetches are cached under `raw/`, so re-runs are cheap.

## Ethics & scope

- All sources are public.
- We do **not** scrape personal contact info (personal phone/email) — only company name, country, website.
- The marketplace data is descriptive of what's publicly listed; it is not a finding that any listing is unlawful.
- Regulatory lists are quoted for identification — full legal effect is in the official text.

## License

Data carries the license of its source. See each `sources/<name>/metadata.yaml` for per-source licensing. Scraper code is MIT (see `LICENSE.txt` when added).
