"""
Organize per-source scraped data into an LLM-friendly directory tree.

Reads: dbs/*.sqlite3, csv/*.csv
Writes: sources/<name>/{data,schema,samples}/ + metadata.yaml + README.md

Safe to re-run. Overwrites sources/<name>/* but not the scrapers' raw dirs.
"""
from __future__ import annotations

import csv
import json
import sqlite3
import textwrap
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent
DB_DIR = ROOT / "dbs"
CSV_DIR = ROOT / "csv"
SOURCES_DIR = ROOT / "sources"
SOURCES_DIR.mkdir(exist_ok=True)

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")

# Human-readable source metadata. Keep short — per-source README will pull from here.
SOURCE_CATALOG = {
    "dea": {
        "title": "US DEA — 21 CFR 1308 & 1310 + Orange Book",
        "authority": "US Drug Enforcement Administration",
        "type": "regulatory",
        "jurisdiction": "US federal",
        "urls": [
            "https://www.deadiversion.usdoj.gov/21cfr/cfr/1310/1310_02.htm",
            "https://www.deadiversion.usdoj.gov/21cfr/cfr/1308/1308_11.htm",
            "https://www.deadiversion.usdoj.gov/21cfr/cfr/1308/1308_12.htm",
            "https://www.deadiversion.usdoj.gov/21cfr/cfr/1308/1308_13.htm",
            "https://www.deadiversion.usdoj.gov/21cfr/cfr/1308/1308_14.htm",
            "https://www.deadiversion.usdoj.gov/21cfr/cfr/1308/1308_15.htm",
            "https://www.deadiversion.usdoj.gov/schedules/orangebook/c_cs_alpha.pdf",
        ],
        "license": "US government work, public domain.",
        "description": (
            "US federal controlled-substance and precursor-chemical lists. "
            "21 CFR 1310.02 enumerates List I and List II precursors; 1308.11–15 "
            "enumerate Schedules I–V; the Orange Book is a comprehensive DEA "
            "cross-reference of controlled substances."
        ),
    },
    "incb": {
        "title": "INCB Red / Yellow / Green Lists",
        "authority": "UN International Narcotics Control Board",
        "type": "regulatory (intl)",
        "jurisdiction": "international (1961/1971/1988 UN conventions)",
        "urls": [
            "https://www.incb.org/incb/en/precursors/Red_Lists/red-list.html",
            "https://www.incb.org/incb/en/narcotic-drugs/Yellowlist/yellow-list.html",
            "https://www.incb.org/incb/en/psychotropics/green-list/green-list.html",
        ],
        "license": "INCB publications; attribution to INCB required.",
        "description": (
            "Red List: precursors controlled under the 1988 UN Convention "
            "(Tables I & II). Yellow List: narcotic drugs under the 1961 "
            "Single Convention. Green List: psychotropics under the 1971 "
            "Convention."
        ),
    },
    "pubchem": {
        "title": "PubChem — DEA & CWC classification + compound properties",
        "authority": "NIH National Library of Medicine — PubChem",
        "type": "reference",
        "jurisdiction": "global",
        "urls": [
            "https://pubchem.ncbi.nlm.nih.gov/rest/pug/",
            "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/",
        ],
        "license": "PubChem data is free for use in accordance with NCBI's data usage policy.",
        "description": (
            "Compounds annotated by PubChem as DEA-controlled or CWC-scheduled, "
            "with IUPAC name, molecular formula, SMILES, InChI, and CAS identifiers."
        ),
    },
    "wikipedia": {
        "title": "Wikipedia — controlled substance & precursor lists",
        "authority": "Wikipedia (crowd-sourced)",
        "type": "reference",
        "jurisdiction": "global (varies by page)",
        "urls": ["https://en.wikipedia.org/"],
        "license": "CC BY-SA 4.0",
        "description": (
            "Extracts of Wikipedia list-articles covering DEA Schedules I–V, "
            "UK controlled drugs, drug precursors, CWC schedules, designer drugs, "
            "and fentanyl analogues. Useful for cross-referencing synonyms and "
            "aliases not present in regulatory text."
        ),
    },
    "export_controls": {
        "title": "Export controls — Australia Group, OPCW CWC, BIS CCL",
        "authority": "Australia Group / OPCW / US BIS",
        "type": "regulatory (export control)",
        "jurisdiction": "US + international",
        "urls": [
            "https://www.dfat.gov.au/publications/minisite/theaustraliagroupnet/site/en/controllists.html",
            "https://www.opcw.org/chemical-weapons-convention/annexes/annex-chemicals",
            "https://www.bis.doc.gov/",
        ],
        "license": "Gov publications; attribution required.",
        "description": (
            "Chemical-weapons and dual-use chemical export-control lists. "
            "Australia Group: multilateral CW precursor control. OPCW CWC "
            "Schedules 1/2/3: treaty-level schedule of chemicals. BIS CCL: "
            "US Commerce Control List chemical ECCNs."
        ),
    },
    "eu_uk": {
        "title": "EU Reg 273/2004 & 111/2005 + UK MDA",
        "authority": "EU / UK.gov",
        "type": "regulatory",
        "jurisdiction": "EU + UK",
        "urls": [
            "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02004R0273-20220101",
            "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02005R0111-20220101",
            "https://www.legislation.gov.uk/",
        ],
        "license": "EUR-Lex © EU, reuse authorised with attribution; UK legislation Open Government Licence.",
        "description": (
            "EU drug precursor regulations (Categories 1/2A/2B/3/4) plus UK "
            "Misuse of Drugs Act 1971 / Regulations 2001 schedules."
        ),
    },
    "chemnet_com": {
        "title": "chemnet.com — B2B chemical directory",
        "authority": "chemnet.com",
        "type": "marketplace",
        "jurisdiction": "global (China-heavy)",
        "urls": ["http://www.chemnet.com/", "http://china.chemnet.com/"],
        "license": "Public listings; attribution to chemnet.com.",
        "description": (
            "Product and supplier listings from chemnet.com, queried against "
            "a seed list of precursor-adjacent CAS numbers."
        ),
    },
    "chem_b2b": {
        "title": "LookChem + ChemicalBook + Guidechem + Molbase",
        "authority": "Private B2B directories",
        "type": "marketplace",
        "jurisdiction": "global",
        "urls": [
            "https://www.lookchem.com/",
            "https://www.chemicalbook.com/",
            "https://www.guidechem.com/",
            "https://www.molbase.com/en/",
        ],
        "license": "Public listings.",
        "description": "B2B supplier and product listings, CAS-seeded.",
    },
    "chem_china": {
        "title": "Made-In-China + echemi + hxchem + wap.china.cn + ecasb",
        "authority": "Private B2B directories",
        "type": "marketplace",
        "jurisdiction": "China-focused",
        "urls": [
            "https://www.made-in-china.com/",
            "https://www.echemi.com/",
            "http://www.hxchem.net/",
            "https://wap.china.cn/",
            "https://www.ecasb.com/",
        ],
        "license": "Public listings.",
        "description": (
            "China-based B2B marketplaces; seed list emphasizes fentanyl "
            "precursors (4-ANPP, NPP, 1-Boc-4-piperidone, 4-piperidone HCl, "
            "aniline, propionyl chloride, piperidine)."
        ),
    },
    "chem_intl": {
        "title": "Alibaba + IndiaMart + supplierlist + lobasources",
        "authority": "Private B2B directories",
        "type": "marketplace",
        "jurisdiction": "global (India + global)",
        "urls": [
            "https://www.alibaba.com/",
            "https://www.indiamart.com/",
            "https://www.supplierlist.com/",
            "https://www.lobasources.com/",
        ],
        "license": "Public listings.",
        "description": (
            "International B2B sourcing platforms, CAS-seeded, covering both "
            "generic precursors and fentanyl precursors."
        ),
    },
}


def yaml_dump(d: dict) -> str:
    """Minimal YAML writer — no external dep. Handles our flat structure."""
    lines: list[str] = []
    for k, v in d.items():
        if isinstance(v, list):
            lines.append(f"{k}:")
            for x in v:
                lines.append(f"  - {json.dumps(x, ensure_ascii=False)[1:-1]}"
                             if isinstance(x, str) else f"  - {x}")
        elif isinstance(v, dict):
            lines.append(f"{k}:")
            for k2, v2 in v.items():
                lines.append(f"  {k2}: {json.dumps(v2, ensure_ascii=False) if isinstance(v2, str) else v2}")
        else:
            lines.append(f"{k}: {json.dumps(v, ensure_ascii=False) if isinstance(v, str) else v}")
    return "\n".join(lines) + "\n"


def table_row_count(con: sqlite3.Connection, table: str) -> int:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except sqlite3.Error:
        return 0


def table_columns(con: sqlite3.Connection, table: str) -> list[tuple[str, str]]:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        return [(r[1], r[2]) for r in rows]
    except sqlite3.Error:
        return []


def export_table_csv_jsonl(con: sqlite3.Connection, table: str, out_dir: Path, stem: str) -> tuple[Path, Path]:
    cur = con.execute(f"SELECT * FROM {table}")
    cols = [d[0] for d in cur.description]
    csv_path = out_dir / f"{stem}.csv"
    jsonl_path = out_dir / f"{stem}.jsonl"
    with csv_path.open("w", newline="", encoding="utf-8") as fcsv, \
         jsonl_path.open("w", encoding="utf-8") as fjsonl:
        w = csv.writer(fcsv)
        w.writerow(cols)
        for row in cur:
            w.writerow(row)
            fjsonl.write(json.dumps(dict(zip(cols, row)), ensure_ascii=False) + "\n")
    return csv_path, jsonl_path


def export_samples(con: sqlite3.Connection, table: str, out_dir: Path, stem: str, n: int = 10) -> Path:
    rows = con.execute(f"SELECT * FROM {table} LIMIT {n}").fetchall()
    cols = [d[0] for d in con.execute(f"SELECT * FROM {table} LIMIT 0").description]
    path = out_dir / f"{stem}_sample.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(dict(zip(cols, r)), ensure_ascii=False) + "\n")
    return path


def write_schema_md(con: sqlite3.Connection, table: str, out_path: Path, stem: str) -> None:
    cols = table_columns(con, table)
    lines = [f"# Schema: `{table}`", "", "| column | type |", "|---|---|"]
    for name, typ in cols:
        lines.append(f"| `{name}` | {typ or 'TEXT'} |")
    out_path.write_text("\n".join(lines) + "\n")


def build_source(source_tag: str) -> dict:
    src_info = SOURCE_CATALOG[source_tag]
    src_dir = SOURCES_DIR / source_tag
    data_dir = src_dir / "data"
    schema_dir = src_dir / "schema"
    sample_dir = src_dir / "samples"
    for d in (data_dir, schema_dir, sample_dir):
        d.mkdir(parents=True, exist_ok=True)

    db_path = DB_DIR / f"{source_tag}.sqlite3"
    if not db_path.exists():
        (src_dir / "README.md").write_text(
            f"# {src_info['title']}\n\n_Not yet scraped or scrape failed._ "
            f"See `../../logs/{source_tag}.log` for details.\n"
        )
        return {"tag": source_tag, "tables": {}, "status": "missing"}

    con = sqlite3.connect(db_path)
    tables = [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )]

    table_meta: dict[str, int] = {}
    for t in tables:
        rc = table_row_count(con, t)
        table_meta[t] = rc
        if rc == 0:
            continue
        export_table_csv_jsonl(con, t, data_dir, t)
        export_samples(con, t, sample_dir, t, n=10)
        write_schema_md(con, t, schema_dir / f"{t}.md", t)

    # metadata.yaml
    meta = {
        "tag": source_tag,
        "title": src_info["title"],
        "authority": src_info["authority"],
        "type": src_info["type"],
        "jurisdiction": src_info["jurisdiction"],
        "license": src_info["license"],
        "urls": src_info["urls"],
        "fetched_at": NOW,
        "table_row_counts": table_meta,
        "total_rows": sum(table_meta.values()),
    }
    (src_dir / "metadata.yaml").write_text(yaml_dump(meta))

    # README.md
    readme = textwrap.dedent(f"""\
        # {src_info['title']}

        **Authority:** {src_info['authority']}
        **Type:** {src_info['type']}
        **Jurisdiction:** {src_info['jurisdiction']}
        **Fetched:** {NOW}
        **Total rows:** {meta['total_rows']:,}

        {src_info['description']}

        ## Source URLs

        {chr(10).join(f'- {u}' for u in src_info['urls'])}

        ## Tables

        | table | rows | files |
        |---|---:|---|
        """)
    for t, rc in table_meta.items():
        readme += f"| `{t}` | {rc:,} | `data/{t}.csv`, `data/{t}.jsonl`, `samples/{t}_sample.jsonl`, `schema/{t}.md` |\n"

    readme += textwrap.dedent(f"""

        ## License

        {src_info['license']}

        ## Reproduce

        From repo root:

        ```bash
        cd data/chemnet
        python3 scrape_{source_tag}.py
        python3 organize_for_llm.py
        ```

        The scraper caches raw fetches under `../raw/{source_tag}/` so re-runs are cheap.
    """)
    (src_dir / "README.md").write_text(readme)

    con.close()
    return {"tag": source_tag, "tables": table_meta, "status": "ok"}


def build_manifest(results: list[dict]) -> None:
    manifest = {
        "generated_at": NOW,
        "total_sources": len(results),
        "total_rows": sum(sum(r["tables"].values()) for r in results),
        "sources": results,
    }
    (ROOT / "MANIFEST.json").write_text(json.dumps(manifest, indent=2))


def main() -> int:
    results = [build_source(tag) for tag in SOURCE_CATALOG.keys()]
    build_manifest(results)
    print("wrote:")
    print("  ", ROOT / "MANIFEST.json")
    for r in results:
        print(f"  sources/{r['tag']}/  [{r['status']}]  rows={sum(r['tables'].values()):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
