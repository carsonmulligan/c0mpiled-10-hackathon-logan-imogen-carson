#!/usr/bin/env python3
"""
PubChem scraper for precursor / controlled-substance compound data.

Pulls CIDs from the PubChem classification hierarchies (DEA, CWC/Chemical
Warfare Agents, DHS Chemicals of Interest), fetches core properties and
synonyms, then writes CSVs and a SQLite DB in the chemnet namespace.

Namespace: data/chemnet/
  raw/pubchem/<slug>/<id>.json    - cached raw responses
  csv/pubchem_*.csv                - output CSVs
  dbs/pubchem.sqlite3              - output SQLite
  logs/pubchem.log                 - progress log
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

BASE = Path(__file__).resolve().parent
RAW_DIR = BASE / "raw" / "pubchem"
CSV_DIR = BASE / "csv"
DB_PATH = BASE / "dbs" / "pubchem.sqlite3"
LOG_PATH = BASE / "logs" / "pubchem.log"

for d in (RAW_DIR, CSV_DIR, DB_PATH.parent, LOG_PATH.parent):
    d.mkdir(parents=True, exist_ok=True)
for sub in ("classification", "properties", "synonyms", "xrefs"):
    (RAW_DIR / sub).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("pubchem")

PUG = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "chemnet-hackathon/0.1 (profiles.co@gmail.com)"})
RATE_DELAY = 0.25  # 4 req/s — below PubChem's 5 rps cap

# ---------------------------------------------------------------------------
# Classification targets. (label, classification_system, category, HNID)
# ---------------------------------------------------------------------------
TARGETS: list[tuple[str, str, str, int]] = [
    ("DEA Schedule I",   "DEA Controlled Substances", "Schedule I",   4025055),
    ("DEA Schedule II",  "DEA Controlled Substances", "Schedule II",  4025056),
    ("DEA Schedule III", "DEA Controlled Substances", "Schedule III", 4025057),
    ("DEA Schedule IV",  "DEA Controlled Substances", "Schedule IV",  4025058),
    ("DEA Schedule V",   "DEA Controlled Substances", "Schedule V",   4025059),
    ("DEA List I",       "DEA Listed Chemicals",      "List I",       4025060),
    ("DEA List II",      "DEA Listed Chemicals",      "List II",      4025061),
    ("DEA Listed Chemicals (root)", "DEA Listed Chemicals", "Listed Chemicals", 4025047),
    ("DEA NFLIS Substances", "DEA NFLIS", "NFLIS Substances", 18246386),
    ("Chemical Warfare Agents (ChemIDplus)", "Chemical Warfare Agents", "CWA", 3092137),
    ("US DHS Chemicals of Interest", "US DHS COI", "Chemicals of Interest", 17060383),
]


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _sleep():
    time.sleep(RATE_DELAY)


def fetch_json(url: str, cache_path: Path | None = None, retries: int = 3) -> dict | None:
    if cache_path and cache_path.exists():
        try:
            return json.loads(cache_path.read_text())
        except Exception:
            pass

    backoff = 1.0
    for attempt in range(retries + 1):
        try:
            r = SESSION.get(url, timeout=60)
        except requests.RequestException as e:
            log.warning("network error %s (%s/%s): %s", url, attempt, retries, e)
            time.sleep(backoff)
            backoff *= 2
            continue

        if r.status_code == 200:
            _sleep()
            try:
                data = r.json()
            except Exception as e:
                log.warning("json decode failed %s: %s", url, e)
                return None
            if cache_path:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(json.dumps(data))
            return data

        if r.status_code in (429, 503) and attempt < retries:
            log.warning("%s on %s, backing off %.1fs", r.status_code, url, backoff)
            time.sleep(backoff)
            backoff *= 2
            continue

        log.warning("HTTP %s on %s", r.status_code, url)
        return None

    log.error("giving up on %s", url)
    return None


# ---------------------------------------------------------------------------
# Step 1: enumerate CIDs from each classification node
# ---------------------------------------------------------------------------
def fetch_classification_cids() -> dict[int, list[dict]]:
    """Returns: {cid: [ {source_tag, classification_system, category}, ... ] }"""
    cid_map: dict[int, list[dict]] = {}
    for label, system, category, hnid in TARGETS:
        url = f"{PUG}/classification/hnid/{hnid}/cids/JSON"
        cache = RAW_DIR / "classification" / f"{hnid}.json"
        data = fetch_json(url, cache)
        if not data:
            log.error("failed classification fetch for %s (hnid=%s)", label, hnid)
            continue
        cids = data.get("IdentifierList", {}).get("CID", []) or []
        log.info("%-45s -> %4d CIDs", label, len(cids))
        for cid in cids:
            cid_map.setdefault(int(cid), []).append({
                "source_tag": label,
                "classification_system": system,
                "category": category,
                "hnid": hnid,
            })
    return cid_map


# ---------------------------------------------------------------------------
# Step 2: fetch properties for batches of CIDs
# ---------------------------------------------------------------------------
PROP_FIELDS = [
    "MolecularFormula", "MolecularWeight",
    "IUPACName", "CanonicalSMILES", "InChI", "InChIKey",
]


def fetch_properties(cids: list[int], batch: int = 150) -> dict[int, dict]:
    out: dict[int, dict] = {}
    prop_path = ",".join(PROP_FIELDS)
    for i in range(0, len(cids), batch):
        chunk = cids[i : i + batch]
        cid_str = ",".join(str(c) for c in chunk)
        url = f"{PUG}/compound/cid/{cid_str}/property/{prop_path}/JSON"
        cache = RAW_DIR / "properties" / f"batch_{i:05d}.json"
        data = fetch_json(url, cache)
        if not data:
            log.warning("property batch %d-%d failed", i, i + batch)
            continue
        for row in data.get("PropertyTable", {}).get("Properties", []) or []:
            cid = int(row.get("CID"))
            out[cid] = row
        log.info("properties: %d/%d cids fetched", len(out), len(cids))
    return out


# ---------------------------------------------------------------------------
# Step 3: CAS via xrefs/RegistryID (filter to CAS-format strings)
# ---------------------------------------------------------------------------
import re
CAS_RE = re.compile(r"^\d{1,7}-\d{2}-\d$")


def looks_like_cas(s: str) -> bool:
    return bool(CAS_RE.match(s.strip()))


def fetch_cas(cids: list[int], batch: int = 100) -> dict[int, str]:
    out: dict[int, str] = {}
    for i in range(0, len(cids), batch):
        chunk = cids[i : i + batch]
        cid_str = ",".join(str(c) for c in chunk)
        url = f"{PUG}/compound/cid/{cid_str}/xrefs/RegistryID/JSON"
        cache = RAW_DIR / "xrefs" / f"batch_{i:05d}.json"
        data = fetch_json(url, cache)
        if not data:
            continue
        for info in data.get("InformationList", {}).get("Information", []) or []:
            cid = int(info.get("CID"))
            regs = info.get("RegistryID") or []
            cas_hits = [r for r in regs if looks_like_cas(str(r))]
            if cas_hits:
                # Smallest CAS number = usually the primary / parent compound
                cas_hits.sort(key=lambda r: int(str(r).split("-")[0]))
                out[cid] = str(cas_hits[0])
        log.info("CAS: %d/%d cids resolved", len(out), len(cids))
    return out


# ---------------------------------------------------------------------------
# Step 4: synonyms (individual calls — cheap, cached)
# ---------------------------------------------------------------------------
def fetch_synonyms(cids: list[int], max_per_cid: int = 25) -> dict[int, list[str]]:
    out: dict[int, list[str]] = {}
    # Batch endpoint also works
    batch = 100
    for i in range(0, len(cids), batch):
        chunk = cids[i : i + batch]
        cid_str = ",".join(str(c) for c in chunk)
        url = f"{PUG}/compound/cid/{cid_str}/synonyms/JSON"
        cache = RAW_DIR / "synonyms" / f"batch_{i:05d}.json"
        data = fetch_json(url, cache)
        if not data:
            continue
        for info in data.get("InformationList", {}).get("Information", []) or []:
            cid = int(info.get("CID"))
            syns = info.get("Synonym") or []
            out[cid] = syns[:max_per_cid]
        log.info("synonyms: %d/%d cids", len(out), len(cids))
    return out


# ---------------------------------------------------------------------------
# Step 5: write CSVs + SQLite
# ---------------------------------------------------------------------------
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_outputs(cid_map, props, cas, syns):
    fetched_at = utc_now()

    # Build compound rows
    compound_rows = []
    for cid in sorted(cid_map.keys()):
        p = props.get(cid, {})
        tags = cid_map[cid]
        primary_tag = tags[0]["source_tag"] if tags else None
        compound_rows.append({
            "cid": cid,
            "iupac_name": p.get("IUPACName"),
            "molecular_formula": p.get("MolecularFormula"),
            "molecular_weight": p.get("MolecularWeight"),
            "canonical_smiles": p.get("CanonicalSMILES"),
            "inchi": p.get("InChI"),
            "inchi_key": p.get("InChIKey"),
            "cas_primary": cas.get(cid),
            "source_tag": primary_tag,
            "fetched_at": fetched_at,
        })

    # Classification rows (dedup by cid + source_tag)
    classification_rows = []
    seen = set()
    for cid, tags in cid_map.items():
        for t in tags:
            key = (cid, t["source_tag"])
            if key in seen:
                continue
            seen.add(key)
            classification_rows.append({
                "cid": cid,
                "classification_system": t["classification_system"],
                "category": t["category"],
                "subcategory": None,
                "raw_path": f"hnid:{t['hnid']}",
                "source_tag": t["source_tag"],
                "fetched_at": fetched_at,
            })

    # Synonym rows (one per row)
    synonym_rows = []
    for cid, names in syns.items():
        for n in names:
            synonym_rows.append({
                "cid": cid,
                "synonym": n,
                "fetched_at": fetched_at,
            })

    # --- CSVs ------------------------------------------------------------
    def write_csv(path: Path, rows, fields):
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in rows:
                w.writerow(r)

    write_csv(
        CSV_DIR / "pubchem_compounds.csv",
        compound_rows,
        ["cid", "iupac_name", "molecular_formula", "molecular_weight",
         "canonical_smiles", "inchi", "inchi_key", "cas_primary",
         "source_tag", "fetched_at"],
    )
    write_csv(
        CSV_DIR / "pubchem_classifications.csv",
        classification_rows,
        ["cid", "classification_system", "category", "subcategory",
         "raw_path", "source_tag", "fetched_at"],
    )
    write_csv(
        CSV_DIR / "pubchem_synonyms.csv",
        synonym_rows,
        ["cid", "synonym", "fetched_at"],
    )

    # --- SQLite ----------------------------------------------------------
    if DB_PATH.exists():
        DB_PATH.unlink()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE pubchem_compounds (
            cid INTEGER PRIMARY KEY,
            iupac_name TEXT,
            molecular_formula TEXT,
            molecular_weight REAL,
            canonical_smiles TEXT,
            inchi TEXT,
            inchi_key TEXT,
            cas_primary TEXT,
            source_tag TEXT,
            fetched_at TEXT
        );
        CREATE TABLE pubchem_classifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cid INTEGER NOT NULL,
            classification_system TEXT,
            category TEXT,
            subcategory TEXT,
            raw_path TEXT,
            source_tag TEXT,
            fetched_at TEXT,
            UNIQUE(cid, source_tag)
        );
        CREATE TABLE pubchem_synonyms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cid INTEGER NOT NULL,
            synonym TEXT,
            fetched_at TEXT
        );
        CREATE INDEX idx_class_cid ON pubchem_classifications(cid);
        CREATE INDEX idx_class_system ON pubchem_classifications(classification_system);
        CREATE INDEX idx_syn_cid ON pubchem_synonyms(cid);
        CREATE INDEX idx_cmp_cas ON pubchem_compounds(cas_primary);
    """)
    cur.executemany(
        "INSERT INTO pubchem_compounds VALUES "
        "(:cid,:iupac_name,:molecular_formula,:molecular_weight,"
        ":canonical_smiles,:inchi,:inchi_key,:cas_primary,"
        ":source_tag,:fetched_at)",
        compound_rows,
    )
    cur.executemany(
        "INSERT OR IGNORE INTO pubchem_classifications "
        "(cid,classification_system,category,subcategory,raw_path,source_tag,fetched_at) "
        "VALUES (:cid,:classification_system,:category,:subcategory,"
        ":raw_path,:source_tag,:fetched_at)",
        classification_rows,
    )
    cur.executemany(
        "INSERT INTO pubchem_synonyms (cid,synonym,fetched_at) "
        "VALUES (:cid,:synonym,:fetched_at)",
        synonym_rows,
    )
    con.commit()
    con.close()

    return {
        "compounds": len(compound_rows),
        "classifications": len(classification_rows),
        "synonyms": len(synonym_rows),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log.info("=== PubChem scrape start ===")
    cid_map = fetch_classification_cids()
    all_cids = sorted(cid_map.keys())
    log.info("total unique CIDs: %d", len(all_cids))

    # Safety: cap at 3000 CIDs (NFLIS adds ~2000 but they're all drug CIDs,
    # keep them — they're small and the property endpoint is efficient).
    props = fetch_properties(all_cids)
    cas = fetch_cas(all_cids)
    syns = fetch_synonyms(all_cids, max_per_cid=15)

    stats = write_outputs(cid_map, props, cas, syns)
    log.info("wrote: %s", stats)

    # Per-system counts
    from collections import Counter
    sys_counts = Counter()
    per_cid_systems: dict[int, set] = {}
    for cid, tags in cid_map.items():
        for t in tags:
            sys_counts[t["source_tag"]] += 1
            per_cid_systems.setdefault(cid, set()).add(t["classification_system"])
    log.info("per-source-tag CID counts:")
    for tag, n in sorted(sys_counts.items(), key=lambda kv: -kv[1]):
        log.info("  %-45s %d", tag, n)

    # Samples
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(
        "SELECT cid, iupac_name, cas_primary, molecular_formula "
        "FROM pubchem_compounds WHERE iupac_name IS NOT NULL "
        "AND cas_primary IS NOT NULL LIMIT 5"
    ).fetchall()
    log.info("sample rows:")
    for r in rows:
        log.info("  cid=%s cas=%s formula=%s name=%s",
                 r["cid"], r["cas_primary"], r["molecular_formula"],
                 (r["iupac_name"] or "")[:80])
    con.close()

    log.info("=== PubChem scrape done ===")


if __name__ == "__main__":
    main()
