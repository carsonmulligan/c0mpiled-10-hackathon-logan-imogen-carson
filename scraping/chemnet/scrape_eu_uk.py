#!/usr/bin/env python3
"""
Scraper for EU drug precursor regulations and UK controlled drugs list.

Sources:
  - EU Reg 273/2004 (intra-EU precursors)   via https://eur-lex.europa.eu/eli/reg/2004/273/2022-01-01
  - EU Reg 111/2005 (extra-EU precursors)   via https://eur-lex.europa.eu/eli/reg/2005/111/2022-01-01
  - UK Misuse of Drugs Regs 2001 schedules  via https://www.legislation.gov.uk/uksi/2001/3998/schedules/made/data.html
  - gov.uk Controlled Drugs List            via /government/publications/controlled-drugs-list--2/list-of-most-commonly-encountered-drugs-currently-controlled-under-the-misuse-of-drugs-legislation
  - EMCDDA NPS overview (bonus)             via https://www.emcdda.europa.eu/publications/topic-overviews/eu-early-warning-system_en

Outputs:
  - raw HTML in  raw/eu_uk/
  - CSVs in      csv/eu_uk_*.csv
  - SQLite in    dbs/eu_uk.sqlite3
  - log in       logs/eu_uk.log
"""
from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ------------------------------------------------------------- paths & config
ROOT    = Path(__file__).resolve().parent
RAW_DIR = ROOT / "raw" / "eu_uk"
CSV_DIR = ROOT / "csv"
DB_DIR  = ROOT / "dbs"
LOG_DIR = ROOT / "logs"
for d in (RAW_DIR, CSV_DIR, DB_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

DB_PATH  = DB_DIR / "eu_uk.sqlite3"
LOG_PATH = LOG_DIR / "eu_uk.log"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
})
RATE_SECS = 1.0

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("eu_uk")
log.addHandler(logging.StreamHandler(sys.stdout))

CAS_RE = re.compile(r"\b\d{2,7}-\d{2}-\d\b")
CN_RE  = re.compile(r"\b\d{4}\s?\d{2}(?:\s?\d{2})?\b")


# ------------------------------------------------------------------ helpers
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def fetch(url: str, cache_name: str) -> str:
    path = RAW_DIR / cache_name
    if path.exists() and path.stat().st_size > 2048:
        log.info("cache hit %s (%d bytes)", cache_name, path.stat().st_size)
        return path.read_text(encoding="utf-8", errors="replace")
    log.info("fetch %s", url)
    time.sleep(RATE_SECS)
    r = SESSION.get(url, timeout=60, allow_redirects=True)
    log.info("  -> HTTP %s (%d bytes)", r.status_code, len(r.content))
    r.raise_for_status()
    path.write_text(r.text, encoding="utf-8")
    return r.text


def digest(*parts: str) -> str:
    h = hashlib.sha1("|".join(p or "" for p in parts).encode("utf-8")).hexdigest()
    return h[:16]


def norm_cas(s: str) -> str:
    m = CAS_RE.search(s or "")
    return m.group(0) if m else ""


def norm_cn(s: str) -> str:
    if not s:
        return ""
    m = CN_RE.search(s)
    return m.group(0).strip() if m else ""


def clean(s: Optional[str]) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", s)


# ------------------------------------------------------------ EU precursors
EU_SOURCES = [
    # (regulation_tag, url, cache filename)
    ("273/2004", "https://eur-lex.europa.eu/eli/reg/2004/273/2022-01-01", "eur-lex-273-2004.html"),
    ("111/2005", "https://eur-lex.europa.eu/eli/reg/2005/111/2022-01-01", "eur-lex-111-2005.html"),
]


def parse_eu_annex(regulation: str, html: str, source_url: str) -> list[dict]:
    """Walk pandas.read_html tables until we have found substance tables."""
    rows: list[dict] = []
    tables = pd.read_html(html)

    # Category is assigned by scanning the surrounding raw text for "CATEGORY <x>"
    # or "SUBCATEGORY 2A" etc. We split the full text on those headings.
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=False)

    # Build list of (category, start_idx) in source order.
    cat_pat = re.compile(r"(SUBCATEGORY\s*2A|SUBCATEGORY\s*2B|CATEGORY\s*1\b|CATEGORY\s*2\b|CATEGORY\s*3\b|CATEGORY\s*4\b)",
                         re.IGNORECASE)
    markers = [(m.group(1).upper().replace(" ", ""), m.start()) for m in cat_pat.finditer(text)]
    # Dedupe adjacent duplicates (heading appears in TOC and body)
    # but keep all — position-based lookup picks the latest ≤ pos.

    def cat_for_position(pos: int) -> str:
        cur = ""
        for label, idx in markers:
            if idx <= pos:
                # Normalise label
                if "2A" in label:
                    cur = "2A"
                elif "2B" in label:
                    cur = "2B"
                elif "CATEGORY1" in label:
                    cur = "1"
                elif "CATEGORY2" in label:
                    cur = "2"
                elif "CATEGORY3" in label:
                    cur = "3"
                elif "CATEGORY4" in label:
                    cur = "4"
            else:
                break
        return cur

    # Map each table to the text position of its first cell so we know its category
    # We locate each table in source by scanning for the first substance's name.
    for ti, t in enumerate(tables):
        # skip tables that don't look like substance lists (expect >=3 cols, first col header "Substance")
        if t.shape[1] < 3:
            continue
        header_row = t.iloc[0]
        if not any("Substance" in clean(str(c)) for c in header_row):
            continue

        # Find position of table in text: use first substance row's first cell
        # (row 1 or later; some tables have "▼M5" marker rows we skip)
        pos = -1
        first_subst = ""
        for j in range(1, len(t)):
            cell = clean(str(t.iloc[j, 0]))
            if cell and not cell.startswith("▼") and not cell.startswith("►") and cell.lower() != "nan":
                first_subst = cell
                break
        if first_subst:
            pos = text.find(first_subst)
        category = cat_for_position(pos) if pos >= 0 else ""

        # Determine column indexes by header
        hdr = [clean(str(c)) for c in header_row]
        def find_col(keys):
            for i, h in enumerate(hdr):
                hl = h.lower()
                if any(k in hl for k in keys):
                    return i
            return -1

        col_name = 0
        col_cn_designation = find_col(["cn designation"])
        col_cn_code        = find_col(["cn code"])
        col_cas            = find_col(["cas"])

        # Need at least a CN code column; CAS is optional (Category 4 has no CAS).
        if col_cn_code < 0:
            continue

        for j in range(1, len(t)):
            raw_row = [clean(str(t.iloc[j, k])) for k in range(t.shape[1])]
            name = raw_row[col_name]
            if not name or name.startswith("▼") or name.startswith("►") or name.lower() == "nan":
                continue
            # Skip footnote / narrative rows
            if name.lower().startswith(("the stereoisomeric", "the salts", "(1)", "(2)", "(3)")):
                continue
            cas = norm_cas(raw_row[col_cas]) if col_cas >= 0 else ""
            cn  = norm_cn(raw_row[col_cn_code])
            notes = raw_row[col_cn_designation] if col_cn_designation >= 0 else ""
            if notes.lower() == "nan":
                notes = ""
            # Skip truly empty rows; allow Category 4 rows that have CN but no CAS.
            if not cas and not cn:
                continue
            row = {
                "id": digest(regulation, name, cas),
                "regulation": regulation,
                "category": category or "",
                "chemical_name": name,
                "cas_number": cas,
                "cn_code": cn,
                "notes": notes,
                "source_url": source_url,
                "raw_text": " | ".join(raw_row),
                "fetched_at": now_iso(),
            }
            rows.append(row)
    return rows


def scrape_eu() -> list[dict]:
    all_rows: list[dict] = []
    for reg, url, cache in EU_SOURCES:
        try:
            html = fetch(url, cache)
            rows = parse_eu_annex(reg, html, url)
            log.info("eu %s parsed rows: %d", reg, len(rows))
            all_rows.extend(rows)
        except Exception as e:
            log.error("eu %s failed: %s", reg, e)
    # dedupe on (regulation, chemical_name, cas_number)
    seen = set()
    deduped = []
    for r in all_rows:
        key = (r["regulation"], r["chemical_name"].lower(), r["cas_number"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped


# ------------------------------------------------------- UK controlled drugs
UK_GOVUK_URL = ("https://www.gov.uk/government/publications/controlled-drugs-list--2/"
                "list-of-most-commonly-encountered-drugs-currently-controlled-under-the-"
                "misuse-of-drugs-legislation")
UK_LEGISLATION_URL = "https://www.legislation.gov.uk/uksi/2001/3998/schedules/made/data.html"


def parse_uk_govuk(html: str) -> list[dict]:
    rows: list[dict] = []
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("div", class_="govspeak") or soup.find("main")
    if not main:
        return rows
    # The govspeak contains a table with headers Drug | Class (MDA) | Schedule (MDR)
    tables = main.find_all("table")
    for t in tables:
        # Use pandas to parse each individual table
        try:
            dfs = pd.read_html(str(t))
        except ValueError:
            continue
        for df in dfs:
            # Expect at least columns: Drug, Class/MDA, Schedule/MDR
            if df.shape[1] < 2:
                continue
            # Normalise header: gov.uk uses a MultiIndex (Drug / Class / Schedule → MDA / MDR).
            if isinstance(df.columns, pd.MultiIndex):
                cols = [" ".join(clean(str(c)) for c in tup if clean(str(c))).strip() for tup in df.columns]
                df.columns = cols
            cols_lower = [str(c).lower() for c in df.columns]

            # Identify columns
            def col_ix(keys):
                for i, c in enumerate(cols_lower):
                    if any(k in c for k in keys):
                        return i
                return -1

            i_name     = col_ix(["drug"])
            i_class    = col_ix(["class", "mda"])
            i_schedule = col_ix(["schedule", "mdr"])
            if i_name < 0 or i_schedule < 0:
                continue
            for _, r in df.iterrows():
                name = clean(str(r.iloc[i_name]))
                if not name or name.lower() == "drug" or name.lower() == "nan":
                    continue
                klass = clean(str(r.iloc[i_class])) if i_class >= 0 else ""
                schedule = clean(str(r.iloc[i_schedule]))
                # Strip footnote markers like [footnote 1]
                klass = re.sub(r"\[footnote[^\]]*\]", "", klass).strip()
                schedule = re.sub(r"\[footnote[^\]]*\]", "", schedule).strip()
                if klass.lower() == "nan":
                    klass = ""
                if schedule.lower() in {"nan", "n/a", ""}:
                    # still keep entry, but leave schedule blank
                    schedule = ""
                # Some rows have compound name like "Adinazolam" or include aliases in parens
                aliases = ""
                mpar = re.search(r"\(([^)]+)\)", name)
                if mpar:
                    aliases = mpar.group(1)
                rows.append({
                    "id": digest("govuk", name, schedule, klass),
                    "schedule": schedule,
                    "drug_name": name,
                    "cas_number": "",  # gov.uk list doesn't expose CAS
                    "class": klass,
                    "aliases": aliases,
                    "source_url": UK_GOVUK_URL,
                    "raw_text": " | ".join(clean(str(x)) for x in r.tolist()),
                    "fetched_at": now_iso(),
                })
    return rows


def parse_uk_legislation(html: str) -> list[dict]:
    """Parse Schedules 1-5 of SI 2001/3998 as narrative-style lists.

    This is a supplement to the gov.uk table; used for drugs not already captured
    (e.g. to add rows with source_url pointing to the statute itself).
    """
    rows: list[dict] = []
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=False)
    # Split by SCHEDULE N heading
    parts = re.split(r"(SCHEDULE\s+(?:1|2|3|4|5)\b)", text)
    schedule = None
    for chunk in parts:
        if chunk.startswith("SCHEDULE"):
            m = re.match(r"SCHEDULE\s+(\d)", chunk)
            schedule = m.group(1) if m else None
            continue
        if not schedule:
            continue
        # substances appear as their own line fragments separated by newlines / punctuation
        # Split on newlines and commas; keep tokens that look like drug names
        tokens = re.split(r"[\n;]+", chunk)
        for tok in tokens:
            tok = clean(tok)
            # Heuristic: proper drug names start with an uppercase letter or digit, are >2 chars,
            # have fewer than 15 words, and aren't regulatory prose.
            if not tok or len(tok) < 3 or len(tok.split()) > 14:
                continue
            if tok.lower().startswith((
                "any compound",
                "any stereoisomeric",
                "any salt",
                "any ester",
                "any preparation",
                "the following",
                "any other",
                "an optical",
                "and includes",
                "where ",
                "for the",
                "regulation ",
                "any ",
                "also named",
                "this schedule",
            )):
                continue
            # Drop sub-paragraph markers "(a)", "(i)", etc.
            if re.fullmatch(r"\([a-z]+\)|\([ivxlcdm]+\)", tok):
                continue
            # Drop pure punctuation / numbers
            if not re.search(r"[A-Za-z]{3,}", tok):
                continue
            # Trim trailing periods
            name = tok.rstrip(".,")
            rows.append({
                "id": digest("uk-legis", name, schedule),
                "schedule": schedule,
                "drug_name": name,
                "cas_number": "",
                "class": "",
                "aliases": "",
                "source_url": UK_LEGISLATION_URL,
                "raw_text": tok,
                "fetched_at": now_iso(),
            })
    return rows


def scrape_uk() -> list[dict]:
    all_rows: list[dict] = []
    try:
        gov_html = fetch(UK_GOVUK_URL, "gov-uk-controlled-drugs-list.html")
        gov_rows = parse_uk_govuk(gov_html)
        log.info("uk gov.uk parsed rows: %d", len(gov_rows))
        all_rows.extend(gov_rows)
    except Exception as e:
        log.error("uk gov.uk failed: %s", e)

    try:
        leg_html = fetch(UK_LEGISLATION_URL, "uk-si-2001-3998-schedules.html")
        leg_rows = parse_uk_legislation(leg_html)
        log.info("uk legislation parsed rows: %d (pre-dedupe)", len(leg_rows))
        # Only keep legislation rows that add something new (schedule-specific drug name
        # not already present). Use (name_lower) as dedupe key.
        seen_names = {r["drug_name"].lower() for r in all_rows}
        new_leg = [r for r in leg_rows if r["drug_name"].lower() not in seen_names]
        log.info("uk legislation adds %d new rows", len(new_leg))
        all_rows.extend(new_leg)
    except Exception as e:
        log.error("uk legislation failed: %s", e)

    # Dedupe (drug_name, schedule, class)
    seen = set()
    deduped = []
    for r in all_rows:
        key = (r["drug_name"].lower(), r["schedule"], r["class"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped


# ------------------------------------------------------------ EMCDDA (bonus)
EMCDDA_URL = "https://www.emcdda.europa.eu/publications/topic-overviews/eu-early-warning-system_en"


def scrape_emcdda() -> list[dict]:
    """Best-effort scrape of EMCDDA EWS overview page for NPS counts / categories.

    The consolidated substance list is not directly exposed as a table on this page;
    we extract any substance names mentioned in prose so downstream cross-references can
    find them. Kept small — this is a bonus source.
    """
    rows: list[dict] = []
    try:
        html = fetch(EMCDDA_URL, "emcdda-ews-overview.html")
    except Exception as e:
        log.error("emcdda fetch failed: %s", e)
        return rows
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup
    text = main.get_text("\n", strip=True)
    # Grab capitalised drug-like tokens and any CAS numbers.
    substance_pat = re.compile(r"\b[A-Z][A-Za-z0-9\-]{3,}(?:\s[A-Z][A-Za-z0-9\-]+){0,3}\b")
    year_pat = re.compile(r"\b(19|20)\d{2}\b")
    candidates: dict[str, dict] = {}
    # Harvest (substance, year) near each other -- rough heuristic.
    for para in main.find_all(["p", "li"]):
        p_text = para.get_text(" ", strip=True)
        names = substance_pat.findall(p_text)
        years = year_pat.findall(p_text)
        BLOCKLIST = {
            "european", "union", "council", "regulation", "directive", "commission",
            "member", "states", "report", "drugs", "monitoring", "addictions",
            "substance", "substances", "system", "warning", "annual", "europol",
            "early", "europe", "euda", "emcdda", "national", "agency", "brussels",
            "parliament", "overview", "information", "data", "topic", "publication",
            "reitox", "focal", "point", "points", "operational", "guidelines",
        }
        for n in names:
            low = n.lower()
            words = low.split()
            if len(n) < 6 or low in BLOCKLIST:
                continue
            # require it look like a chemical / drug (contain a digit, hyphen, or a known suffix)
            if not (re.search(r"\d", n) or "-" in n or
                    any(low.endswith(suf) for suf in (
                        "ine", "ole", "one", "ate", "ene", "ole", "ide",
                        "amine", "cyclidine", "fentanyl", "oxetine", "benz",
                        "oid", "cannabinol", "tryptamine"))):
                continue
            # reject any word in the name being in blocklist
            if any(w in BLOCKLIST for w in words):
                continue
            candidates.setdefault(n, {"substance_name": n, "year": years[0] if years else ""})

    for n, data in candidates.items():
        rows.append({
            "id": digest("emcdda", n),
            "substance_name": n,
            "category": "NPS",
            "cas_number": "",
            "first_reported_year": data.get("year", ""),
            "source_url": EMCDDA_URL,
            "raw_text": n,
            "fetched_at": now_iso(),
        })
    log.info("emcdda rows: %d", len(rows))
    return rows


# ------------------------------------------------------------------- sqlite
DDL = {
    "eu_precursors": """
        CREATE TABLE IF NOT EXISTS eu_precursors (
            id TEXT PRIMARY KEY,
            regulation TEXT,
            category TEXT,
            chemical_name TEXT,
            cas_number TEXT,
            cn_code TEXT,
            notes TEXT,
            source_url TEXT,
            raw_text TEXT,
            fetched_at TEXT
        );
    """,
    "uk_controlled_drugs": """
        CREATE TABLE IF NOT EXISTS uk_controlled_drugs (
            id TEXT PRIMARY KEY,
            schedule TEXT,
            drug_name TEXT,
            cas_number TEXT,
            class TEXT,
            aliases TEXT,
            source_url TEXT,
            raw_text TEXT,
            fetched_at TEXT
        );
    """,
    "emcdda_nps": """
        CREATE TABLE IF NOT EXISTS emcdda_nps (
            id TEXT PRIMARY KEY,
            substance_name TEXT,
            category TEXT,
            cas_number TEXT,
            first_reported_year TEXT,
            source_url TEXT,
            raw_text TEXT,
            fetched_at TEXT
        );
    """,
}


def write_outputs(eu_rows, uk_rows, nps_rows) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        for ddl in DDL.values():
            conn.execute(ddl)
        conn.execute("DELETE FROM eu_precursors")
        conn.execute("DELETE FROM uk_controlled_drugs")
        conn.execute("DELETE FROM emcdda_nps")

        if eu_rows:
            cols = ["id", "regulation", "category", "chemical_name", "cas_number",
                    "cn_code", "notes", "source_url", "raw_text", "fetched_at"]
            conn.executemany(
                f"INSERT INTO eu_precursors ({','.join(cols)}) VALUES ({','.join('?'*len(cols))})",
                [tuple(r.get(c, "") for c in cols) for r in eu_rows],
            )
            pd.DataFrame(eu_rows).to_csv(CSV_DIR / "eu_uk_eu_precursors.csv", index=False)

        if uk_rows:
            cols = ["id", "schedule", "drug_name", "cas_number", "class", "aliases",
                    "source_url", "raw_text", "fetched_at"]
            conn.executemany(
                f"INSERT INTO uk_controlled_drugs ({','.join(cols)}) VALUES ({','.join('?'*len(cols))})",
                [tuple(r.get(c, "") for c in cols) for r in uk_rows],
            )
            pd.DataFrame(uk_rows).to_csv(CSV_DIR / "eu_uk_uk_controlled_drugs.csv", index=False)

        if nps_rows:
            cols = ["id", "substance_name", "category", "cas_number",
                    "first_reported_year", "source_url", "raw_text", "fetched_at"]
            conn.executemany(
                f"INSERT INTO emcdda_nps ({','.join(cols)}) VALUES ({','.join('?'*len(cols))})",
                [tuple(r.get(c, "") for c in cols) for r in nps_rows],
            )
            pd.DataFrame(nps_rows).to_csv(CSV_DIR / "eu_uk_emcdda_nps.csv", index=False)

        conn.commit()
    finally:
        conn.close()


# -------------------------------------------------------------------- entry
def main() -> None:
    log.info("=== start run %s ===", now_iso())
    eu_rows  = scrape_eu()
    uk_rows  = scrape_uk()
    nps_rows = scrape_emcdda()

    write_outputs(eu_rows, uk_rows, nps_rows)

    log.info("done: eu_precursors=%d uk_controlled_drugs=%d emcdda_nps=%d",
             len(eu_rows), len(uk_rows), len(nps_rows))
    print(f"eu_precursors={len(eu_rows)} uk_controlled_drugs={len(uk_rows)} emcdda_nps={len(nps_rows)}")


if __name__ == "__main__":
    main()
