#!/usr/bin/env python3
"""Scrape INCB Red, Yellow, and Green lists and persist to CSV + SQLite.

Sources:
- Red List (precursors, 23rd ed, July 2025)     : https://www.incb.org/documents/PRECURSORS/RED_LIST/RED_LIST_E.pdf
- Yellow List (narcotic drugs, 64th ed, 2025)   : https://www.incb.org/incb/uploads/documents/Narcotic-Drugs/Yellow_List/64th_edition/YL_64th_E.pdf
- Green List (psychotropics, 36th ed, 2025)     : https://www.incb.org/incb/uploads/documents/Psychotropics/forms/greenlist/2026/2510307E.pdf

Output: raw/incb/*.pdf, csv/incb_*.csv, dbs/incb.sqlite3, logs/incb.log
"""
from __future__ import annotations

import csv
import logging
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

# silence pdfplumber cropbox spam
import warnings
warnings.filterwarnings("ignore")
logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.getLogger("pdfplumber").setLevel(logging.ERROR)

import pdfplumber  # noqa: E402


BASE = Path(__file__).resolve().parent
RAW_DIR = BASE / "raw" / "incb"
CSV_DIR = BASE / "csv"
DB_PATH = BASE / "dbs" / "incb.sqlite3"
LOG_PATH = BASE / "logs" / "incb.log"

for d in (RAW_DIR, CSV_DIR, DB_PATH.parent, LOG_PATH.parent):
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, mode="w"), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("incb")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) holocron-gov-hackathon/0.1"
HEADERS = {"User-Agent": UA}

SOURCES = {
    "red": {
        "url": "https://www.incb.org/documents/PRECURSORS/RED_LIST/RED_LIST_E.pdf",
        "landing": "https://www.incb.org/incb/en/precursors/Red_Forms/red-list.html",
        "file": RAW_DIR / "red_list_E.pdf",
    },
    "yellow": {
        "url": "https://www.incb.org/incb/uploads/documents/Narcotic-Drugs/Yellow_List/64th_edition/YL_64th_E.pdf",
        "landing": "https://www.incb.org/incb/en/narcotic-drugs/Yellowlist/yellow-list.html",
        "file": RAW_DIR / "yellow_list_64th_E.pdf",
    },
    "green": {
        "url": "https://www.incb.org/incb/uploads/documents/Psychotropics/forms/greenlist/2026/2510307E.pdf",
        "landing": "https://www.incb.org/incb/en/psychotropics/green-list.html",
        "file": RAW_DIR / "green_list_E.pdf",
    },
}

CAS_RE = re.compile(r"\b(\d{2,7}-\d{2}-\d)\b")
FETCHED_AT = datetime.now(timezone.utc).isoformat()


def _last_top_level_paren(s: str) -> int:
    """Return index of the '(' that opens the final top-level parenthetical
    on the line (i.e. the IUPAC/chem-abstracts name). -1 if none."""
    depth = 0
    last_open = -1
    # iterate forward; track depth
    starts: list[int] = []
    for i, ch in enumerate(s):
        if ch == "(":
            if depth == 0:
                starts.append(i)
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
    if not starts:
        return -1
    return starts[-1]


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 1000:
        log.info("cached %s (%d bytes)", dest.name, dest.stat().st_size)
        return
    log.info("GET %s", url)
    r = requests.get(url, headers=HEADERS, timeout=120)
    r.raise_for_status()
    dest.write_bytes(r.content)
    log.info("wrote %s (%d bytes)", dest, len(r.content))
    time.sleep(1.0)


# ---------------------------------------------------------------------------
# Red List parsing
# ---------------------------------------------------------------------------

def extract_pdf_text(path: Path) -> list[str]:
    pages = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or "")
    return pages


@dataclass
class RedRow:
    table: str
    chemical_name: str
    cas_number: str
    other_names: str
    source_url: str
    source_doc: str
    raw_text: str


def parse_red_list(path: Path, source_url: str) -> list[RedRow]:
    """Red List entries follow this layout (English version):

        English chemical name (english iupac/chem-abstracts name)
        Nom français                  HS code: xxx.xx   CAS number: nnn-nn-n
        Nombre español
        [optional continuation lines for wrapped IUPAC or Spanish name]

    Strategy: walk lines; when we encounter an HS/CAS anchor line, look
    backwards for the most recent English-name line (starts with an uppercase
    letter and contains an opening parenthesis on the same line OR is
    followed by a continuation line that is the open-paren content).
    """
    pages = extract_pdf_text(path)
    full = "\n".join(pages)
    rows: list[RedRow] = []

    part_one_start = full.find("SUBSTANCES INCLUDED IN TABLE I")
    part_two_start = full.find("PART TWO")
    if part_one_start < 0:
        log.error("Red List: PART ONE marker not found")
        return rows
    part_one = full[part_one_start:part_two_start if part_two_start > 0 else len(full)]

    table2_marker = "SUBSTANCES INCLUDED IN TABLE II"
    t2_idx = part_one.find(table2_marker)
    t1_text = part_one[:t2_idx] if t2_idx > 0 else part_one
    t2_text = part_one[t2_idx:] if t2_idx > 0 else ""

    french_starts = (
        "Acide ", "Anhydride ", "Ephédrine ", "Éphédrine ", "Ergométrine",
        "Éphédra", "Ephédra", "Acétone", "Acétique", "Phényl", "Phénylacétique",
        "Pipéronal", "Pipéridone", "tert-butyl", "α-P", "alpha-phén",
        "Ester ", "Noréphédrine", "Pseudoéphédrine", "Permanganate",
        "Safrole ", "Ether ", "Éther ", "Isosafrole", "Méthyl", "Pipéridyl",
        "1-phényl", "1-Phényl", "Huile ", "Chlorhydrate ", "Toluène",
        "4-pipéridone", "alpha-P", "N-Acétyl", "N-phényl", "Propyl",
    )

    def is_english_name_line(ln: str, next_ln: str | None) -> bool:
        """Heuristic: a new English-name line starts with a letter/digit,
        is not obviously a French/Spanish translation, and the next line or
        the same line contains an HS code anchor (for tight entries) or the
        line contains an open parenthesis with English description."""
        if not ln:
            return False
        if ln.startswith(("HS code", "PART", "SUBSTANCES INCLUDED", "English,",
                          "Page", "#", "*")):
            return False
        # Skip lines that are obviously French (common starts) or Spanish
        # translations. The simpler signal: English name lines end with ')'
        # or contain '(' somewhere before the end.
        if "(" in ln:
            return True
        return False

    def parse_table(block: str, table_num: str) -> list[RedRow]:
        out: list[RedRow] = []
        lines = [ln.rstrip() for ln in block.splitlines()]
        i = 0
        # Build entries by finding HS/CAS anchor lines and scanning back.
        n = len(lines)
        # Also capture english-name continuation lines (e.g. 2-line names).
        # Approach: walk forward, maintain a "buffer" of the most recent
        # non-HS/CAS lines; at each HS/CAS line, pick the English name as
        # the first buffer line that looks English (has '(' or is title-cased
        # and not a french-translation line).

        buffer: list[str] = []
        for idx in range(n):
            line = lines[idx].strip()
            if not line:
                continue
            if "HS code" in line and "CAS number" in line:
                # anchor line: extract CAS
                cas = ""
                cas_m = CAS_RE.search(line)
                if cas_m:
                    cas = cas_m.group(1)

                # Find the English name: the LAST line in the buffer that
                # contains '(' (the IUPAC parenthetical) - because the
                # buffer may start with leftover Spanish lines from the
                # previous entry, followed by the English line, then maybe
                # a wrap line.
                english_name = ""
                iupac = ""
                english_idx = -1
                for bidx in range(len(buffer) - 1, -1, -1):
                    if "(" in buffer[bidx]:
                        english_idx = bidx
                        break

                # Fallback: if no '(' found, the English line is likely the
                # last buffer line (single-word substances like "Safrole"
                # still have '(' with synonym; rare).
                if english_idx < 0 and buffer:
                    english_idx = len(buffer) - 1

                # If the English name line looks like a continuation (prev
                # buffer line ends with '-' or current line starts with a
                # lowercase word that is a typical chemical suffix), prepend
                # the prior buffer line.
                if english_idx > 0:
                    prev_ln = buffer[english_idx - 1]
                    cur_ln = buffer[english_idx]
                    if prev_ln.rstrip().endswith("-") or cur_ln.split()[0].islower():
                        # merge
                        buffer[english_idx] = prev_ln.rstrip() + cur_ln
                        # remove prev line so we don't double-count
                        del buffer[english_idx - 1]
                        english_idx -= 1

                if english_idx >= 0:
                    english_name = buffer[english_idx]
                    # Use the LAST top-level '(' as the split between the
                    # common name and the IUPAC / chem-abstracts name, since
                    # common names often contain embedded parens like
                    # "(phenylamino)" or "(S)-".
                    paren_start = _last_top_level_paren(english_name)
                    if paren_start >= 0:
                        iupac = english_name[paren_start + 1:].rstrip(") ").strip()
                        english_name_clean = english_name[:paren_start].strip()
                    else:
                        english_name_clean = english_name

                    # Handle IUPAC that wraps: if the paren isn't closed on
                    # same line, continuation may be next buffer line.
                    if english_name.count("(") > english_name.count(")"):
                        # append next buffer line
                        if english_idx + 1 < len(buffer):
                            iupac = (iupac + " " + buffer[english_idx + 1]).strip()
                            iupac = iupac.rstrip(")").strip()

                    # Trim trailing footnote digits
                    english_name_clean = re.sub(r"\d+\s*$", "", english_name_clean).strip()

                    french_name = line.split("HS code")[0].strip()
                    block_text = "\n".join(buffer + [line])

                    out.append(RedRow(
                        table=table_num,
                        chemical_name=english_name_clean,
                        cas_number=cas,
                        other_names="; ".join(filter(None, [iupac, french_name])),
                        source_url=source_url,
                        source_doc=path.name,
                        raw_text=block_text,
                    ))
                buffer = []
                continue

            # Skip obvious non-name lines (footnotes start with a digit + space)
            if re.match(r"^\d+\s", line) and len(line) > 60:
                # looks like a footnote body; drop
                continue
            # Skip trailing "salts of the substances..." note
            if line.lower().startswith("the salts of the substances"):
                continue
            if line.lower().startswith("# since january"):
                continue
            # Skip section header repeats
            if line.startswith(("SUBSTANCES INCLUDED", "PART ")):
                continue
            buffer.append(line)
        return out

    rows.extend(parse_table(t1_text, "I"))
    rows.extend(parse_table(t2_text, "II"))

    cleaned: list[RedRow] = []
    seen = set()
    for r in rows:
        cname = r.chemical_name.strip()
        if not cname or len(cname) < 3:
            continue
        if cname.lower().startswith(("page ", "english,", "part ")):
            continue
        # French-translation filter: drop rows whose english_name actually
        # looks French (starts with French article/accent).
        if cname.split()[0] in ("Acide", "Anhydride", "Phényl", "Éther", "Ether"):
            continue
        key = (r.table, cname.lower())
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(r)

    log.info("Red List: parsed %d rows (Table I+II)", len(cleaned))
    return cleaned


# ---------------------------------------------------------------------------
# Yellow List parsing
# ---------------------------------------------------------------------------

@dataclass
class YellowRow:
    drug_name: str
    synonyms: str
    convention_schedule: str
    cas_number: str
    notes: str
    source_url: str
    raw_text: str


def parse_yellow_list(path: Path, source_url: str) -> list[YellowRow]:
    """Yellow list rows look like:
        NA 001 25333-77-1 ACETORPHINE 3-O-acetyltetrahydro-...
    IDS code + optional CAS + DRUG NAME (uppercase) + chemical description.
    Sometimes lines wrap. Schedule derived from section header.
    """
    rows: list[YellowRow] = []
    section = ""  # current schedule
    current: YellowRow | None = None

    ids_re = re.compile(r"^(N[A-Z])[\s\-]?(\d{3})\b")
    # A CAS at start of rest-of-line (after IDS): optional.
    cas_head_re = re.compile(r"^(\d{2,7}-\d{2}-\d)\s+(.*)$")

    # Order matters. Use the exact "Section N / Narcotic Drugs Included in
    # Schedule X" markers which appear once at each transition.
    section_headers = [
        ("Narcotic Drugs Included in Schedule I of the 1961 Convention", "Schedule I (1961)"),
        ("Narcotic Drugs Included in Schedule II of the 1961 Convention", "Schedule II (1961)"),
        ("Narcotic Drugs Included in Schedule IV of the 1961 Convention", "Schedule IV (1961)"),
        ("Intermediate Opiate Raw Materials", "Intermediate Opiate Raw Materials"),
    ]

    with pdfplumber.open(path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line:
                    if current is not None:
                        rows.append(current)
                        current = None
                    continue

                # Section markers - require line to START with header,
                # not merely contain it (avoids mis-matching inline refs like
                # "*Refer to Section entitled 'Intermediate Opiate ...'").
                for key, label in section_headers:
                    if line.startswith(key):
                        section = label
                        if current is not None:
                            rows.append(current)
                            current = None
                        break

                m = ids_re.match(line)
                if m:
                    # flush prior
                    if current is not None:
                        rows.append(current)
                    rest = line[m.end():].strip()
                    cas = ""
                    cas_m = cas_head_re.match(rest)
                    if cas_m:
                        cas = cas_m.group(1)
                        rest = cas_m.group(2)
                    # Rest starts with UPPERCASE drug name then chemical desc.
                    # Split on first lowercase char that follows two consecutive
                    # whitespaces or heuristic: drug name is the run of ALLCAPS
                    # tokens (allowing hyphen, digits, parens) up to a
                    # non-uppercase token.
                    tokens = rest.split()
                    name_tokens: list[str] = []
                    desc_tokens: list[str] = []
                    hit_desc = False
                    for tok in tokens:
                        if not hit_desc and _looks_uppercase(tok):
                            name_tokens.append(tok)
                        else:
                            hit_desc = True
                            desc_tokens.append(tok)
                    drug_name = " ".join(name_tokens).strip()
                    desc = " ".join(desc_tokens).strip()

                    current = YellowRow(
                        drug_name=drug_name,
                        synonyms="",
                        convention_schedule=section,
                        cas_number=cas,
                        notes=desc,
                        source_url=source_url,
                        raw_text=line,
                    )
                    continue

                # Continuation line (description wrap) - append to notes
                if current is not None:
                    # Skip page numbers / headers (typically "IDS CODE CAS NO. ...")
                    if line.startswith("IDS CODE") or line.startswith("NARCOTIC DRUGS"):
                        continue
                    current.notes = (current.notes + " " + line).strip()
                    current.raw_text = current.raw_text + "\n" + line

    if current is not None:
        rows.append(current)

    # Clean rows: require drug_name
    cleaned = []
    seen = set()
    for r in rows:
        if not r.drug_name or len(r.drug_name) < 2:
            continue
        # Sanity: drug_name should be uppercase-ish. Skip rows whose name is
        # obviously not a drug header.
        if r.drug_name.lower() in ("", "and", "the"):
            continue
        key = (r.drug_name, r.convention_schedule)
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(r)
    log.info("Yellow List: parsed %d rows", len(cleaned))
    return cleaned


def _looks_uppercase(tok: str) -> bool:
    """Return True if token is drug-name-ish (ALL CAPS, digits, hyphens,
    commas, parentheses, slashes)."""
    # Allow commas/parens/hyphens/digits but require at least one uppercase
    # letter and no lowercase letters.
    if any(c.islower() for c in tok):
        return False
    return any(c.isupper() for c in tok) or tok.strip(",.()[]/") == ""


# ---------------------------------------------------------------------------
# Green List parsing
# ---------------------------------------------------------------------------

@dataclass
class GreenRow:
    substance_name: str
    synonyms: str
    convention_schedule: str
    cas_number: str
    notes: str
    source_url: str
    raw_text: str


def parse_green_list(path: Path, source_url: str) -> list[GreenRow]:
    """Green list rows look like:
        PC 010 CATHINONE (–)-(S)-2-aminopropiophenone
        PL 002 (+)-LYSERGIDE LSD, LSD-25 9,10-didehydro-...
    Sections: Schedule I/II/III/IV of the 1971 Convention.
    Sometimes there's a CAS number column (Schedule II/III/IV).
    """
    rows: list[GreenRow] = []
    section = ""
    current: GreenRow | None = None

    ids_re = re.compile(r"^(P[A-Z])[\s\-]?(\d{3})\b")

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line:
                    if current is not None:
                        rows.append(current)
                        current = None
                    continue
                # Detect section headers
                if line.startswith("Substances in Schedule"):
                    if current is not None:
                        rows.append(current)
                        current = None
                    # e.g. "Substances in Schedule I" -> "Schedule I (1971)"
                    m = re.match(r"Substances in Schedule\s+([IV]+)", line)
                    if m:
                        section = f"Schedule {m.group(1)} (1971)"
                    continue

                m = ids_re.match(line)
                if m:
                    if current is not None:
                        rows.append(current)
                    rest = line[m.end():].strip()

                    # Try to extract trailing CAS number anywhere in rest
                    cas = ""
                    cas_m = CAS_RE.search(rest)
                    if cas_m:
                        cas = cas_m.group(1)

                    # Name parsing: first token(s) until a parenthesis/chemical
                    # descriptor appears or lowercase word. The INN (if any)
                    # is ALL CAPS. Trivial/chemical desc follows.
                    # Simple heuristic: drug_name = first ALLCAPS run; rest =
                    # synonyms+chemical description.
                    tokens = rest.split()
                    name_tokens: list[str] = []
                    syn_tokens: list[str] = []
                    hit = False
                    for tok in tokens:
                        if not hit and _looks_uppercase(tok):
                            name_tokens.append(tok)
                        else:
                            hit = True
                            syn_tokens.append(tok)

                    name = " ".join(name_tokens).strip(" ,;-") or " ".join(tokens[:1])
                    syn = " ".join(syn_tokens).strip()

                    current = GreenRow(
                        substance_name=name,
                        synonyms=syn,
                        convention_schedule=section,
                        cas_number=cas,
                        notes="",
                        source_url=source_url,
                        raw_text=line,
                    )
                    continue

                if current is not None:
                    # Skip headers
                    low = line.lower()
                    if low.startswith(("ids code", "international non-proprietary",
                                       "other", "non-proprietary", "part one", "part two",
                                       "part three")):
                        continue
                    current.notes = (current.notes + " " + line).strip()
                    current.raw_text += "\n" + line
                    # Opportunistically grab CAS from continuation line
                    if not current.cas_number:
                        cas_m = CAS_RE.search(line)
                        if cas_m:
                            current.cas_number = cas_m.group(1)

    if current is not None:
        rows.append(current)

    cleaned = []
    seen = set()
    for r in rows:
        if not r.substance_name or len(r.substance_name) < 2:
            continue
        key = (r.substance_name, r.convention_schedule)
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(r)
    log.info("Green List: parsed %d rows", len(cleaned))
    return cleaned


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def write_csv(path: Path, header: list[str], rows: Iterable[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    log.info("wrote %s (%d bytes)", path, path.stat().st_size)


def write_sqlite(red: list[RedRow], yellow: list[YellowRow], green: list[GreenRow]) -> None:
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE incb_red_list (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            "table" TEXT,
            chemical_name TEXT,
            cas_number TEXT,
            international_nonproprietary_name TEXT,
            other_names TEXT,
            source_url TEXT,
            source_doc TEXT,
            raw_text TEXT,
            fetched_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE incb_yellow_list (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            drug_name TEXT,
            synonyms TEXT,
            convention_schedule TEXT,
            cas_number TEXT,
            notes TEXT,
            source_url TEXT,
            raw_text TEXT,
            fetched_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE incb_green_list (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            substance_name TEXT,
            synonyms TEXT,
            convention_schedule TEXT,
            cas_number TEXT,
            notes TEXT,
            source_url TEXT,
            raw_text TEXT,
            fetched_at TEXT
        )
    """)

    c.executemany(
        """INSERT INTO incb_red_list
           ("table", chemical_name, cas_number,
            international_nonproprietary_name, other_names,
            source_url, source_doc, raw_text, fetched_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        [
            (r.table, r.chemical_name, r.cas_number, "",
             r.other_names, r.source_url, r.source_doc, r.raw_text, FETCHED_AT)
            for r in red
        ],
    )
    c.executemany(
        """INSERT INTO incb_yellow_list
           (drug_name, synonyms, convention_schedule, cas_number, notes,
            source_url, raw_text, fetched_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        [
            (r.drug_name, r.synonyms, r.convention_schedule, r.cas_number,
             r.notes, r.source_url, r.raw_text, FETCHED_AT)
            for r in yellow
        ],
    )
    c.executemany(
        """INSERT INTO incb_green_list
           (substance_name, synonyms, convention_schedule, cas_number, notes,
            source_url, raw_text, fetched_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        [
            (r.substance_name, r.synonyms, r.convention_schedule, r.cas_number,
             r.notes, r.source_url, r.raw_text, FETCHED_AT)
            for r in green
        ],
    )
    conn.commit()
    conn.close()
    log.info("wrote %s (%d bytes)", DB_PATH, DB_PATH.stat().st_size)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    for key, spec in SOURCES.items():
        download(spec["url"], spec["file"])

    red = parse_red_list(SOURCES["red"]["file"], SOURCES["red"]["url"])
    yellow = parse_yellow_list(SOURCES["yellow"]["file"], SOURCES["yellow"]["url"])
    green = parse_green_list(SOURCES["green"]["file"], SOURCES["green"]["url"])

    # CSVs
    write_csv(
        CSV_DIR / "incb_red_list.csv",
        ["table", "chemical_name", "cas_number",
         "international_nonproprietary_name", "other_names",
         "source_url", "source_doc", "raw_text", "fetched_at"],
        (
            {
                "table": r.table,
                "chemical_name": r.chemical_name,
                "cas_number": r.cas_number,
                "international_nonproprietary_name": "",
                "other_names": r.other_names,
                "source_url": r.source_url,
                "source_doc": r.source_doc,
                "raw_text": r.raw_text,
                "fetched_at": FETCHED_AT,
            }
            for r in red
        ),
    )
    write_csv(
        CSV_DIR / "incb_yellow_list.csv",
        ["drug_name", "synonyms", "convention_schedule", "cas_number",
         "notes", "source_url", "raw_text", "fetched_at"],
        (
            {
                "drug_name": r.drug_name,
                "synonyms": r.synonyms,
                "convention_schedule": r.convention_schedule,
                "cas_number": r.cas_number,
                "notes": r.notes,
                "source_url": r.source_url,
                "raw_text": r.raw_text,
                "fetched_at": FETCHED_AT,
            }
            for r in yellow
        ),
    )
    write_csv(
        CSV_DIR / "incb_green_list.csv",
        ["substance_name", "synonyms", "convention_schedule", "cas_number",
         "notes", "source_url", "raw_text", "fetched_at"],
        (
            {
                "substance_name": r.substance_name,
                "synonyms": r.synonyms,
                "convention_schedule": r.convention_schedule,
                "cas_number": r.cas_number,
                "notes": r.notes,
                "source_url": r.source_url,
                "raw_text": r.raw_text,
                "fetched_at": FETCHED_AT,
            }
            for r in green
        ),
    )
    write_sqlite(red, yellow, green)

    log.info("summary: red=%d yellow=%d green=%d", len(red), len(yellow), len(green))


if __name__ == "__main__":
    main()
