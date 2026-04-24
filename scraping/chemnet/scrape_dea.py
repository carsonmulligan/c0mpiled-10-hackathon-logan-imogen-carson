"""
DEA scraper: List I/II precursors, Schedules I-V, and Orange Book alphabetical list.

The DEA moved its old /21cfr/cfr/... HTML pages. Current canonical sources:

  eCFR XML (section-accurate text of the CFR):
    https://www.ecfr.gov/api/versioner/v1/full/<date>/title-21.xml?chapter=II&part=1310&section=1310.02
    https://www.ecfr.gov/api/versioner/v1/full/<date>/title-21.xml?chapter=II&part=1308&section=1308.11   (Schedule I)
    ... 1308.12 / 1308.13 / 1308.14 / 1308.15 for Schedules II–V

  DEA Orange Book PDFs (tabular published lists):
    https://www.deadiversion.usdoj.gov/schedules/orangebook/c_cs_alpha.pdf       (controlled substances alphabetical)
    https://www.deadiversion.usdoj.gov/schedules/orangebook/e_cs_sched.pdf       (controlled substances by schedule)
    https://www.deadiversion.usdoj.gov/schedules/orangebook/f_chemlist_alpha.pdf (List I/II chemicals alphabetical)

  DEA Chemical Control page (landing HTML):
    https://www.deadiversion.usdoj.gov/chem_prog/34chems.html

Outputs under data/chemnet/ only:
  raw/dea/*.xml, *.pdf, *.html, *.txt, manifest.json
  csv/dea_precursors.csv, csv/dea_schedules.csv, csv/dea_orange_book.csv
  dbs/dea.sqlite3
  logs/dea.log
"""
from __future__ import annotations

import csv
import json
import logging
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import warnings

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent
RAW = ROOT / "raw" / "dea"
CSV_DIR = ROOT / "csv"
DB_DIR = ROOT / "dbs"
LOG_DIR = ROOT / "logs"
DB_PATH = DB_DIR / "dea.sqlite3"
LOG_PATH = LOG_DIR / "dea.log"

for d in (RAW, CSV_DIR, DB_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("dea")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml,application/pdf,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

ECFR_DATE = "2026-04-22"  # most recent published issue date as of run
ECFR_BASE = (
    "https://www.ecfr.gov/api/versioner/v1/full/"
    f"{ECFR_DATE}/title-21.xml?chapter=II"
)

URLS = {
    # eCFR XML — precursors
    "ecfr_1310_02": f"{ECFR_BASE}&part=1310&section=1310.02",
    # eCFR XML — schedules
    "ecfr_1308_11": f"{ECFR_BASE}&part=1308&section=1308.11",
    "ecfr_1308_12": f"{ECFR_BASE}&part=1308&section=1308.12",
    "ecfr_1308_13": f"{ECFR_BASE}&part=1308&section=1308.13",
    "ecfr_1308_14": f"{ECFR_BASE}&part=1308&section=1308.14",
    "ecfr_1308_15": f"{ECFR_BASE}&part=1308&section=1308.15",
    # DEA Orange Book PDFs
    "ob_c_cs_alpha": "https://www.deadiversion.usdoj.gov/schedules/orangebook/c_cs_alpha.pdf",
    "ob_e_cs_sched": "https://www.deadiversion.usdoj.gov/schedules/orangebook/e_cs_sched.pdf",
    "ob_f_chemlist_alpha": "https://www.deadiversion.usdoj.gov/schedules/orangebook/f_chemlist_alpha.pdf",
    "ob_g_chemlist_deacode": "https://www.deadiversion.usdoj.gov/schedules/orangebook/g_chemlist_deacode.pdf",
    # Landing page (kept as raw reference)
    "chem_prog_34chems": "https://www.deadiversion.usdoj.gov/chem_prog/34chems.html",
}

SCHEDULE_SECTIONS = {
    "I":   "ecfr_1308_11",
    "II":  "ecfr_1308_12",
    "III": "ecfr_1308_13",
    "IV":  "ecfr_1308_14",
    "V":   "ecfr_1308_15",
}

CAS_RE = re.compile(r"\b\d{2,7}-\d{2}-\d\b")
CODE_RE = re.compile(r"\b\d{4}\b")

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# HTTP + caching
# ---------------------------------------------------------------------------
def fetch(url: str, out: Path, binary: bool = False, tries: int = 2):
    if out.exists() and out.stat().st_size > 0:
        log.info("cache hit %s", out.name)
        return out.read_bytes() if binary else out.read_text(encoding="utf-8", errors="replace")

    for attempt in range(1, tries + 1):
        try:
            log.info("GET %s (try %d)", url, attempt)
            r = requests.get(url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            if binary:
                out.write_bytes(r.content)
                return r.content
            out.write_text(r.text, encoding="utf-8")
            return r.text
        except Exception as e:  # noqa: BLE001
            log.warning("fetch fail %s attempt %d: %s", url, attempt, e)
            time.sleep(1.5 * attempt)
    log.error("giving up on %s", url)
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def clean_ws(s: str) -> str:
    return " ".join(s.split()).strip()


def strip_leading_enum(s: str) -> str:
    s = re.sub(r"^\(\s*[A-Za-z0-9ivxlc]+\s*\)\s*", "", s)
    s = re.sub(r"^\d+\.\s*", "", s)
    return s.strip()


def is_garbage(name: str) -> bool:
    if not name:
        return True
    if len(name) < 2 or len(name) > 500:
        return True
    low = name.lower()
    bad = ("click here", "adobe", "print this", "privacy policy",
           "u.s. department", "website", "page ")
    return any(h in low for h in bad)


# ---------------------------------------------------------------------------
# Parser: eCFR section XML -> list of (enum_name, dea_code, raw)
# ---------------------------------------------------------------------------
def parse_ecfr_section(xml_text: str) -> list[tuple[str, str, str]]:
    """
    eCFR section XML renders substance tables as:

        <TR>
          <TD class="left">(1) Acetyl-...-fentanyl</TD>
          <TD class="right">9815</TD>
        </TR>

    We pull every TR that has (a) a 'left' TD starting with "(n) " and
    (b) a 'right' TD containing a 4-digit DEA code.
    Returns list of (name, code, raw_line).
    """
    # Use html.parser so fragments (produced by string-slicing the XML to
    # separate List I/II blocks) still parse. html.parser is case-insensitive
    # and lenient about missing roots.
    soup = BeautifulSoup(xml_text, "html.parser")
    out: list[tuple[str, str, str]] = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        left = clean_ws(tds[0].get_text(" ", strip=True))
        right = clean_ws(tds[-1].get_text(" ", strip=True))
        if not left:
            continue
        code_match = re.search(r"\b(\d{4})\b", right)
        dea_code = code_match.group(1) if code_match else ""
        raw = f"{left}  {right}".strip()
        # Require either enumerated entry or a DEA code present
        if not (re.match(r"^\(\s*\w+\s*\)", left) or dea_code):
            continue
        name = strip_leading_enum(left).strip(" .;,")
        if is_garbage(name):
            continue
        out.append((name, dea_code, raw))
    return out


# ---------------------------------------------------------------------------
# PDF -> text fallback chain
# ---------------------------------------------------------------------------
def pdf_to_text(pdf_path: Path, layout: bool = False) -> str:
    # pdfplumber
    try:
        import pdfplumber  # type: ignore
        with pdfplumber.open(str(pdf_path)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
        text = "\n".join(pages)
        if text.strip():
            return text
    except Exception as e:  # noqa: BLE001
        log.warning("pdfplumber failed on %s: %s", pdf_path.name, e)

    # pypdf
    try:
        import pypdf  # type: ignore
        reader = pypdf.PdfReader(str(pdf_path))
        text = "\n".join((p.extract_text() or "") for p in reader.pages)
        if text.strip():
            return text
    except Exception as e:  # noqa: BLE001
        log.warning("pypdf failed on %s: %s", pdf_path.name, e)

    # pdftotext CLI
    try:
        args = ["pdftotext"]
        if layout:
            args.append("-layout")
        args += [str(pdf_path), "-"]
        out = subprocess.run(args, capture_output=True, text=True, timeout=120)
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout
    except Exception as e:  # noqa: BLE001
        log.warning("pdftotext failed on %s: %s", pdf_path.name, e)

    return ""


# ---------------------------------------------------------------------------
# Parser: Orange Book alphabetical PDF (c_cs_alpha.pdf / e_cs_sched.pdf)
# ---------------------------------------------------------------------------
def parse_orange_book(text: str, source_url: str, source_label: str) -> list[dict]:
    SCHED_TOKENS = {"I", "II", "III", "IV", "V"}
    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()

    def has_code_and_schedule(s: str) -> bool:
        if not re.search(r"\b\d{4}\b", s):
            return False
        return bool(re.search(r"(?<![A-Za-z])(I{1,3}|IV|V)(?![A-Za-z])", s))

    # Pre-pass: collect (main_line, [continuation_lines]) pairs.
    raw_lines = text.splitlines()
    grouped: list[tuple[str, list[str]]] = []
    for raw in raw_lines:
        stripped = raw.strip()
        if not stripped:
            continue
        if has_code_and_schedule(stripped):
            grouped.append((stripped, []))
        elif grouped:
            # continuation of the last main row
            grouped[-1][1].append(stripped)

    for main_line, conts in grouped:
        line = main_line
        if not line:
            continue
        low = line.lower()
        if low.startswith(("substance", "controlled substances", "page ",
                           "alphabetical", "dea diversion", "u.s. department",
                           "drug enforcement", "cscn csa", "cscn ",
                           "exceptions", "other names")):
            continue

        tokens = line.split()
        if len(tokens) < 3:
            continue

        # Find 4-digit DEA code
        code_idx = -1
        for i, t in enumerate(tokens):
            if re.fullmatch(r"\d{4}", t):
                code_idx = i
                break
        if code_idx == -1:
            continue

        # Schedule should be within a couple tokens after the code
        sched = ""
        sched_idx = -1
        for j in range(code_idx + 1, min(code_idx + 4, len(tokens))):
            tok = tokens[j].rstrip(",.").upper()
            if tok in SCHED_TOKENS:
                sched = tok
                sched_idx = j
                break
        if not sched:
            continue

        name = " ".join(tokens[:code_idx]).strip(" .;,")
        dea_code = tokens[code_idx]

        # If the name ended with a hyphen (wrapped mid-word) and we have a
        # continuation, splice the first continuation chunk into the name and
        # treat any further tokens on that continuation as trailing "other
        # names".
        extra_other: list[str] = []
        if conts:
            first = conts[0]
            first_tokens = first.split()
            # Heuristic: first continuation token fuses onto the hyphen.
            if name.endswith("-") and first_tokens:
                name = name + first_tokens[0]
                rest = " ".join(first_tokens[1:])
                if rest:
                    extra_other.append(rest)
            else:
                extra_other.append(first)
            for more in conts[1:]:
                extra_other.append(more)

        narcotic = ""
        rest_start = sched_idx + 1
        if rest_start < len(tokens):
            nxt = tokens[rest_start].rstrip(",.").upper()
            if nxt in {"Y", "N"}:
                narcotic = nxt
                rest_start += 1

        other_parts = [" ".join(tokens[rest_start:]).strip()] + extra_other
        other = " ".join(p for p in other_parts if p).strip()

        if is_garbage(name) or len(name) < 2:
            continue

        key = (name.lower(), dea_code)
        if key in seen:
            continue
        seen.add(key)

        rows.append({
            "name": name,
            "dea_code": dea_code,
            "schedule": sched,
            "narcotic": narcotic,
            "other_names": other,
            "source": source_label,
            "source_url": source_url,
            "raw_text": (main_line + " " + " ".join(conts)).strip(),
            "fetched_at": NOW,
        })
    return rows


# ---------------------------------------------------------------------------
# Parser: Chemlist PDF (f_chemlist_alpha.pdf) - List I/II precursors
# columns: SUBSTANCE CSCN LIST
# ---------------------------------------------------------------------------
def parse_chemlist(text: str, source_url: str) -> list[dict]:
    """
    f_chemlist_alpha.pdf rows look like:

       BENZYL CYANIDE                               I 8735 Pub. L. 100-690 3/18/1989
       NAME ...                                     II <code> <citation> <date>

    The LIST token ("I" or "II") precedes the 4-digit DEA code, and the rest
    of the row is the Federal Register citation and effective date. Lines are
    often wrapped; we only pick the header line (which has the LIST + CODE).
    Some rows have no CODE (e.g. "superseded" or "REMOVED") — skip those.
    """
    rows: list[dict] = []
    seen: set[str] = set()
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        low = line.lower()
        if low.startswith(("substance", "regulated chemical", "list i and ii",
                           "alphabetical", "by list", "by dea",
                           "page ", "u.s. department", "drug enforcement",
                           "dea diversion", "chemical", "dea", "code",
                           "effective")):
            continue
        tokens = line.split()
        if len(tokens) < 4:
            continue
        # Find LIST token followed by a 4-digit CODE token
        list_val = ""
        code_idx = -1
        for i, t in enumerate(tokens[:-1]):
            tok = t.rstrip(",.").upper()
            nxt = tokens[i + 1]
            if tok in {"I", "II"} and re.fullmatch(r"\d{4}", nxt):
                list_val = tok
                code_idx = i + 1
                break
        if not list_val or code_idx == -1:
            continue
        name = " ".join(tokens[:code_idx - 1]).strip(" .;,")
        dea_code = tokens[code_idx]
        # Filter historical "superseded"/"REMOVED" entries that are
        # administrative artefacts, not live listings.
        low_name = name.lower()
        if "superseded" in low_name or "removed" in low_name:
            continue
        if is_garbage(name) or len(name) < 2:
            continue
        key = (list_val, name.lower())
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "list": list_val,
            "name": name,
            "cas_numbers": "",  # chemlist PDF does not include CAS
            "dea_code": dea_code,
            "source_url": source_url,
            "raw_text": raw.strip(),
            "fetched_at": NOW,
        })
    return rows


# ---------------------------------------------------------------------------
# CSV + SQLite writers
# ---------------------------------------------------------------------------
def write_csv(rows: list[dict], path: Path, preferred: list[str]) -> None:
    if not rows:
        path.write_text("")
        log.warning("no rows for %s", path.name)
        return
    fields = list({k for r in rows for k in r.keys()})
    ordered = [f for f in preferred if f in fields] + [f for f in fields if f not in preferred]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ordered)
        w.writeheader()
        w.writerows(rows)
    log.info("wrote %s (%d rows)", path.name, len(rows))


def write_sqlite_table(con: sqlite3.Connection, table: str, schema_sql: str,
                       rows: list[dict], cols: list[str]) -> None:
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(schema_sql)
    if not rows:
        return
    placeholders = ",".join(":" + c for c in cols)
    con.executemany(
        f"INSERT INTO {table}({','.join(cols)}) VALUES({placeholders})",
        [{c: r.get(c, "") for c in cols} for r in rows],
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    failures: list[tuple[str, str]] = []

    # Cache the 34chems landing HTML for provenance
    fetch(URLS["chem_prog_34chems"], RAW / "dea_34chems.html")

    # ---------- Precursors (eCFR 1310.02 XML) ----------
    log.info("=== 1310.02 precursors via eCFR XML ===")
    precursors: list[dict] = []
    xml = fetch(URLS["ecfr_1310_02"], RAW / "ecfr_1310_02.xml")
    if xml:
        entries = parse_ecfr_section(xml)
        # eCFR 1310.02 contains "(a) List I" ... "(b) List II" with tables for each.
        # Because parse_ecfr_section just flattens all TR rows, we need to map each
        # entry to I vs II by re-reading the XML around the TR.
        # Simpler: walk the raw XML and split on "(a) List I" / "(b) List II" markers.
        sA, sB = xml.find("(a) List I"), xml.find("(b) List II")
        if sA == -1 or sB == -1:
            # fall back: everything into "I"
            list_a_xml = xml; list_b_xml = ""
        else:
            list_a_xml = xml[sA:sB]; list_b_xml = xml[sB:]

        list_I = parse_ecfr_section(list_a_xml)
        list_II = parse_ecfr_section(list_b_xml)

        for name, dea_code, raw in list_I:
            precursors.append({
                "list": "I",
                "name": name,
                "cas_numbers": ";".join(dict.fromkeys(CAS_RE.findall(raw))),
                "dea_code": dea_code,
                "source_url": URLS["ecfr_1310_02"],
                "raw_text": raw,
                "fetched_at": NOW,
            })
        for name, dea_code, raw in list_II:
            precursors.append({
                "list": "II",
                "name": name,
                "cas_numbers": ";".join(dict.fromkeys(CAS_RE.findall(raw))),
                "dea_code": dea_code,
                "source_url": URLS["ecfr_1310_02"],
                "raw_text": raw,
                "fetched_at": NOW,
            })
        log.info("1310.02 parsed %d precursors (I=%d II=%d)",
                 len(precursors), len(list_I), len(list_II))
    else:
        failures.append((URLS["ecfr_1310_02"], "fetch failed"))

    # Augment/verify via DEA chemlist PDF
    pdf_bytes = fetch(URLS["ob_f_chemlist_alpha"],
                      RAW / "dea_f_chemlist_alpha.pdf", binary=True)
    if pdf_bytes:
        txt = pdf_to_text(RAW / "dea_f_chemlist_alpha.pdf", layout=True)
        (RAW / "dea_f_chemlist_alpha.txt").write_text(txt, encoding="utf-8")
        extra = parse_chemlist(txt, URLS["ob_f_chemlist_alpha"])
        have_name = {(r["list"], r["name"].lower()) for r in precursors}
        have_code = {(r["list"], r["dea_code"]) for r in precursors if r.get("dea_code")}
        added = 0
        for r in extra:
            name_key = (r["list"], r["name"].lower())
            code_key = (r["list"], r["dea_code"]) if r.get("dea_code") else None
            if name_key in have_name:
                continue
            if code_key and code_key in have_code:
                # same DEA code as an existing canonical entry — skip historical variant
                continue
            precursors.append(r)
            have_name.add(name_key)
            if code_key:
                have_code.add(code_key)
            added += 1
        log.info("chemlist PDF added %d rows (total precursors=%d)", added, len(precursors))
    else:
        failures.append((URLS["ob_f_chemlist_alpha"], "fetch failed"))

    # ---------- Schedules (eCFR 1308.11–15 XML) ----------
    log.info("=== 1308 schedules via eCFR XML ===")
    schedules: list[dict] = []
    for sched, key in SCHEDULE_SECTIONS.items():
        url = URLS[key]
        xml = fetch(url, RAW / f"ecfr_{key.replace('ecfr_', '')}.xml")
        if not xml:
            failures.append((url, "fetch failed")); continue
        entries = parse_ecfr_section(xml)
        for name, dea_code, raw in entries:
            schedules.append({
                "schedule": sched,
                "name": name,
                "aliases": "",
                "cas_numbers": ";".join(dict.fromkeys(CAS_RE.findall(raw))),
                "dea_code": dea_code,
                "source_url": url,
                "raw_text": raw,
                "fetched_at": NOW,
            })
        log.info("schedule %s -> %d entries", sched, len(entries))

    # Augment via Orange Book "by schedule" PDF
    pdf_bytes = fetch(URLS["ob_e_cs_sched"],
                      RAW / "dea_e_cs_sched.pdf", binary=True)
    if pdf_bytes:
        txt = pdf_to_text(RAW / "dea_e_cs_sched.pdf", layout=True)
        (RAW / "dea_e_cs_sched.txt").write_text(txt, encoding="utf-8")
        extra = parse_orange_book(txt, URLS["ob_e_cs_sched"],
                                  "DEA Orange Book (e_cs_sched.pdf)")
        have = {(r["schedule"], r["name"].lower()) for r in schedules}
        added = 0
        for r in extra:
            key = (r["schedule"], r["name"].lower())
            if key not in have:
                schedules.append({
                    "schedule": r["schedule"],
                    "name": r["name"],
                    "aliases": r.get("other_names", ""),
                    "cas_numbers": "",
                    "dea_code": r["dea_code"],
                    "source_url": r["source_url"],
                    "raw_text": r["raw_text"],
                    "fetched_at": NOW,
                })
                have.add(key); added += 1
        log.info("e_cs_sched PDF added %d rows (total schedules=%d)", added, len(schedules))
    else:
        failures.append((URLS["ob_e_cs_sched"], "fetch failed"))

    # ---------- Orange Book alphabetical PDF ----------
    log.info("=== Orange Book c_cs_alpha ===")
    orange: list[dict] = []
    pdf_bytes = fetch(URLS["ob_c_cs_alpha"],
                      RAW / "dea_c_cs_alpha.pdf", binary=True)
    if pdf_bytes:
        txt = pdf_to_text(RAW / "dea_c_cs_alpha.pdf", layout=True)
        (RAW / "dea_c_cs_alpha.txt").write_text(txt, encoding="utf-8")
        orange = parse_orange_book(txt, URLS["ob_c_cs_alpha"],
                                   "DEA Orange Book (c_cs_alpha.pdf)")
        log.info("orange book -> %d rows", len(orange))
    else:
        failures.append((URLS["ob_c_cs_alpha"], "fetch failed"))

    # ---------- Write outputs ----------
    write_csv(precursors, CSV_DIR / "dea_precursors.csv",
              ["list", "name", "cas_numbers", "dea_code", "source_url",
               "raw_text", "fetched_at"])
    write_csv(schedules, CSV_DIR / "dea_schedules.csv",
              ["schedule", "name", "aliases", "cas_numbers", "dea_code",
               "source_url", "raw_text", "fetched_at"])
    write_csv(orange, CSV_DIR / "dea_orange_book.csv",
              ["name", "dea_code", "schedule", "narcotic", "other_names",
               "source", "source_url", "raw_text", "fetched_at"])

    con = sqlite3.connect(DB_PATH)
    try:
        write_sqlite_table(
            con, "dea_precursors",
            """CREATE TABLE dea_precursors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                list TEXT, name TEXT, cas_numbers TEXT, dea_code TEXT,
                source_url TEXT, raw_text TEXT, fetched_at TEXT
            )""",
            precursors,
            ["list", "name", "cas_numbers", "dea_code", "source_url",
             "raw_text", "fetched_at"],
        )
        write_sqlite_table(
            con, "dea_schedules",
            """CREATE TABLE dea_schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule TEXT, name TEXT, aliases TEXT, cas_numbers TEXT,
                dea_code TEXT, source_url TEXT, raw_text TEXT, fetched_at TEXT
            )""",
            schedules,
            ["schedule", "name", "aliases", "cas_numbers", "dea_code",
             "source_url", "raw_text", "fetched_at"],
        )
        write_sqlite_table(
            con, "dea_orange_book",
            """CREATE TABLE dea_orange_book (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT, dea_code TEXT, schedule TEXT, narcotic TEXT,
                other_names TEXT, source TEXT, source_url TEXT,
                raw_text TEXT, fetched_at TEXT
            )""",
            orange,
            ["name", "dea_code", "schedule", "narcotic", "other_names",
             "source", "source_url", "raw_text", "fetched_at"],
        )
        con.commit()
    finally:
        con.close()

    (RAW / "manifest.json").write_text(json.dumps({
        "precursors": len(precursors),
        "schedules": len(schedules),
        "orange_book": len(orange),
        "failures": failures,
        "fetched_at": NOW,
    }, indent=2))

    print(f"[DEA] precursors={len(precursors)} schedules={len(schedules)} orange_book={len(orange)}")
    if failures:
        print(f"[DEA] failures: {failures}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
