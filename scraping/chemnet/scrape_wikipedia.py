#!/usr/bin/env python3
"""
Wikipedia scraper for controlled substances, drug precursors, and chemical warfare agents.

Writes to:
  data/chemnet/raw/wikipedia/<page>.json and .html
  data/chemnet/csv/wikipedia_*.csv
  data/chemnet/dbs/wikipedia.sqlite3
  data/chemnet/logs/wikipedia.log
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "raw" / "wikipedia"
CSV_DIR = ROOT / "csv"
DB_DIR = ROOT / "dbs"
LOG_DIR = ROOT / "logs"
for d in (RAW_DIR, CSV_DIR, DB_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

LOG_PATH = LOG_DIR / "wikipedia.log"
DB_PATH = DB_DIR / "wikipedia.sqlite3"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="w"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("wikipedia")

USER_AGENT = (
    "chemnet-hackathon-scraper/1.0 (educational; contact: profiles.co@gmail.com)"
)
API = "https://en.wikipedia.org/w/api.php"
HTML_BASE = "https://en.wikipedia.org/wiki/"
SLEEP = 0.5

PAGES = [
    "List_of_Schedule_I_controlled_substances_(U.S.)",
    "List_of_Schedule_II_controlled_substances_(U.S.)",
    "List_of_Schedule_III_controlled_substances_(U.S.)",
    "List_of_Schedule_IV_controlled_substances_(U.S.)",
    "List_of_Schedule_V_controlled_substances_(U.S.)",
    "DEA_list_of_chemicals",
    "List_of_psychoactive_plants",
    "Drug_precursor",
    "Chemical_Weapons_Convention",
    "List_of_chemical_warfare_agents",
    "NFPA_704",
    "Methamphetamine",
    "List_of_controlled_drugs_in_the_United_Kingdom",
    "Misuse_of_Drugs_Act_1971",
    "Controlled_Substances_Act",
    "List_of_fentanyl_analogues",
    "List_of_designer_drugs",
    "List_of_investigational_dissociative_drugs",
    "Australia_Group",
]

CAS_RE = re.compile(r"\b\d{2,7}-\d{2}-\d\b")
HEADING_TAGS = {"h2", "h3", "h4"}


@dataclass
class CompoundRow:
    page_title: str
    name: str
    cas_numbers: str
    aliases: str
    schedule_or_category: str
    row_section: str
    raw_text: str
    source_url: str
    fetched_at: str


@dataclass
class PageRow:
    page_title: str
    url: str
    extracted_at: str
    html_path: str
    wikitext_path: str
    row_count: int


def http_get(url: str, params: dict | None = None) -> requests.Response:
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning("fetch error %s (attempt %s): %s", url, attempt + 1, e)
            time.sleep(1 + attempt)
    raise RuntimeError(f"failed to fetch {url}")


def fetch_page(title: str) -> tuple[str, str]:
    """Fetch both HTML and wikitext. Return (html, wikitext)."""
    # HTML
    html_path = RAW_DIR / f"{safe_name(title)}.html"
    wikitext_path = RAW_DIR / f"{safe_name(title)}.wikitext"

    if html_path.exists() and html_path.stat().st_size > 1000:
        html = html_path.read_text(encoding="utf-8")
    else:
        url = HTML_BASE + title
        log.info("fetch html %s", url)
        r = http_get(url)
        html = r.text
        html_path.write_text(html, encoding="utf-8")
        time.sleep(SLEEP)

    if wikitext_path.exists() and wikitext_path.stat().st_size > 100:
        wikitext = wikitext_path.read_text(encoding="utf-8")
    else:
        log.info("fetch wikitext %s", title)
        r = http_get(
            API,
            params={
                "action": "parse",
                "page": title,
                "format": "json",
                "prop": "wikitext",
                "redirects": 1,
            },
        )
        try:
            data = r.json()
            wikitext = data.get("parse", {}).get("wikitext", {}).get("*", "") or ""
        except Exception as e:
            log.warning("wikitext parse error for %s: %s", title, e)
            wikitext = ""
        wikitext_path.write_text(wikitext, encoding="utf-8")
        time.sleep(SLEEP)

    return html, wikitext


def safe_name(title: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", title)[:200]


def extract_cas(text: str) -> list[str]:
    if not text:
        return []
    # Validate check-digit loosely: accept what regex matches; dedupe
    seen: list[str] = []
    for m in CAS_RE.findall(text):
        if m not in seen:
            seen.append(m)
    return seen


def nearest_heading(node) -> str:
    """Walk backwards to find the nearest preceding h2/h3/h4 heading text."""
    cur = node
    while cur is not None:
        prev = cur.find_previous(list(HEADING_TAGS))
        if prev is None:
            return ""
        # mw-headline span is inside
        span = prev.find("span", class_="mw-headline")
        if span:
            return span.get_text(" ", strip=True)
        return prev.get_text(" ", strip=True)
    return ""


def clean_name(text: str) -> str:
    # Strip citation brackets like [1], footnote refs
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text).strip(" -–—•\t\n")
    return text.strip()


NOISE_TOKENS = (
    "portal",
    "see also",
    "references",
    "external links",
    "further reading",
    "main article",
    "wikipedia",
    "wikimedia",
    "commons",
    "category:",
    "template:",
    "relates to",
    "this article",
    "this list",
    "not to be confused",
    "retrieved from",
    "isbn ",
    "doi:",
    "pmid",
)


def looks_like_noise(name: str) -> bool:
    n = name.strip().lower()
    if not n:
        return True
    if len(n) < 2 or len(n) > 200:
        return True
    # purely punctuation/numeric junk
    if not re.search(r"[A-Za-z]", n):
        return True
    # sentence fragments tend to have many spaces and stopwords
    for t in NOISE_TOKENS:
        if t in n:
            return True
    # reject common list page phrases
    if n in {"nan", "none", "edit", "v t e", "show", "hide"}:
        return True
    # sentence-like: starts with lowercase conjunction/article and has many words
    if n.startswith(("the ", "a ", "an ", "and ", "or ", "this ", "these ", "those ", "for ")) and len(n.split()) > 4:
        return True
    return False


def extract_from_tables(
    title: str, html: str, source_url: str, now_iso: str
) -> list[CompoundRow]:
    rows: list[CompoundRow] = []
    try:
        tables = pd.read_html(io.StringIO(html))
    except ValueError:
        tables = []
    except Exception as e:
        log.warning("read_html failed for %s: %s", title, e)
        tables = []

    soup = BeautifulSoup(html, "html.parser")
    # Also map each table to its nearest heading (in DOM order)
    dom_tables = soup.select("table.wikitable")
    heading_for = []
    for t in dom_tables:
        heading_for.append(nearest_heading(t))

    for i, df in enumerate(tables):
        # Flatten potentially multi-index columns
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                " ".join([str(x) for x in c if x and "Unnamed" not in str(x)]).strip()
                for c in df.columns
            ]
        df.columns = [str(c).strip() for c in df.columns]

        # Heuristic: find a column that looks like compound name
        name_col = None
        for c in df.columns:
            cl = c.lower()
            if any(
                k in cl
                for k in (
                    "name",
                    "compound",
                    "substance",
                    "drug",
                    "chemical",
                    "agent",
                    "precursor",
                )
            ):
                name_col = c
                break
        if name_col is None and len(df.columns) > 0:
            # fall back to first column
            name_col = df.columns[0]

        alias_col = None
        for c in df.columns:
            cl = c.lower()
            if any(k in cl for k in ("trade", "alias", "other name", "synonym", "iupac")):
                alias_col = c
                break

        cas_col = None
        for c in df.columns:
            if "cas" in c.lower():
                cas_col = c
                break

        sched_col = None
        for c in df.columns:
            cl = c.lower()
            if any(k in cl for k in ("schedule", "class", "category", "schedule/class")):
                sched_col = c
                break

        heading = heading_for[i] if i < len(heading_for) else ""

        for _, row in df.iterrows():
            try:
                raw_name = row.get(name_col, "") if name_col else ""
            except Exception:
                raw_name = ""
            name = clean_name(str(raw_name)) if pd.notna(raw_name) else ""
            if looks_like_noise(name):
                continue

            alias = ""
            if alias_col:
                av = row.get(alias_col, "")
                alias = clean_name(str(av)) if pd.notna(av) else ""

            cas_src = ""
            if cas_col:
                cs = row.get(cas_col, "")
                cas_src = str(cs) if pd.notna(cs) else ""
            # Fall back to whole-row join for CAS
            full_row_text = " | ".join(
                str(v) for v in row.values if pd.notna(v)
            )
            cas = extract_cas(cas_src) or extract_cas(full_row_text)
            cas_str = ";".join(cas)

            sched = ""
            if sched_col:
                sv = row.get(sched_col, "")
                sched = clean_name(str(sv)) if pd.notna(sv) else ""
            if not sched:
                # try to infer from page title
                m = re.search(r"Schedule_(I{1,3}V?|V|IV)", title)
                if m:
                    sched = f"Schedule {m.group(1)}"

            rows.append(
                CompoundRow(
                    page_title=title,
                    name=name,
                    cas_numbers=cas_str,
                    aliases=alias,
                    schedule_or_category=sched,
                    row_section=heading,
                    raw_text=full_row_text[:1000],
                    source_url=source_url,
                    fetched_at=now_iso,
                )
            )
    return rows


def extract_from_lists(
    title: str, html: str, source_url: str, now_iso: str
) -> list[CompoundRow]:
    rows: list[CompoundRow] = []
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one("div.mw-parser-output") or soup
    # Identify bullet list items that belong under content headings
    # Walk all <li> inside <ul> under h2/h3 sections in main content
    for ul in content.find_all("ul"):
        # skip navboxes, toc, references
        if ul.find_parent(["div"], class_=re.compile(r"(navbox|toc|reference|reflist|hatnote|shortdescription)")) is not None:
            continue
        if ul.find_parent("table") is not None:
            continue
        heading = nearest_heading(ul)
        if not heading:
            continue
        # Filter out irrelevant sections
        h_low = heading.lower()
        if any(
            kw in h_low
            for kw in (
                "references",
                "external links",
                "further reading",
                "see also",
                "notes",
                "bibliography",
                "citations",
            )
        ):
            continue
        for li in ul.find_all("li", recursive=False):
            text = clean_name(li.get_text(" ", strip=True))
            if not text or len(text) < 2 or len(text) > 400:
                continue
            # Heuristic: skip pure sentence-looking entries (too many words + no capitalized compound-like token)
            # Keep first line / main name token if long
            first = text.split(" – ")[0].split(" — ")[0].split(":")[0].split("(")[0].strip()
            name = first if 1 <= len(first) <= 200 else text[:200]
            if looks_like_noise(name):
                continue
            cas = extract_cas(text)
            sched = ""
            m = re.search(r"Schedule\s+(I{1,3}V?|V|IV)", title.replace("_", " "))
            if m:
                sched = f"Schedule {m.group(1)}"
            rows.append(
                CompoundRow(
                    page_title=title,
                    name=name,
                    cas_numbers=";".join(cas),
                    aliases="",
                    schedule_or_category=sched,
                    row_section=heading,
                    raw_text=text[:1000],
                    source_url=source_url,
                    fetched_at=now_iso,
                )
            )
    return rows


def dedupe(rows: list[CompoundRow]) -> list[CompoundRow]:
    seen: set[tuple[str, str]] = set()
    out: list[CompoundRow] = []
    for r in rows:
        key = (r.page_title.lower().strip(), r.name.lower().strip())
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def run() -> None:
    now = datetime.now(timezone.utc).isoformat()
    all_compounds: list[CompoundRow] = []
    page_rows: list[PageRow] = []

    for title in PAGES:
        src_url = HTML_BASE + title
        try:
            html, wikitext = fetch_page(title)
        except Exception as e:
            log.error("FAILED fetch %s: %s", title, e)
            page_rows.append(
                PageRow(
                    page_title=title,
                    url=src_url,
                    extracted_at=now,
                    html_path="",
                    wikitext_path="",
                    row_count=-1,
                )
            )
            continue

        table_rows = extract_from_tables(title, html, src_url, now)
        list_rows: list[CompoundRow] = []
        # Only mine <li> lists when tables produced few rows (these pages are list-y)
        if len(table_rows) < 5 or any(
            k in title
            for k in (
                "List_of_fentanyl_analogues",
                "List_of_designer_drugs",
                "List_of_chemical_warfare_agents",
                "List_of_investigational_dissociative_drugs",
                "List_of_psychoactive_plants",
                "Drug_precursor",
                "Australia_Group",
                "NFPA_704",
            )
        ):
            list_rows = extract_from_lists(title, html, src_url, now)

        combined = dedupe(table_rows + list_rows)
        log.info(
            "%s -> tables=%d lists=%d deduped=%d",
            title,
            len(table_rows),
            len(list_rows),
            len(combined),
        )
        all_compounds.extend(combined)
        page_rows.append(
            PageRow(
                page_title=title,
                url=src_url,
                extracted_at=now,
                html_path=str((RAW_DIR / f"{safe_name(title)}.html").resolve()),
                wikitext_path=str((RAW_DIR / f"{safe_name(title)}.wikitext").resolve()),
                row_count=len(combined),
            )
        )

    # Global dedupe across pages? Keep per (page,name) — already deduped per page.
    compounds_df = pd.DataFrame([asdict(r) for r in all_compounds])
    pages_df = pd.DataFrame([asdict(r) for r in page_rows])

    # Add incremental id
    if not compounds_df.empty:
        compounds_df.insert(0, "id", range(1, len(compounds_df) + 1))
    else:
        compounds_df["id"] = []
    if not pages_df.empty:
        pages_df.insert(0, "id", range(1, len(pages_df) + 1))
    else:
        pages_df["id"] = []

    compounds_csv = CSV_DIR / "wikipedia_compounds.csv"
    pages_csv = CSV_DIR / "wikipedia_pages.csv"
    compounds_df.to_csv(compounds_csv, index=False)
    pages_df.to_csv(pages_csv, index=False)
    log.info("csv: %s (%d), %s (%d)", compounds_csv, len(compounds_df), pages_csv, len(pages_df))

    # SQLite
    if DB_PATH.exists():
        DB_PATH.unlink()
    with sqlite3.connect(DB_PATH) as conn:
        compounds_df.to_sql("wikipedia_compounds", conn, index=False)
        pages_df.to_sql("wikipedia_pages", conn, index=False)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wc_page ON wikipedia_compounds(page_title)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wc_name ON wikipedia_compounds(name)"
        )
        conn.commit()
    log.info("sqlite written %s", DB_PATH)

    # Summary
    print("---- SUMMARY ----")
    print(f"wikipedia_compounds rows: {len(compounds_df)}")
    print(f"wikipedia_pages rows: {len(pages_df)}")
    if not compounds_df.empty:
        top = compounds_df.groupby("page_title").size().sort_values(ascending=False).head(5)
        print("top 5 pages by compound count:")
        for k, v in top.items():
            print(f"  {k}: {v}")
        print("sample 5 compounds:")
        sample = compounds_df.sample(min(5, len(compounds_df)), random_state=42)
        for _, r in sample.iterrows():
            print(f"  - [{r['page_title']}] {r['name']} CAS={r['cas_numbers']} sched={r['schedule_or_category']}")
    failed = pages_df[pages_df["row_count"] < 0]
    if not failed.empty:
        print("failed pages:")
        for _, r in failed.iterrows():
            print(f"  - {r['page_title']}")


if __name__ == "__main__":
    run()
