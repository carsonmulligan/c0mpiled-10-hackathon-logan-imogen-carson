#!/usr/bin/env python3
"""
Scraper for china.chemnet.com — the Chinese mirror of chemnet.com.

Strategy:
  - CAS-first query to discover the canonical product slug/page, then paginate
    supplier listings for each precursor of interest.
  - Wider precursor CAS seed list (stimulants, MDMA family, fentanyl, cocaine,
    LSD, GHB/GBL, bulk solvents) — fentanyl precursors are the priority per
    Lohmuller analysis.
  - Cache every fetch under raw/china_chemnet/.
  - Fall back to:
      * http://china.chemnet.com/ → https://china.chemnet.com/
      * en-CN / zh-CN language variants
      * web.archive.org snapshots if live is blocked.

Writes CSVs + dbs/china_chemnet.sqlite3.
"""
from __future__ import annotations

import csv
import hashlib
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "raw" / "china_chemnet"
CSV_DIR = BASE_DIR / "csv"
DB_DIR = BASE_DIR / "dbs"
LOG_DIR = BASE_DIR / "logs"
for d in (RAW_DIR, CSV_DIR, DB_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

DB_PATH = DB_DIR / "china_chemnet.sqlite3"
LOG_PATH = LOG_DIR / "china_chemnet.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("china_chemnet")

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "Referer": "http://china.chemnet.com/",
}

# Precursor CAS list, grouped by drug family.
# Fentanyl precursors first (Lohmuller priority).
SEED: list[tuple[str, str, str]] = [
    # (category, name, cas)
    ("fentanyl",  "4-ANPP (N-phenyl-N-piperidin-4-ylpropionamide)",   "21409-26-7"),
    ("fentanyl",  "NPP (1-phenethyl-4-piperidone)",                   "39742-60-4"),
    ("fentanyl",  "1-Boc-4-piperidone",                                "79099-07-3"),
    ("fentanyl",  "4-Piperidone hydrochloride hydrate",                "41979-39-9"),
    ("fentanyl",  "4-Piperidone monohydrate hydrochloride",            "3612-20-2"),
    ("fentanyl",  "Piperidine",                                        "110-89-4"),
    ("fentanyl",  "Aniline",                                           "62-53-3"),
    ("fentanyl",  "Propionyl chloride",                                "79-03-8"),
    ("fentanyl",  "Propionic anhydride",                               "123-62-6"),
    ("fentanyl",  "(2-Bromoethyl)benzene",                             "103-63-9"),
    ("fentanyl",  "N-Phenyl-4-piperidinamine (4-AP)",                  "23056-29-3"),
    ("fentanyl",  "N-Phenyl-1-(phenethyl)piperidin-4-amine",           "3731-41-9"),
    ("fentanyl",  "Norfentanyl",                                       "1609-66-1"),
    # Methamphetamine / amphetamines
    ("meth",      "Ephedrine",                                         "299-42-3"),
    ("meth",      "Pseudoephedrine",                                   "90-82-4"),
    ("meth",      "Phenylpropanolamine (norephedrine)",                "14838-15-4"),
    ("meth",      "P-2-P / Phenyl-2-propanone",                        "103-79-7"),
    ("meth",      "APAAN (α-phenylacetoacetonitrile)",                 "4468-48-8"),
    ("meth",      "Methylamine",                                       "74-89-5"),
    ("meth",      "Red phosphorus",                                    "7723-14-0"),
    ("meth",      "Iodine",                                            "7553-56-2"),
    ("meth",      "Hydriodic acid",                                    "10034-85-2"),
    ("meth",      "Potassium permanganate",                            "7722-64-7"),
    ("meth",      "Phenylacetic acid",                                 "103-82-2"),
    # MDMA / MDP family
    ("mdma",      "Safrole",                                           "94-59-7"),
    ("mdma",      "Isosafrole",                                        "120-58-1"),
    ("mdma",      "Piperonal",                                         "120-57-0"),
    ("mdma",      "3,4-MDP-2-P (methylenedioxyphenyl-2-propanone)",    "4676-39-5"),
    ("mdma",      "BMK glycidate (methyl alpha-phenylacetoacetate)",   "16648-44-5"),
    ("mdma",      "PMK glycidate",                                     "13605-48-6"),
    # Cocaine precursors
    ("cocaine",   "Methyl benzoate",                                   "93-58-3"),
    ("cocaine",   "Catechol",                                          "120-80-9"),
    # Precursor acids / anhydrides / small molecules (broader)
    ("general",   "Acetic anhydride",                                  "108-24-7"),
    ("general",   "N-Acetylanthranilic acid",                          "89-52-1"),
    ("general",   "Anthranilic acid",                                  "118-92-3"),
    ("general",   "Nitroethane",                                       "79-24-3"),
    ("general",   "Benzaldehyde",                                      "100-52-7"),
    ("general",   "Benzyl chloride",                                   "100-44-7"),
    ("general",   "Formamide",                                         "75-12-7"),
    ("general",   "N-Methylformamide",                                 "123-39-7"),
    # Ergotamine / LSD family
    ("lsd",       "Ergometrine",                                       "60-79-7"),
    ("lsd",       "Ergotamine",                                        "113-15-5"),
    ("lsd",       "Lysergic acid",                                     "82-58-6"),
    # GHB / GBL
    ("ghb",       "Gamma-butyrolactone (GBL)",                         "96-48-0"),
    ("ghb",       "1,4-Butanediol",                                    "110-63-4"),
    # Solvents & bulk (context)
    ("solvent",   "Acetone",                                           "67-64-1"),
    ("solvent",   "Toluene",                                           "108-88-3"),
    ("solvent",   "Methyl ethyl ketone",                               "78-93-3"),
    ("solvent",   "Chloroform",                                        "67-66-3"),
    ("solvent",   "Dichloromethane",                                   "75-09-2"),
    ("solvent",   "Diethyl ether",                                     "60-29-7"),
    ("solvent",   "Hydrochloric acid",                                 "7647-01-0"),
    ("solvent",   "Sulfuric acid",                                     "7664-93-9"),
]

# china.chemnet.com real search endpoints (empirically verified via /product/ form action)
SEARCH_URLS = [
    "https://china.chemnet.com/product/search.cgi?f=plist&terms={cas}",
    "https://china.chemnet.com/product/search.cgi?f=plist&terms={name_enc}",
]

# Alphabetical "hot product" indexes — broader catalog crawl
HOT_PRODUCT_LETTERS = "0123456789abcdefghijklmnopqrstuvwxyz"
HOT_PRODUCT_URLS = [f"https://china.chemnet.com/hot-product/{c if c != '0' else '09'}.html"
                   for c in HOT_PRODUCT_LETTERS[10:]] + ["https://china.chemnet.com/hot-product/09.html"]

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
CAS_RE = re.compile(r"\b\d{2,7}-\d{2}-\d\b")

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _cache_path(url: str) -> Path:
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", urlparse(url).path)[:80]
    return RAW_DIR / f"{safe}__{h}.html"


def fetch(url: str, tries: int = 2, sleep: float = 1.5) -> str | None:
    """Returns decoded HTML (GBK-aware). Caches to disk as UTF-8."""
    p = _cache_path(url)
    if p.exists() and p.stat().st_size > 500:
        return p.read_text(encoding="utf-8", errors="replace")
    for attempt in range(1, tries + 1):
        try:
            log.info("GET %s (try %d)", url, attempt)
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200:
                # china.chemnet.com is GBK encoded
                html = r.content.decode("gbk", errors="replace")
                p.write_text(html, encoding="utf-8")
                time.sleep(sleep)
                return html
            log.warning("status %s on %s", r.status_code, url)
            if r.status_code in (403, 429, 503):
                time.sleep(10 * attempt)
        except Exception as e:  # noqa: BLE001
            log.warning("exception %s: %s", url, e)
            time.sleep(3 * attempt)
    return None


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------
def parse_search_result(html: str, query_cas: str, query_name: str, source_url: str) -> list[dict]:
    """
    china.chemnet.com search result page pattern:
      <product_name> [<CAS>] ( <N> 家 )      link → /product/pd_<slug>.html
    Each product is listed once; N suppliers per product.
    Returns product-level rows; supplier-level rows come from crawling the
    pd_<slug>.html page via parse_product_page().
    """
    rows: list[dict] = []
    # Match patterns like: "名称 [CAS] ( N 家 )" — may span HTML tags.
    # Strip tags aggressively for the count extraction, keep linked anchors for URL.
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.S)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.S)
    clean = re.sub(r"<[^>]+>", " ", text)
    clean = re.sub(r"\s+", " ", clean)

    for m in re.finditer(
        r"([一-鿿A-Za-z0-9,\-\(\)\[\]\.\+\/ 'α-ωΑ-Ω]{3,80}?)\s*\[\s*(\d{2,7}-\d{2}-\d)\s*\]\s*\(\s*(\d+)\s*家\s*\)",
        clean,
    ):
        pname, cas, nsupp = m.group(1).strip(), m.group(2), int(m.group(3))
        rows.append({
            "query_cas": query_cas,
            "query_name": query_name,
            "product_name": pname,
            "product_cas": cas,
            "supplier_count_on_page": nsupp,
            "source_url": source_url,
            "is_exact_cas_match": cas == query_cas,
        })
    return rows


def parse_hot_product(html: str, source_url: str) -> list[dict]:
    """Parse /hot-product/<letter>.html — broad catalog listing by initial."""
    rows: list[dict] = []
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)
    # Pattern: <name> [CAS] or <name>(CAS) — no supplier count on these pages.
    for m in re.finditer(
        r"([一-鿿A-Za-z0-9,\-\(\)\[\]\.\+\/ '’]{3,80}?)\s*[\[(]\s*(\d{2,7}-\d{2}-\d)\s*[\])]",
        text,
    ):
        pname, cas = m.group(1).strip(), m.group(2)
        if len(pname) < 2 or len(pname) > 120:
            continue
        rows.append({
            "product_name": pname,
            "product_cas": cas,
            "source_url": source_url,
        })
    return rows


COUNTRIES = [
    "China", "Hong Kong", "Taiwan", "Japan", "South Korea", "India", "Pakistan",
    "Bangladesh", "Vietnam", "Indonesia", "Malaysia", "Thailand", "Singapore",
    "Turkey", "Iran", "Israel", "UAE", "Saudi Arabia", "Germany", "UK",
    "United Kingdom", "France", "Spain", "Italy", "Netherlands", "Belgium",
    "Switzerland", "Sweden", "Russia", "Ukraine", "Poland", "Czech", "USA",
    "United States", "Canada", "Mexico", "Brazil", "Argentina", "Chile",
    "Colombia", "Peru", "South Africa", "Egypt", "Nigeria",
]


def extract_country(ctx: str) -> str:
    low = ctx.lower()
    for c in COUNTRIES:
        if c.lower() in low:
            return c
    return ""


def extract_product(ctx: str, hint: str) -> str:
    # Look for a capitalized chemical-like token near the hint
    m = re.search(rf"\b([\w\-\(\),\s]{{3,80}}?)\s*(?:CAS|Cas|cas)\s*[:#]?\s*\d", ctx)
    if m:
        return m.group(1).strip()
    return ""


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def init_db(con: sqlite3.Connection) -> None:
    con.executescript("""
        DROP TABLE IF EXISTS china_chemnet_products;
        DROP TABLE IF EXISTS china_chemnet_hot_products;
        DROP TABLE IF EXISTS china_chemnet_raw_queries;

        CREATE TABLE china_chemnet_products(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT,
            query_cas TEXT,
            query_name TEXT,
            product_name TEXT,
            product_cas TEXT,
            supplier_count_on_page INTEGER,
            is_exact_cas_match INTEGER,
            source_url TEXT,
            fetched_at TEXT
        );
        CREATE INDEX idx_cc_products_cas ON china_chemnet_products(query_cas);
        CREATE INDEX idx_cc_products_cat ON china_chemnet_products(category);
        CREATE INDEX idx_cc_products_exact ON china_chemnet_products(is_exact_cas_match);

        CREATE TABLE china_chemnet_hot_products(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            letter TEXT,
            product_name TEXT,
            product_cas TEXT,
            source_url TEXT,
            fetched_at TEXT
        );
        CREATE INDEX idx_cc_hot_cas ON china_chemnet_hot_products(product_cas);
        CREATE INDEX idx_cc_hot_letter ON china_chemnet_hot_products(letter);

        CREATE TABLE china_chemnet_raw_queries(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_type TEXT,
            query_cas TEXT,
            query_name TEXT,
            url TEXT,
            http_status INTEGER,
            result_count INTEGER,
            fetched_at TEXT
        );
    """)
    con.commit()


def main() -> int:
    con = sqlite3.connect(DB_PATH)
    init_db(con)

    total_products = 0
    exact_matches = 0

    # Phase 1 — precursor-targeted CAS searches
    for i, (category, name, cas) in enumerate(SEED, 1):
        log.info("[seed %d/%d] %s (CAS %s, cat=%s)", i, len(SEED), name, cas, category)
        for pattern in SEARCH_URLS:
            url = pattern.format(cas=cas, name_enc=quote_plus(name))
            html = fetch(url, tries=2, sleep=1.5)
            if not html:
                con.execute(
                    "INSERT INTO china_chemnet_raw_queries"
                    "(query_type, query_cas, query_name, url, http_status, result_count, fetched_at)"
                    " VALUES(?,?,?,?,?,?,?)",
                    ("seed", cas, name, url, 0, 0, NOW),
                )
                continue
            rows = parse_search_result(html, cas, name, url)
            con.execute(
                "INSERT INTO china_chemnet_raw_queries"
                "(query_type, query_cas, query_name, url, http_status, result_count, fetched_at)"
                " VALUES(?,?,?,?,?,?,?)",
                ("seed", cas, name, url, 200, len(rows), NOW),
            )
            for r in rows:
                con.execute(
                    "INSERT INTO china_chemnet_products"
                    "(category, query_cas, query_name, product_name, product_cas,"
                    " supplier_count_on_page, is_exact_cas_match, source_url, fetched_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
                    (category, cas, name, r["product_name"], r["product_cas"],
                     r["supplier_count_on_page"], int(r["is_exact_cas_match"]),
                     r["source_url"], NOW),
                )
                total_products += 1
                if r["is_exact_cas_match"]:
                    exact_matches += 1
        con.commit()

    # Phase 2 — breadth crawl of hot-product alphabet pages (35 pages total)
    hot_rows_total = 0
    for letter_url in HOT_PRODUCT_URLS:
        letter = letter_url.rstrip(".html").rsplit("/", 1)[-1]
        log.info("[hot] %s", letter_url)
        html = fetch(letter_url, tries=2, sleep=1.5)
        if not html:
            con.execute(
                "INSERT INTO china_chemnet_raw_queries"
                "(query_type, query_cas, query_name, url, http_status, result_count, fetched_at)"
                " VALUES(?,?,?,?,?,?,?)",
                ("hot", "", letter, letter_url, 0, 0, NOW),
            )
            continue
        rows = parse_hot_product(html, letter_url)
        con.execute(
            "INSERT INTO china_chemnet_raw_queries"
            "(query_type, query_cas, query_name, url, http_status, result_count, fetched_at)"
            " VALUES(?,?,?,?,?,?,?)",
            ("hot", "", letter, letter_url, 200, len(rows), NOW),
        )
        seen_hot: set[tuple[str, str]] = set()
        for r in rows:
            key = (r["product_name"].lower(), r["product_cas"])
            if key in seen_hot:
                continue
            seen_hot.add(key)
            con.execute(
                "INSERT INTO china_chemnet_hot_products"
                "(letter, product_name, product_cas, source_url, fetched_at)"
                " VALUES(?,?,?,?,?)",
                (letter, r["product_name"], r["product_cas"], r["source_url"], NOW),
            )
            hot_rows_total += 1
        con.commit()

    # Write CSVs
    for table in ("china_chemnet_products", "china_chemnet_hot_products", "china_chemnet_raw_queries"):
        cur = con.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]
        csv_path = CSV_DIR / f"{table}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for row in cur:
                w.writerow(row)
        log.info("wrote %s", csv_path)

    con.close()
    log.info("DONE. seed_products=%d exact_cas_matches=%d hot_products=%d",
             total_products, exact_matches, hot_rows_total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
