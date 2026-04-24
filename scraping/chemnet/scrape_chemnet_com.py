#!/usr/bin/env python3
"""
Scraper for chemnet.com B2B chemical supplier directory.

Strategy:
  1. For each seed chemical (CAS + name), hit the CAS lookup page to discover the
     canonical product slug and a small supplier list.
  2. Hit the Global/Products/<slug>/Suppliers-0-<page>.html pages to paginate through
     the full supplier listing for each chemical. These pages carry supplier name,
     address/country, email, telephone, fax, and cross-product links.
  3. Parse each supplier block into a product row (one per chemical-supplier match)
     plus populate a supplier table.
  4. Also crawl "Other products" links embedded in each supplier block to pick up
     broader chemical listings beyond the 40 seed chemicals (product cross-section).
  5. Cache every HTML fetch to raw/chemnet_com/.
  6. Fallbacks if blocked: back off 30s, retry once; otherwise archive.org; otherwise
     china.chemnet.com mirror.

Writes CSVs and a SQLite DB under data/chemnet/.
"""

from __future__ import annotations

import csv
import hashlib
import logging
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path("/Users/carsonmulligan/Desktop/Workspace/apps/web-rails-holocron-gov-hackathon/data/chemnet")
RAW_DIR = BASE_DIR / "raw" / "chemnet_com"
CSV_DIR = BASE_DIR / "csv"
DB_DIR = BASE_DIR / "dbs"
LOG_DIR = BASE_DIR / "logs"

for d in (RAW_DIR, CSV_DIR, DB_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

LOG_PATH = LOG_DIR / "chemnet_com.log"
DB_PATH = DB_DIR / "chemnet_com.sqlite3"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("chemnet")

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": UA,
    "Referer": "http://www.chemnet.com/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

SEED = [
    ("Acetic anhydride", "108-24-7"),
    ("Acetone", "67-64-1"),
    ("Ephedrine", "299-42-3"),
    ("Pseudoephedrine", "90-82-4"),
    ("Phenylpropanolamine", "14838-15-4"),
    ("Ergometrine", "60-79-7"),
    ("Ergotamine", "113-15-5"),
    ("Lysergic acid", "82-58-6"),
    ("N-acetylanthranilic acid", "89-52-1"),
    ("Anthranilic acid", "118-92-3"),
    ("Phenylacetic acid", "103-82-2"),
    ("Piperonal", "120-57-0"),
    ("Safrole", "94-59-7"),
    ("Isosafrole", "120-58-1"),
    ("3,4-MDP-2-P", "4676-39-5"),
    ("Phenyl-2-propanone", "103-79-7"),
    ("APAAN", "4468-48-8"),
    ("BMK glycidate", "16648-44-5"),
    ("Methylamine", "74-89-5"),
    ("Red phosphorus", "7723-14-0"),
    ("Iodine", "7553-56-2"),
    ("Hydriodic acid", "10034-85-2"),
    ("Toluene", "108-88-3"),
    ("Diethyl ether", "60-29-7"),
    ("Hydrochloric acid", "7647-01-0"),
    ("Sulfuric acid", "7664-93-9"),
    ("Potassium permanganate", "7722-64-7"),
    ("Sodium permanganate", "10101-50-5"),
    ("Sodium hypochlorite", "7681-52-9"),
    ("Palladium chloride", "7647-10-1"),
    ("Chloroform", "67-66-3"),
    ("Dichloromethane", "75-09-2"),
    ("N-methylformamide", "123-39-7"),
    ("Formamide", "75-12-7"),
    ("Benzaldehyde", "100-52-7"),
    ("Nitroethane", "79-24-3"),
    ("Hydrogen peroxide", "7722-84-1"),
    ("Benzyl chloride", "100-44-7"),
    ("Methyl ethyl ketone", "78-93-3"),
    ("Gamma-butyrolactone", "96-48-0"),
]

REQUEST_INTERVAL_SEC = 2.0
TIMEOUT_SEC = 20
MAX_PAGINATION_PAGES = 20  # safety cap per chemical
MAX_OTHER_PRODUCTS_TO_FOLLOW_PER_SUPPLIER = 3  # small fan-out for breadth
OTHER_PRODUCT_BUDGET = 80  # total extra product pages to fetch for breadth


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

_session = requests.Session()
_session.headers.update(HEADERS)
_last_fetch_ts = 0.0


def _throttle():
    global _last_fetch_ts
    delta = time.time() - _last_fetch_ts
    if delta < REQUEST_INTERVAL_SEC:
        time.sleep(REQUEST_INTERVAL_SEC - delta)
    _last_fetch_ts = time.time()


def _cache_path_for(url: str) -> Path:
    # Deterministic hashed filename preserving a readable tail.
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    tail = re.sub(r"[^A-Za-z0-9._-]+", "_", url.split("?", 1)[-1])[:80]
    path_part = re.sub(r"[^A-Za-z0-9._-]+", "_", urlparse(url).path)[:80]
    return RAW_DIR / f"{path_part}__{tail}__{h}.html"


def fetch(url: str, *, cache_key: str | None = None) -> tuple[str, Path] | tuple[None, None]:
    """Fetch URL with on-disk cache.

    Returns (html_text, cache_path) or (None, None) on hard failure.
    """
    if cache_key:
        cache_file = RAW_DIR / cache_key
    else:
        cache_file = _cache_path_for(url)

    if cache_file.exists() and cache_file.stat().st_size > 500:
        log.debug("cache hit %s", cache_file.name)
        try:
            return cache_file.read_text(encoding="utf-8", errors="replace"), cache_file
        except Exception as e:
            log.warning("cache read failed %s: %s", cache_file, e)

    for attempt in (1, 2):
        _throttle()
        try:
            resp = _session.get(url, timeout=TIMEOUT_SEC, allow_redirects=True)
        except requests.RequestException as e:
            log.warning("fetch error %s (attempt %d): %s", url, attempt, e)
            if attempt == 2:
                return None, None
            time.sleep(5)
            continue

        if resp.status_code in (403, 429, 503):
            log.warning("blocked status %d on %s — backing off 30s", resp.status_code, url)
            time.sleep(30)
            if attempt == 2:
                return None, None
            continue

        if resp.status_code != 200:
            log.warning("status %d on %s", resp.status_code, url)
            return None, None

        text = resp.text
        try:
            cache_file.write_text(text, encoding="utf-8")
        except Exception as e:
            log.warning("cache write failed %s: %s", cache_file, e)
        return text, cache_file

    return None, None


def fetch_with_fallback(url: str) -> tuple[str, Path, str] | tuple[None, None, None]:
    """Try chemnet.com; fall back to china.chemnet.com mirror; then archive.org."""
    text, path = fetch(url)
    if text:
        return text, path, "chemnet.com"

    # china mirror swap
    mirror = url.replace("://www.chemnet.com", "://china.chemnet.com").replace(
        "://chemnet.com", "://china.chemnet.com"
    )
    if mirror != url:
        text, path = fetch(mirror)
        if text:
            return text, path, "china.chemnet.com"

    # archive.org wayback
    way = f"https://web.archive.org/web/2024/{url}"
    text, path = fetch(way)
    if text:
        return text, path, "web.archive.org"

    return None, None, None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


@dataclass
class Supplier:
    supplier_id: str
    supplier_name: str
    country: str = ""
    city: str = ""
    address: str = ""
    email: str = ""
    telephone: str = ""
    fax: str = ""
    website: str = ""
    description: str = ""
    chemnet_url: str = ""
    fetched_at: str = ""


@dataclass
class ProductRow:
    query_chemical: str
    query_cas: str
    product_name: str
    product_cas: str
    supplier_id: str
    supplier_name: str
    supplier_country: str
    purity: str = ""
    grade: str = ""
    min_order: str = ""
    price: str = ""
    product_url: str = ""
    fetched_at: str = ""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def extract_slug_from_cas_page(html: str) -> str | None:
    """Pull the Global/Products/<slug>/Suppliers link out of a CAS page."""
    m = re.search(r"/Global/Products/([^/\"']+)/Suppliers-0-0\.html", html)
    if m:
        return m.group(1)
    return None


def extract_inline_suppliers_from_cas_page(html: str) -> list[tuple[str, str, str, str]]:
    """Return (supplier_id, country, product_slug_id, supplier_name) tuples from the
    small jhg listing on a CAS info page."""
    soup = BeautifulSoup(html, "lxml")
    out = []
    for a in soup.select("div.jhg a"):
        href = a.get("href", "")
        m = re.search(r"/([A-Za-z-]+)Suppliers/(\d+)/([^\"']+?)--(\d+)\.html", href)
        if not m:
            continue
        country = m.group(1).replace("-", " ").strip()
        sid = m.group(2)
        slug = m.group(3)
        prod_id = m.group(4)
        name = _clean(a.get_text())
        if name and sid:
            out.append((sid, country, slug, name, prod_id, href))
    return out


SUPPLIER_BLOCK_RE = re.compile(
    r"<p>\s*<input type=\"checkbox\" name=\"selectbox\" value=\"(?P<sid>\d+)\"\s*>\s*"
    r"<a href=\"(?P<prod_href>[^\"]+)\"[^>]*class=\"blue u fb\"[^>]*>"
    r"(?P<name>.*?)</a>",
    re.DOTALL,
)


def parse_supplier_listing_page(
    html: str,
    *,
    query_chemical: str,
    query_cas: str,
) -> tuple[list[Supplier], list[ProductRow], int, int]:
    """Return (suppliers, product_rows, total_suppliers, max_page_index)."""
    soup = BeautifulSoup(html, "lxml")

    # Total count (e.g., "Total 17 Suppliers")
    total = 0
    m = re.search(r"Total\s*<font[^>]*>(\d+)</font>\s*Suppliers", html)
    if m:
        total = int(m.group(1))

    # Max pagination index we can see in the page links
    max_page = 0
    for pm in re.finditer(r"Suppliers-0-(\d+)\.html", html):
        try:
            n = int(pm.group(1))
            if n > max_page:
                max_page = n
        except Exception:
            pass

    suppliers: list[Supplier] = []
    products: list[ProductRow] = []

    # Each supplier block starts with <p><input ... value="<sid>"> and ends at </table>
    # We split on the opening <p><input ...> marker and process each segment.
    parts = re.split(
        r'(?=<p>\s*<input type="checkbox" name="selectbox" value="\d+")',
        html,
    )
    for part in parts[1:]:
        head = SUPPLIER_BLOCK_RE.search(part)
        if not head:
            continue
        sid = head.group("sid")
        prod_href = head.group("prod_href")
        raw_name = head.group("name")
        supplier_name = _clean(BeautifulSoup(raw_name, "lxml").get_text())

        # Product slug + country
        pm = re.search(r"/([A-Za-z-]+)Suppliers/(\d+)/([^/\"']+?)--(\d+)\.html", prod_href)
        country = ""
        product_slug = ""
        product_id = ""
        if pm:
            country = pm.group(1).replace("-", " ").strip()
            product_slug = pm.group(3)
            product_id = pm.group(4)

        # Limit to the current block end — up to the next supplier block or a clear
        # terminator like </form>, </table><br>, or the pagination marker.
        block = part.split("</form>")[0]

        # Parse the contact table (first <table> in block)
        blk_soup = BeautifulSoup(block, "lxml")
        address = email = telephone = fax = website = ""
        for tr in blk_soup.select("table tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            label = _clean(tds[0].get_text()).rstrip(":").lower()
            value = _clean(tds[1].get_text())
            if "address" in label and not address:
                address = value
            elif label == "email" and not email:
                email = value
            elif "telephone" in label and not telephone:
                telephone = value
            elif label == "fax" and not fax:
                fax = value
            elif "web site" in label and not website:
                # site is often obfuscated (white-font inside font tag). We keep the
                # visible text minus the obfuscation.
                website = value

        product_name = re.sub(r"-", " ", product_slug).strip() if product_slug else ""
        product_url = urljoin("https://www.chemnet.com/", prod_href)
        chemnet_supplier_url = urljoin(
            "https://www.chemnet.com/",
            f"/{country.replace(' ', '-')}Suppliers/{sid}/Products-Catalog--1.html"
            if country
            else f"/Suppliers/{sid}.html",
        )

        fetched = now_iso()
        suppliers.append(
            Supplier(
                supplier_id=sid,
                supplier_name=supplier_name,
                country=country,
                address=address,
                email=email,
                telephone=telephone,
                fax=fax,
                website=website,
                chemnet_url=chemnet_supplier_url,
                fetched_at=fetched,
            )
        )
        products.append(
            ProductRow(
                query_chemical=query_chemical,
                query_cas=query_cas,
                product_name=product_name or query_chemical,
                product_cas=query_cas,
                supplier_id=sid,
                supplier_name=supplier_name,
                supplier_country=country,
                product_url=product_url,
                fetched_at=fetched,
            )
        )

        # Also pick up "Other products" cross-product rows for broader coverage.
        for other in blk_soup.select("td.other_pro a.blues"):
            h = other.get("href", "")
            om = re.search(
                r"/([A-Za-z-]+)Suppliers/(\d+)/([^/\"']+?)--(\d+)\.html", h
            )
            if not om:
                continue
            o_slug = om.group(3)
            products.append(
                ProductRow(
                    query_chemical=query_chemical,
                    query_cas=query_cas,
                    product_name=_clean(other.get_text()) or o_slug.replace("-", " "),
                    product_cas="",  # unknown without extra fetch
                    supplier_id=sid,
                    supplier_name=supplier_name,
                    supplier_country=country,
                    product_url=urljoin("https://www.chemnet.com/", h),
                    fetched_at=fetched,
                )
            )

    return suppliers, products, total, max_page


# ---------------------------------------------------------------------------
# SQLite / CSV output
# ---------------------------------------------------------------------------


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS chemnet_raw_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_chemical TEXT,
            query_cas TEXT,
            query_url TEXT,
            result_count INTEGER,
            html_path TEXT,
            fetched_at TEXT
        );
        CREATE TABLE IF NOT EXISTS chemnet_suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_id TEXT UNIQUE,
            supplier_name TEXT,
            country TEXT,
            city TEXT,
            website TEXT,
            description TEXT,
            chemnet_url TEXT,
            address TEXT,
            email TEXT,
            telephone TEXT,
            fax TEXT,
            fetched_at TEXT
        );
        CREATE TABLE IF NOT EXISTS chemnet_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_chemical TEXT,
            query_cas TEXT,
            product_name TEXT,
            product_cas TEXT,
            supplier_id TEXT,
            supplier_name TEXT,
            supplier_country TEXT,
            purity TEXT,
            grade TEXT,
            min_order TEXT,
            price TEXT,
            product_url TEXT,
            fetched_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_products_cas ON chemnet_products(query_cas);
        CREATE INDEX IF NOT EXISTS idx_products_supplier ON chemnet_products(supplier_id);
        CREATE INDEX IF NOT EXISTS idx_suppliers_country ON chemnet_suppliers(country);
        """
    )
    conn.commit()


def upsert_supplier(conn: sqlite3.Connection, s: Supplier) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chemnet_suppliers
          (supplier_id, supplier_name, country, city, website, description,
           chemnet_url, address, email, telephone, fax, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(supplier_id) DO UPDATE SET
          supplier_name=excluded.supplier_name,
          country=COALESCE(NULLIF(excluded.country,''), chemnet_suppliers.country),
          website=COALESCE(NULLIF(excluded.website,''), chemnet_suppliers.website),
          address=COALESCE(NULLIF(excluded.address,''), chemnet_suppliers.address),
          email=COALESCE(NULLIF(excluded.email,''), chemnet_suppliers.email),
          telephone=COALESCE(NULLIF(excluded.telephone,''), chemnet_suppliers.telephone),
          fax=COALESCE(NULLIF(excluded.fax,''), chemnet_suppliers.fax),
          chemnet_url=COALESCE(NULLIF(excluded.chemnet_url,''), chemnet_suppliers.chemnet_url)
        """,
        (
            s.supplier_id,
            s.supplier_name,
            s.country,
            s.city,
            s.website,
            s.description,
            s.chemnet_url,
            s.address,
            s.email,
            s.telephone,
            s.fax,
            s.fetched_at,
        ),
    )


def insert_product(conn: sqlite3.Connection, p: ProductRow) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chemnet_products
          (query_chemical, query_cas, product_name, product_cas, supplier_id,
           supplier_name, supplier_country, purity, grade, min_order, price,
           product_url, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            p.query_chemical,
            p.query_cas,
            p.product_name,
            p.product_cas,
            p.supplier_id,
            p.supplier_name,
            p.supplier_country,
            p.purity,
            p.grade,
            p.min_order,
            p.price,
            p.product_url,
            p.fetched_at,
        ),
    )


def insert_raw_query(
    conn: sqlite3.Connection,
    *,
    query_chemical: str,
    query_cas: str,
    query_url: str,
    result_count: int,
    html_path: str,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chemnet_raw_queries
          (query_chemical, query_cas, query_url, result_count, html_path, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (query_chemical, query_cas, query_url, result_count, html_path, now_iso()),
    )


def export_csvs(conn: sqlite3.Connection) -> list[Path]:
    exports = []
    for table in ("chemnet_products", "chemnet_suppliers", "chemnet_raw_queries"):
        # Namespace per spec: csv/chemnet_com_*.csv
        suffix = table.replace("chemnet_", "")
        out = CSV_DIR / f"chemnet_com_{suffix}.csv"
        cur = conn.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for row in cur:
                w.writerow(row)
        exports.append(out)
        log.info("wrote %s (%d rows)", out, sum(1 for _ in open(out)) - 1)
    return exports


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def process_chemical(
    conn: sqlite3.Connection, name: str, cas: str, *, extra_budget_ref: list[int]
) -> tuple[int, int]:
    """Returns (n_products_inserted, n_suppliers_upserted)."""
    # 1. CAS lookup to discover canonical slug.
    cas_url = (
        "http://www.chemnet.com/cas/supplier.cgi"
        f"?terms={cas}&exact=dict&l=&f=plist&terms1=&f1=plist&home1=&scale=pw&criteria=bprod"
    )
    html, path, source = fetch_with_fallback(cas_url)
    if not html:
        log.warning("no CAS page for %s (%s)", name, cas)
        # try by name as a fallback query
        name_url = (
            "http://www.chemnet.com/cas/supplier.cgi"
            f"?terms={requests.utils.quote(name)}&exact=contain&f=plist&mid=&scale=pw"
        )
        html, path, source = fetch_with_fallback(name_url)
        if not html:
            insert_raw_query(
                conn,
                query_chemical=name,
                query_cas=cas,
                query_url=cas_url,
                result_count=0,
                html_path="",
            )
            return 0, 0
        cas_url = name_url

    html_path_rel = str(path.relative_to(BASE_DIR)) if path else ""

    slug = extract_slug_from_cas_page(html)
    inline = extract_inline_suppliers_from_cas_page(html)

    products_written = 0
    suppliers_written = 0

    # Record the raw CAS query.
    insert_raw_query(
        conn,
        query_chemical=name,
        query_cas=cas,
        query_url=cas_url,
        result_count=len(inline),
        html_path=html_path_rel,
    )

    # 2. If we have a slug, hit the full listing page + paginate.
    if slug:
        page = 0
        while page < MAX_PAGINATION_PAGES:
            listing_url = f"https://www.chemnet.com/Global/Products/{slug}/Suppliers-0-{page}.html"
            html2, path2, src2 = fetch_with_fallback(listing_url)
            if not html2:
                log.warning("no supplier listing page %s", listing_url)
                break
            suppliers, rows, total, max_page = parse_supplier_listing_page(
                html2, query_chemical=name, query_cas=cas
            )
            for s in suppliers:
                upsert_supplier(conn, s)
                suppliers_written += 1
            for r in rows:
                insert_product(conn, r)
                products_written += 1

            insert_raw_query(
                conn,
                query_chemical=name,
                query_cas=cas,
                query_url=listing_url,
                result_count=len(suppliers),
                html_path=str(path2.relative_to(BASE_DIR)) if path2 else "",
            )

            if page >= max_page or len(suppliers) == 0:
                break
            page += 1
        # Flush per-chemical so crash doesn't lose progress.
        conn.commit()
    else:
        # 3. No Global slug — use inline supplier IDs + synthesize product rows.
        for sid, country, pslug, sname, prod_id, href in inline:
            fetched = now_iso()
            upsert_supplier(
                conn,
                Supplier(
                    supplier_id=sid,
                    supplier_name=sname,
                    country=country,
                    chemnet_url=urljoin(
                        "https://www.chemnet.com/",
                        f"/{country.replace(' ', '-')}Suppliers/{sid}/Products-Catalog--1.html",
                    ),
                    fetched_at=fetched,
                ),
            )
            suppliers_written += 1
            insert_product(
                conn,
                ProductRow(
                    query_chemical=name,
                    query_cas=cas,
                    product_name=pslug.replace("-", " "),
                    product_cas=cas,
                    supplier_id=sid,
                    supplier_name=sname,
                    supplier_country=country,
                    product_url=urljoin("https://www.chemnet.com/", href),
                    fetched_at=fetched,
                ),
            )
            products_written += 1
        conn.commit()

    return products_written, suppliers_written


def crawl_breadth(conn: sqlite3.Connection, limit: int = OTHER_PRODUCT_BUDGET) -> int:
    """Pick N 'other product' URLs from the products table and fetch their supplier
    listing pages to widen the chemical cross-section. Each of those pages also
    contributes additional supplier rows."""
    cur = conn.execute(
        """
        SELECT DISTINCT product_url, product_name FROM chemnet_products
        WHERE product_url LIKE '%/Products/%/Suppliers-0-0.html' OR
              product_url LIKE '%Suppliers/%/%--%.html'
        """
    )
    # We want Global/Products/<slug>/Suppliers-0-0.html ones. Derive slugs from
    # per-product URLs instead.
    cur2 = conn.execute(
        """
        SELECT DISTINCT product_name FROM chemnet_products
        WHERE product_name != '' AND query_chemical NOT IN (SELECT product_name FROM chemnet_products)
        """
    )
    seen_slugs: set[str] = set()
    rows = conn.execute(
        "SELECT DISTINCT product_name, product_url FROM chemnet_products WHERE product_url LIKE '%Suppliers/%'"
    ).fetchall()
    count = 0
    for pname, purl in rows:
        if count >= limit:
            break
        # Extract slug from Other-product URL (e.g. /United-StatesSuppliers/7822/Calcium-Chloride--246889.html)
        m = re.search(r"Suppliers/\d+/([^/\"']+?)--\d+\.html", purl or "")
        if not m:
            continue
        slug = m.group(1).lower()
        # Chemnet's Global slug is typically lowercase and dash-free or hyphenated.
        # The real slug is the 'Products/<slug>' form; try both lower and a normalized one.
        slug_norm = slug.lower()
        if slug_norm in seen_slugs:
            continue
        seen_slugs.add(slug_norm)

        listing_url = f"https://www.chemnet.com/Global/Products/{slug_norm}/Suppliers-0-0.html"
        html, path, src = fetch_with_fallback(listing_url)
        if not html:
            continue
        suppliers, prows, total, max_page = parse_supplier_listing_page(
            html, query_chemical=pname, query_cas=""
        )
        if not suppliers:
            continue
        for s in suppliers:
            upsert_supplier(conn, s)
        for r in prows:
            insert_product(conn, r)
        insert_raw_query(
            conn,
            query_chemical=pname,
            query_cas="",
            query_url=listing_url,
            result_count=len(suppliers),
            html_path=str(path.relative_to(BASE_DIR)) if path else "",
        )
        conn.commit()
        count += 1
        log.info("breadth [%d/%d] %s -> %d suppliers", count, limit, slug_norm, len(suppliers))
    return count


def main() -> int:
    log.info("chemnet.com scraper starting; %d seed chemicals", len(SEED))
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    t0 = time.time()
    total_products = 0
    total_suppliers = 0
    failures: list[str] = []

    for i, (name, cas) in enumerate(SEED, 1):
        try:
            log.info("[%d/%d] %s (%s)", i, len(SEED), name, cas)
            p, s = process_chemical(conn, name, cas, extra_budget_ref=[OTHER_PRODUCT_BUDGET])
            total_products += p
            total_suppliers += s
        except Exception as e:
            log.exception("error processing %s (%s): %s", name, cas, e)
            failures.append(f"{name} ({cas}): {e}")
            continue

    log.info(
        "seed pass done; products=%d supplier-upserts=%d elapsed=%.1fs",
        total_products,
        total_suppliers,
        time.time() - t0,
    )

    # Breadth crawl — fetch extra product pages discovered via "Other products"
    # cross-links to widen cross-section.
    n_breadth = crawl_breadth(conn, limit=OTHER_PRODUCT_BUDGET)
    log.info("breadth crawl fetched %d additional product pages", n_breadth)

    # Final CSV export
    export_csvs(conn)

    cur = conn.execute("SELECT COUNT(*) FROM chemnet_products")
    n_products = cur.fetchone()[0]
    cur = conn.execute("SELECT COUNT(*) FROM chemnet_suppliers")
    n_suppliers = cur.fetchone()[0]
    cur = conn.execute("SELECT COUNT(*) FROM chemnet_raw_queries")
    n_queries = cur.fetchone()[0]
    log.info(
        "DONE products=%d suppliers=%d raw_queries=%d failures=%d",
        n_products,
        n_suppliers,
        n_queries,
        len(failures),
    )
    if failures:
        log.info("failures: %s", failures)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
