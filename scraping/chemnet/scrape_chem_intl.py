#!/usr/bin/env python3
"""
International B2B precursor chemical scraper.

Findings after probing (2026-04-24):
  - Alibaba:     JS-rendered SPA for both live search AND showroom. Archive.org snapshots
                 also lack offer data (JS). We still scrape showroom live + archive for
                 snippets / supplier tag text.
  - IndiaMart:   Live domain is a Next.js SPA (empty HTML). BUT archive.org has 2015 SSR
                 snapshots of dir.indiamart.com/impcat/<chemical>.html with 30 listings
                 each. That is our primary IndiaMart data source.
  - supplierlist.com: Live site works. Its search ignores queries (returns generic feed),
                      so we mine its sitemap-products.xml for precursor-relevant product
                      URLs and scrape those directly.
  - lobasources.com:  DNS doesn't resolve. Domain dead.

Writes:
  - raw HTML  -> data/chemnet/raw/chem_intl/<site>/
  - CSVs      -> data/chemnet/csv/chem_intl_*.csv
  - SQLite    -> data/chemnet/dbs/chem_intl.sqlite3
  - Log       -> data/chemnet/logs/chem_intl.log
"""

import csv
import hashlib
import logging
import os
import re
import socket
import sqlite3
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# -------------------- Paths --------------------
ROOT = Path("/Users/carsonmulligan/Desktop/Workspace/apps/web-rails-holocron-gov-hackathon/data/chemnet")
RAW = ROOT / "raw" / "chem_intl"
CSV_DIR = ROOT / "csv"
DB_DIR = ROOT / "dbs"
LOG_DIR = ROOT / "logs"
DB_PATH = DB_DIR / "chem_intl.sqlite3"
LOG_PATH = LOG_DIR / "chem_intl.log"

for p in (RAW, CSV_DIR, DB_DIR, LOG_DIR):
    p.mkdir(parents=True, exist_ok=True)
for site in ("alibaba", "indiamart", "supplierlist", "lobasources"):
    (RAW / site).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("chem_intl")
sh = logging.StreamHandler(sys.stderr)
sh.setLevel(logging.INFO)
sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
log.addHandler(sh)


# -------------------- Seed CAS list --------------------
# (cas, canonical_chemical, [impcat_slugs_to_try])
SEED = [
    ("108-24-7", "acetic anhydride", ["acetic-anhydride"]),
    ("299-42-3", "ephedrine", ["ephedrine-hydrochloride", "ephedrine-hcl", "ephedrine"]),
    ("90-82-4", "pseudoephedrine", ["pseudoephedrine", "pseudoephedrine-hydrochloride"]),
    ("103-79-7", "P-2-P phenylacetone", ["phenylacetone", "phenyl-acetone", "phenyl-2-propanone"]),
    ("4468-48-8", "APAAN", ["alpha-phenylacetoacetonitrile", "apaan"]),
    ("16648-44-5", "BMK glycidate", ["bmk-glycidate", "bmk", "methyl-glycidate"]),
    ("94-59-7", "safrole", ["safrole", "safrol"]),
    ("120-57-0", "piperonal", ["piperonal", "heliotropin"]),
    ("74-89-5", "methylamine", ["methylamine", "methyl-amine", "monomethylamine"]),
    ("7723-14-0", "red phosphorus", ["red-phosphorus", "phosphorus"]),
    ("7722-64-7", "potassium permanganate", ["potassium-permanganate"]),
    ("100-52-7", "benzaldehyde", ["benzaldehyde"]),
    ("79-24-3", "nitroethane", ["nitroethane"]),
    ("96-48-0", "gamma-butyrolactone", ["gamma-butyrolactone", "butyrolactone", "gbl"]),
    ("110-89-4", "piperidine", ["piperidine"]),
    ("79099-07-3", "1-Boc-4-piperidone", ["1-boc-4-piperidone", "boc-piperidone", "n-boc-4-piperidone"]),
    ("21409-26-7", "4-ANPP", ["4-anpp", "4-anilino-n-phenethylpiperidine"]),
    ("39742-60-4", "NPP", ["npp", "n-phenethyl-4-piperidone"]),
    ("41979-39-9", "4-piperidone HCl", ["4-piperidone-hydrochloride", "4-piperidone", "piperidone-hcl"]),
    ("62-53-3", "aniline", ["aniline"]),
    ("79-03-8", "propionyl chloride", ["propionyl-chloride"]),
    ("103-63-9", "2-Bromoethylbenzene", ["2-bromoethylbenzene", "phenethyl-bromide"]),
    ("78-93-3", "methyl ethyl ketone", ["methyl-ethyl-ketone", "mek", "butan-2-one"]),
    ("67-64-1", "acetone", ["acetone"]),
]

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": UA,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Upgrade-Insecure-Requests": "1",
}

RATE = {
    "alibaba": 3.0,
    "indiamart": 3.0,
    "supplierlist": 3.0,
    "lobasources": 3.0,
    "archive": 5.0,  # archive.org is sensitive; 5s spacing
}
_last_hit = {}


def polite_sleep(site):
    now = time.time()
    last = _last_hit.get(site, 0)
    wait = RATE.get(site, 3.0) - (now - last)
    if wait > 0:
        time.sleep(wait)
    _last_hit[site] = time.time()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def slugify(s):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s)[:80]


def cache_path(site, url, suffix=".html"):
    h = hashlib.md5(url.encode("utf-8")).hexdigest()[:12]
    tail = url.rsplit("/", 1)[-1].split("?", 1)[0] or site
    return RAW / site / f"{slugify(tail)}_{h}{suffix}"


def fetch(site, url, timeout=30, allow_archive=False, retry_on_error=True):
    """Fetch URL with caching, archive.org fallback."""
    polite_sleep(site)
    cp = cache_path(site, url)
    if cp.exists() and cp.stat().st_size > 1000:
        try:
            txt = cp.read_text(encoding="utf-8", errors="ignore")
            log.info(f"[cache] {site} {url}")
            return 200, txt, str(cp)
        except Exception:
            pass

    status, text = None, ""
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        status = r.status_code
        text = r.text
        log.info(f"[fetch] {site} {status} {url}")
        if status in (429, 503) and retry_on_error:
            time.sleep(30)
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            status, text = r.status_code, r.text
            log.info(f"[retry] {site} {status} {url}")
    except Exception as e:
        log.warning(f"[err] {site} {url} {e}")
        status = -1

    if allow_archive and (
        status in (403, 429, 503, -1, 404)
        or (text and len(text) < 4000)
    ):
        arc = f"https://web.archive.org/web/2024/{url}"
        polite_sleep("archive")
        try:
            r = requests.get(arc, headers=HEADERS, timeout=timeout + 15, allow_redirects=True)
            status = r.status_code
            text = r.text
            log.info(f"[archive] {status} {arc}")
        except Exception as e:
            log.warning(f"[archive-err] {arc} {e}")

    try:
        cp.write_text(text or "", encoding="utf-8")
    except Exception:
        pass
    return status, text or "", str(cp)


# -------------------- SQLite --------------------
def db_init():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS intl_products (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_site TEXT, query_cas TEXT, query_chemical TEXT,
          product_name TEXT, product_cas TEXT, product_url TEXT,
          supplier_name TEXT, supplier_country TEXT, supplier_city TEXT,
          price TEXT, moq TEXT, purity TEXT,
          raw_snippet TEXT, fetched_at TEXT,
          UNIQUE(source_site, supplier_name, product_cas, product_url)
        );
        CREATE TABLE IF NOT EXISTS intl_suppliers (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_site TEXT, supplier_name TEXT, country TEXT, city TEXT,
          website TEXT, years_on_platform TEXT, gold_supplier INTEGER,
          verified INTEGER, supplier_url TEXT, fetched_at TEXT,
          UNIQUE(source_site, supplier_name, supplier_url)
        );
        CREATE TABLE IF NOT EXISTS intl_raw_queries (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_site TEXT, query_cas TEXT, query_url TEXT,
          http_status INTEGER, result_count INTEGER,
          html_path TEXT, fetched_at TEXT
        );
        """
    )
    conn.commit()
    return conn


def ins_product(conn, p):
    try:
        conn.execute(
            """INSERT OR IGNORE INTO intl_products
            (source_site, query_cas, query_chemical, product_name, product_cas, product_url,
             supplier_name, supplier_country, supplier_city, price, moq, purity,
             raw_snippet, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                p["source_site"], p["query_cas"], p["query_chemical"], p["product_name"],
                p["product_cas"], p["product_url"], p["supplier_name"], p["supplier_country"],
                p["supplier_city"], p["price"], p["moq"], p["purity"], p["raw_snippet"], now_iso(),
            ),
        )
    except Exception as e:
        log.warning(f"ins_product err: {e}")


def ins_supplier(conn, s):
    try:
        conn.execute(
            """INSERT OR IGNORE INTO intl_suppliers
            (source_site, supplier_name, country, city, website, years_on_platform,
             gold_supplier, verified, supplier_url, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                s["source_site"], s["supplier_name"], s["country"], s["city"], s["website"],
                s["years_on_platform"], s["gold_supplier"], s["verified"], s["supplier_url"], now_iso(),
            ),
        )
    except Exception as e:
        log.warning(f"ins_supplier err: {e}")


def ins_raw_query(conn, site, cas, url, status, count, path):
    conn.execute(
        """INSERT INTO intl_raw_queries
        (source_site, query_cas, query_url, http_status, result_count, html_path, fetched_at)
        VALUES (?,?,?,?,?,?,?)""",
        (site, cas, url, status or 0, count, path, now_iso()),
    )


# ==================== IndiaMart (archived) ====================
def parse_indiamart_archive(html, query_cas, query_chem):
    """Parse archived dir.indiamart.com/impcat/... page (SSR circa 2015)."""
    soup = BeautifulSoup(html, "html.parser")
    products, suppliers = [], []

    for card in soup.select(".listing"):
        name_el = card.select_one(".product-name")
        comp_el = card.select_one(".company-name, .company-link")
        loc_el = card.select_one(".cityLocation")

        name = (name_el.get_text(" ", strip=True) if name_el else "")[:200]
        comp = (comp_el.get_text(" ", strip=True) if comp_el else "")[:200]
        loc = (loc_el.get_text(" ", strip=True) if loc_el else "")[:120]

        a = card.find("a", href=True)
        purl = a["href"] if a else ""
        # archive URLs are web.archive.org/web/<ts>/http://original...
        # Keep as-is; user can unwrap.

        city = ""
        country = "India"
        if loc:
            parts = [x.strip() for x in re.split(r"[,|]", loc) if x.strip()]
            city = parts[0] if parts else ""

        if not (name or comp):
            continue

        txt = card.get_text(" ", strip=True)[:400]
        products.append({
            "source_site": "indiamart",
            "query_cas": query_cas,
            "query_chemical": query_chem,
            "product_name": name or query_chem,
            "product_cas": query_cas,
            "product_url": purl,
            "supplier_name": comp,
            "supplier_country": country,
            "supplier_city": city,
            "price": "",
            "moq": "",
            "purity": "",
            "raw_snippet": txt,
        })
        if comp:
            suppliers.append({
                "source_site": "indiamart",
                "supplier_name": comp,
                "country": country,
                "city": city,
                "website": purl,
                "years_on_platform": "",
                "gold_supplier": 0,
                "verified": 0,
                "supplier_url": purl,
            })
    return products, suppliers


def run_indiamart(conn, cas, chem, impcat_slugs):
    """Try multiple slugs against archive.org; take the first that returns listings."""
    # Also record a "live tried" row to show we looked at live (SPA, js-required)
    live_url = f"https://dir.indiamart.com/search.mp?ss={urllib.parse.quote(cas)}"
    ins_raw_query(conn, "indiamart", cas, live_url, -2, 0, "js-required")

    total_prods, total_sups = 0, 0
    for slug in impcat_slugs:
        arc_url = f"https://web.archive.org/web/2024/https://dir.indiamart.com/impcat/{slug}.html"
        # Use archive rate-limit bucket
        polite_sleep("archive")
        status, html, path = fetch_raw(arc_url, site_dir="indiamart")
        prods, sups = parse_indiamart_archive(html or "", cas, chem)
        ins_raw_query(conn, "indiamart", cas, arc_url, status, len(prods), path)
        if prods:
            for p in prods:
                ins_product(conn, p)
            for s in sups:
                ins_supplier(conn, s)
            total_prods += len(prods)
            total_sups += len(sups)
            break  # stop at first successful slug
    conn.commit()
    return total_prods, total_sups


def fetch_raw(url, site_dir, timeout=35):
    """Simple raw fetch used when caller already handled rate-limit."""
    cp = cache_path(site_dir, url)
    if cp.exists() and cp.stat().st_size > 1000:
        try:
            log.info(f"[cache] {site_dir} {url}")
            return 200, cp.read_text(encoding="utf-8", errors="ignore"), str(cp)
        except Exception:
            pass
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        status, text = r.status_code, r.text
        log.info(f"[fetch] {site_dir} {status} {url}")
        if status in (429, 503):
            time.sleep(30)
            r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
            status, text = r.status_code, r.text
            log.info(f"[retry] {site_dir} {status} {url}")
        cp.write_text(text, encoding="utf-8")
        return status, text, str(cp)
    except Exception as e:
        log.warning(f"[err] {site_dir} {url} {e}")
        return -1, "", str(cp)


# ==================== Alibaba ====================
def parse_alibaba(html, query_cas, query_chem):
    """Try a variety of selectors; Alibaba is JS-heavy so yields may be slim."""
    soup = BeautifulSoup(html, "html.parser")
    products, suppliers = [], []

    # Search-card SSR (newer)
    cards = soup.select(
        ".organic-gallery-offer-outter, .J-search-card-wrapper, "
        ".fy23-search-card, .search-card-m, .m-gallery-product-item-v2, "
        "[class*='search-card'], .list-no-v2-outter"
    )
    for card in cards[:60]:
        txt = card.get_text(" ", strip=True)
        if len(txt) < 15:
            continue
        a = card.find("a", href=True)
        purl = a["href"] if a else ""
        if purl.startswith("//"):
            purl = "https:" + purl
        name_el = card.select_one(
            "h2, h4, .search-card-e-title, .elements-title-normal__content, [class*='title']"
        )
        name = (name_el.get_text(" ", strip=True) if name_el else txt[:100])[:200]
        sup_el = card.select_one(
            ".company-name, .search-card-e-company, .supplier, [class*='company']"
        )
        sup = (sup_el.get_text(" ", strip=True) if sup_el else "")[:200]
        loc_el = card.select_one(
            ".supplier-tag-country, .search-card-e-country, .location, [class*='country']"
        )
        loc = (loc_el.get_text(" ", strip=True) if loc_el else "")[:80]

        price_el = card.select_one(
            ".elements-offer-price-normal, .search-card-e-price-main, .price"
        )
        price = (price_el.get_text(" ", strip=True) if price_el else "")[:80]
        moq_el = card.select_one(
            ".search-card-m-sale-features__item, .moq, [class*='moq']"
        )
        moq = (moq_el.get_text(" ", strip=True) if moq_el else "")[:80]

        products.append({
            "source_site": "alibaba",
            "query_cas": query_cas,
            "query_chemical": query_chem,
            "product_name": name,
            "product_cas": query_cas,
            "product_url": purl,
            "supplier_name": sup,
            "supplier_country": loc or "China",
            "supplier_city": "",
            "price": price,
            "moq": moq,
            "purity": "",
            "raw_snippet": txt[:400],
        })
        if sup:
            suppliers.append({
                "source_site": "alibaba",
                "supplier_name": sup,
                "country": loc or "China",
                "city": "",
                "website": purl,
                "years_on_platform": "",
                "gold_supplier": 1 if "gold" in txt.lower() else 0,
                "verified": 1 if "verified" in txt.lower() else 0,
                "supplier_url": purl,
            })

    # Fallback: mine anchors for /product-detail/ slugs
    if not products:
        for a in soup.select("a[href*='product-detail']"):
            href = a["href"]
            if href.startswith("//"):
                href = "https:" + href
            t = a.get_text(" ", strip=True) or (a.get("title") or "")
            if not t or len(t) < 4:
                continue
            products.append({
                "source_site": "alibaba",
                "query_cas": query_cas,
                "query_chemical": query_chem,
                "product_name": t[:200],
                "product_cas": query_cas,
                "product_url": href,
                "supplier_name": "",
                "supplier_country": "China",
                "supplier_city": "",
                "price": "",
                "moq": "",
                "purity": "",
                "raw_snippet": t[:400],
            })

    # Mine JSON-embedded supplier info if present
    subjects = re.findall(r'"subject"\s*:\s*"([^"]{5,160})"', html)
    companies = re.findall(r'"(?:companyName|company_name|compName)"\s*:\s*"([^"]{3,120})"', html)
    countries = re.findall(r'"(?:country|countryCode)"\s*:\s*"([A-Z]{2,4})"', html)
    urls_json = re.findall(r'"(?:productUrl|detailUrl)"\s*:\s*"([^"]{10,300})"', html)
    for i, s_name in enumerate(subjects):
        comp = companies[i] if i < len(companies) else ""
        country = countries[i] if i < len(countries) else "China"
        u = urls_json[i] if i < len(urls_json) else ""
        u = u.replace("\\/", "/")
        if u.startswith("//"):
            u = "https:" + u
        products.append({
            "source_site": "alibaba",
            "query_cas": query_cas,
            "query_chemical": query_chem,
            "product_name": s_name.replace("\\u0026", "&")[:200],
            "product_cas": query_cas,
            "product_url": u,
            "supplier_name": comp,
            "supplier_country": country,
            "supplier_city": "",
            "price": "",
            "moq": "",
            "purity": "",
            "raw_snippet": s_name[:400],
        })
        if comp:
            suppliers.append({
                "source_site": "alibaba",
                "supplier_name": comp,
                "country": country,
                "city": "",
                "website": u,
                "years_on_platform": "",
                "gold_supplier": 0,
                "verified": 0,
                "supplier_url": u,
            })
    return products, suppliers


def run_alibaba(conn, cas, chem):
    prods_all, sups_all = [], []
    # 1. Live showroom (SSR-ish, expect sparse)
    chem_slug = chem.lower().replace(" ", "-").replace("/", "-")
    live_url = f"https://www.alibaba.com/showroom/{chem_slug}.html"
    status, html, path = fetch("alibaba", live_url, allow_archive=False)
    prods, sups = parse_alibaba(html or "", cas, chem)
    ins_raw_query(conn, "alibaba", cas, live_url, status, len(prods), path)
    prods_all.extend(prods); sups_all.extend(sups)

    # 2. Live search
    q = urllib.parse.quote(chem)
    s_url = f"https://www.alibaba.com/trade/search?SearchText={q}"
    status, html, path = fetch("alibaba", s_url, allow_archive=False)
    prods, sups = parse_alibaba(html or "", cas, chem)
    ins_raw_query(conn, "alibaba", cas, s_url, status, len(prods), path)
    prods_all.extend(prods); sups_all.extend(sups)

    # 3. Archive fallback
    if not prods_all:
        for year in (2022, 2019, 2018):
            arc = f"https://web.archive.org/web/{year}/https://www.alibaba.com/showroom/{chem_slug}.html"
            status, html, path = fetch("alibaba", arc, allow_archive=False)
            prods, sups = parse_alibaba(html or "", cas, chem)
            ins_raw_query(conn, "alibaba", cas, arc, status, len(prods), path)
            prods_all.extend(prods); sups_all.extend(sups)
            if prods:
                break

    for p in prods_all:
        ins_product(conn, p)
    for s in sups_all:
        ins_supplier(conn, s)
    conn.commit()
    return len(prods_all), len(sups_all)


# ==================== Supplierlist ====================
def get_supplierlist_sitemap_urls():
    """Fetch sitemap once and return list of product URLs."""
    cache = RAW / "supplierlist" / "sitemap_products.xml"
    if cache.exists() and cache.stat().st_size > 10000:
        txt = cache.read_text(encoding="utf-8", errors="ignore")
    else:
        try:
            polite_sleep("supplierlist")
            r = requests.get(
                "https://www.supplierlist.com/sitemap-products.xml",
                headers=HEADERS, timeout=60,
            )
            txt = r.text
            cache.write_text(txt, encoding="utf-8")
            log.info(f"[sitemap] supplierlist bytes={len(txt)}")
        except Exception as e:
            log.warning(f"sitemap err: {e}")
            return []
    return re.findall(r"<loc>([^<]+)</loc>", txt)


def parse_supplierlist_product_page(html, query_cas, query_chem, url):
    """Individual product page on supplierlist."""
    soup = BeautifulSoup(html, "html.parser")
    name_el = soup.select_one("h1, .product-title, .ptitle, title")
    name = (name_el.get_text(" ", strip=True) if name_el else "")[:200]
    # Supplier box: typically has class 'seller' or 'supplier' or link to /company_
    sup = ""
    country = ""
    city = ""
    website = ""
    sup_link = soup.select_one("a[href*='supplier'], a[href*='company_'], a[href*='member']")
    if sup_link:
        sup = sup_link.get_text(" ", strip=True)[:200]
        website = sup_link.get("href", "")
    # Fallback: look for "by <name>" patterns
    if not sup:
        m = re.search(r"by\s+([A-Z][A-Za-z0-9&\.\-\s]{2,80}?)(?:\s*\||\s*,|\s*\.|$)", soup.get_text("\n"))
        if m:
            sup = m.group(1).strip()[:200]
    # Country/city — scan for common labels
    text = soup.get_text("\n", strip=True)
    mloc = re.search(r"(?:Location|Country|From)\s*[:\-]\s*([A-Z][A-Za-z ,]+)", text)
    if mloc:
        loc = mloc.group(1).strip()[:120]
        parts = [x.strip() for x in re.split(r"[,|]", loc) if x.strip()]
        if parts:
            country = parts[-1]
            if len(parts) > 1:
                city = parts[0]
    price_el = soup.select_one(".price, .product-price, [class*='price']")
    price = (price_el.get_text(" ", strip=True) if price_el else "")[:80]

    snippet = text[:400]
    product = {
        "source_site": "supplierlist",
        "query_cas": query_cas,
        "query_chemical": query_chem,
        "product_name": name,
        "product_cas": query_cas,
        "product_url": url,
        "supplier_name": sup,
        "supplier_country": country,
        "supplier_city": city,
        "price": price,
        "moq": "",
        "purity": "",
        "raw_snippet": snippet,
    }
    supplier = None
    if sup:
        supplier = {
            "source_site": "supplierlist",
            "supplier_name": sup,
            "country": country,
            "city": city,
            "website": website,
            "years_on_platform": "",
            "gold_supplier": 0,
            "verified": 0,
            "supplier_url": website or url,
        }
    return product, supplier


def run_supplierlist(conn, sitemap_urls, cas, chem):
    """Match this cas/chem against product URLs and scrape matches."""
    # Tokenize chem name AND also try raw CAS (CAS rarely in URL but try)
    clean = chem.lower()
    clean = re.sub(r"[^a-z0-9\s]", " ", clean)
    toks = [t for t in clean.split() if len(t) >= 4 and t not in {"with", "from"}]
    # Also map informal names to broader keywords
    keyword_expand = {
        "phenylacetone": ["phenylacet"],
        "methylamine": ["methylamine", "methyl amine"],
        "pseudoephedrine": ["ephedrine"],
        "piperonal": ["piperonal", "heliotrop"],
        "potassium permanganate": ["permanganate"],
        "p 2 p": ["phenylacet"],
        "bmk": ["bmk", "methyl glycidate"],
    }
    extra = keyword_expand.get(clean.strip(), [])
    for t in list(toks):
        if t in keyword_expand:
            extra.extend(keyword_expand[t])
    toks = list({*toks, *[e.lower() for e in extra]})

    matches = set()
    for t in toks:
        t_norm = t.replace(" ", "-")
        for u in sitemap_urls:
            if t in u.lower() or t_norm in u.lower():
                matches.add(u)
    # Also search by CAS digits (rarely present but cheap)
    for u in sitemap_urls:
        if cas in u:
            matches.add(u)
    matches = list(matches)[:8]

    ins_raw_query(conn, "supplierlist", cas,
                  f"sitemap-match::{chem}", 200, len(matches),
                  str(RAW / "supplierlist" / "sitemap_products.xml"))

    prods_total, sups_total = 0, 0
    for url in matches:
        status, html, path = fetch("supplierlist", url, allow_archive=False)
        prod, sup = parse_supplierlist_product_page(html or "", cas, chem, url)
        ins_raw_query(conn, "supplierlist", cas, url, status, 1 if prod["product_name"] else 0, path)
        if prod["product_name"] or prod["supplier_name"]:
            ins_product(conn, prod)
            prods_total += 1
        if sup:
            ins_supplier(conn, sup)
            sups_total += 1
    conn.commit()
    return prods_total, sups_total


# ==================== Lobasources ====================
def run_lobasources(conn, cas, chem):
    """Domain DNS fails. Record that fact in raw_queries."""
    url = f"https://www.lobasources.com/?s={urllib.parse.quote(cas)}"
    try:
        socket.gethostbyname("www.lobasources.com")
        status, html, path = fetch("lobasources", url, allow_archive=True)
    except Exception as e:
        log.info(f"[lobasources] DNS/connect failed: {e}")
        # Try archive.org for the homepage
        arc = f"https://web.archive.org/web/2022/https://www.lobasources.com/"
        status, html, path = fetch("lobasources", arc, allow_archive=False)
        ins_raw_query(conn, "lobasources", cas, url, -1, 0, "dns-fail")
        ins_raw_query(conn, "lobasources", cas, arc, status, 0, path)
        return 0, 0
    ins_raw_query(conn, "lobasources", cas, url, status, 0, path)
    return 0, 0


# ==================== Export ====================
def export_csvs(conn):
    for table, fname in [
        ("intl_products", "chem_intl_products.csv"),
        ("intl_suppliers", "chem_intl_suppliers.csv"),
        ("intl_raw_queries", "chem_intl_raw_queries.csv"),
    ]:
        cur = conn.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        out = CSV_DIR / fname
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        log.info(f"[csv] {out} rows={len(rows)}")


# ==================== Main ====================
def main():
    log.info(f"=== chem_intl start {now_iso()} ===")
    conn = db_init()

    # Pre-fetch supplierlist sitemap once
    log.info("Loading supplierlist sitemap ...")
    sitemap = get_supplierlist_sitemap_urls()
    log.info(f"sitemap size: {len(sitemap)} URLs")

    # Track how many times a site yields zero; if too many, skip its live calls
    zero_streak = {"alibaba": 0, "indiamart": 0, "supplierlist": 0, "lobasources": 0}

    # Was lobasources even reachable? Check once.
    try:
        socket.gethostbyname("www.lobasources.com")
        lobasources_up = True
    except Exception:
        lobasources_up = False
        log.info("lobasources.com DNS does not resolve — will log once, skip.")
    loba_logged = False

    for cas, chem, impcat_slugs in SEED:
        # --- IndiaMart archive ---
        try:
            p, s = run_indiamart(conn, cas, chem, impcat_slugs)
            log.info(f"[indiamart] {cas} {chem} -> prods={p} sups={s}")
            zero_streak["indiamart"] = 0 if p else zero_streak["indiamart"] + 1
        except Exception as e:
            log.exception(f"[indiamart] {cas}: {e}")

        # --- Alibaba ---
        if zero_streak["alibaba"] < 6:
            try:
                p, s = run_alibaba(conn, cas, chem)
                log.info(f"[alibaba] {cas} {chem} -> prods={p} sups={s}")
                zero_streak["alibaba"] = 0 if p else zero_streak["alibaba"] + 1
            except Exception as e:
                log.exception(f"[alibaba] {cas}: {e}")
        else:
            log.info(f"[alibaba] skip {cas} (too many empties)")

        # --- Supplierlist ---
        try:
            p, s = run_supplierlist(conn, sitemap, cas, chem)
            log.info(f"[supplierlist] {cas} {chem} -> prods={p} sups={s}")
        except Exception as e:
            log.exception(f"[supplierlist] {cas}: {e}")

        # --- Lobasources ---
        if lobasources_up:
            try:
                run_lobasources(conn, cas, chem)
            except Exception as e:
                log.exception(f"[lobasources] {cas}: {e}")
        else:
            if not loba_logged:
                ins_raw_query(conn, "lobasources", cas,
                              "https://www.lobasources.com/",
                              -1, 0, "dns-fail-domain-parked")
                loba_logged = True

    export_csvs(conn)

    cur = conn.cursor()
    log.info("--- FINAL COUNTS ---")
    for t in ("intl_products", "intl_suppliers", "intl_raw_queries"):
        n = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        log.info(f"  {t} = {n}")
    for site, n in cur.execute(
        "SELECT source_site, COUNT(*) FROM intl_products GROUP BY source_site"
    ):
        log.info(f"  products.{site} = {n}")
    for site, n in cur.execute(
        "SELECT source_site, COUNT(*) FROM intl_suppliers GROUP BY source_site"
    ):
        log.info(f"  suppliers.{site} = {n}")

    conn.close()
    log.info(f"=== chem_intl done {now_iso()} ===")


if __name__ == "__main__":
    main()
