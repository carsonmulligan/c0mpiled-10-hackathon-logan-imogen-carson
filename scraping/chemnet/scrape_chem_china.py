#!/usr/bin/env python3
"""
scrape_chem_china.py

Scrape China-based B2B precursor-chemical marketplaces for fentanyl precursors
and other controlled-precursor listings. Defensive / investigative dataset for
gov hackathon.

Sites:
  - made-in-china.com    (working well, rich listings)
  - echemi.com           (JS challenge 202, try + archive fallback)
  - hxchem.net           (search broken -> scrape product/company directories)
  - wap.china.cn         (redirects to goldsupplier.com; scrape that)
  - ecasb.com            (DNS/connect timeout -> archive only)
Fallback: web.archive.org

Author: chem-china scraping agent (gov hackathon)
"""
import os
import re
import time
import json
import random
import sqlite3
import logging
import hashlib
import datetime as dt
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ---------- PATHS ----------
BASE = "/Users/carsonmulligan/Desktop/Workspace/apps/web-rails-holocron-gov-hackathon/data/chemnet"
RAW = os.path.join(BASE, "raw", "chem_china")
CSV_DIR = os.path.join(BASE, "csv")
DB_PATH = os.path.join(BASE, "dbs", "chem_china.sqlite3")
LOG_PATH = os.path.join(BASE, "logs", "chem_china.log")

SITES = {
    "made_in_china":  os.path.join(RAW, "made_in_china"),
    "echemi":         os.path.join(RAW, "echemi"),
    "hxchem":         os.path.join(RAW, "hxchem"),
    "wap_china":      os.path.join(RAW, "wap_china"),
    "ecasb":          os.path.join(RAW, "ecasb"),
    "archive":        os.path.join(RAW, "archive"),
    "goldsupplier":   os.path.join(RAW, "wap_china"),  # reuse
}
for p in [RAW, CSV_DIR, os.path.dirname(DB_PATH), os.path.dirname(LOG_PATH)] + list(SITES.values()):
    os.makedirs(p, exist_ok=True)

# ---------- LOGGING ----------
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("chem_china")
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(console)

# ---------- TARGETS ----------
FENTANYL_PRECURSORS = [
    ("21409-26-7",  "4-ANPP"),
    ("39742-60-4",  "NPP 1-phenethyl-4-piperidone"),
    ("79099-07-3",  "1-Boc-4-piperidone"),
    ("41979-39-9",  "4-piperidone hydrochloride hydrate"),
    ("3612-20-2",   "4-Piperidone monohydrate hydrochloride"),
    ("110-89-4",    "Piperidine"),
    ("62-53-3",     "Aniline"),
    ("79-03-8",     "Propionyl chloride"),
    ("123-75-1",    "Pyrrolidine"),
    ("103-63-9",    "2-Bromoethylbenzene"),
    ("122-90-7",    "Propionic anhydride"),
]

GENERIC_PRECURSORS = [
    ("108-24-7",    "Acetic anhydride"),
    ("299-42-3",    "Ephedrine"),
    ("103-79-7",    "Phenyl-2-propanone P-2-P"),
    ("4468-48-8",   "APAAN"),
    ("16648-44-5",  "BMK glycidate"),
    ("94-59-7",     "Safrole"),
    ("120-57-0",    "Piperonal"),
    ("74-89-5",     "Methylamine"),
    ("67-64-1",     "Acetone"),
    ("108-88-3",    "Toluene"),
]

ALL_TARGETS = FENTANYL_PRECURSORS + GENERIC_PRECURSORS

# ---------- HTTP ----------
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept-Language": "en-US,en;q=0.9,zh;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

_last_fetch = {}
BLOCKED_SITES = set()

def rate_sleep(site, min_gap=3.0):
    now = time.time()
    last = _last_fetch.get(site, 0)
    diff = now - last
    if diff < min_gap:
        time.sleep(min_gap - diff + random.uniform(0.1, 0.4))
    _last_fetch[site] = time.time()

def cache_path(site, url):
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:20]
    d = SITES.get(site, RAW)
    return os.path.join(d, f"{h}.html")

def fetch(url, site, timeout=20, retries=2, method="GET", data=None, headers_extra=None):
    path = cache_path(site, url + (str(data) if data else ""))
    if os.path.exists(path) and os.path.getsize(path) > 500:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return 200, f.read(), path
    if site in BLOCKED_SITES:
        return 0, "", path

    backoffs = [3, 8]
    last_status = 0
    headers = dict(HEADERS)
    if headers_extra:
        headers.update(headers_extra)
    for attempt in range(retries + 1):
        rate_sleep(site)
        try:
            if method == "POST":
                r = requests.post(url, headers=headers, data=data, timeout=timeout, allow_redirects=True)
            else:
                r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            last_status = r.status_code
            logger.info(f"[{site}] {method} {r.status_code} len={len(r.text)} {url[:120]}")
            if r.status_code == 200 and r.text and len(r.text) > 200:
                try:
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(r.text)
                except Exception as e:
                    logger.warning(f"cache write fail {e}")
                return r.status_code, r.text, path
            if r.status_code in (202, 403, 429, 503):
                if attempt < retries:
                    time.sleep(backoffs[attempt])
                    continue
                logger.warning(f"[{site}] {r.status_code} after retries: {url[:120]}")
                time.sleep(10)
                return r.status_code, r.text or "", path
            return r.status_code, r.text or "", path
        except Exception as e:
            logger.warning(f"[{site}] fetch error {e} (attempt {attempt})")
            if attempt < retries:
                time.sleep(backoffs[attempt])
    return last_status, "", path

def fetch_archive(orig_url):
    url = f"https://web.archive.org/web/2024/{orig_url}"
    return fetch(url, "archive")

# ---------- DB ----------
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS china_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_site TEXT,
        query_cas TEXT,
        query_chemical TEXT,
        product_name TEXT,
        product_cas TEXT,
        product_url TEXT,
        supplier_name TEXT,
        supplier_country TEXT,
        supplier_city TEXT,
        price TEXT,
        purity TEXT,
        min_order TEXT,
        raw_snippet TEXT,
        fetched_at TEXT
    );
    CREATE TABLE IF NOT EXISTS china_suppliers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_site TEXT,
        supplier_id TEXT,
        supplier_name TEXT,
        country TEXT,
        province TEXT,
        city TEXT,
        website TEXT,
        years_on_platform TEXT,
        verified TEXT,
        supplier_url TEXT,
        fetched_at TEXT
    );
    CREATE TABLE IF NOT EXISTS china_raw_queries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_site TEXT,
        query_cas TEXT,
        query_url TEXT,
        http_status INTEGER,
        result_count INTEGER,
        html_path TEXT,
        fetched_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_prod_cas ON china_products(query_cas);
    CREATE INDEX IF NOT EXISTS idx_prod_site ON china_products(source_site);
    CREATE INDEX IF NOT EXISTS idx_sup_name ON china_suppliers(supplier_name);
    """)
    con.commit()
    return con

def now_iso():
    return dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

# ---------- HELPERS ----------
CAS_RE = re.compile(r"\b(\d{2,7}-\d{2}-\d)\b")

# common province/city strings to tag
CHINA_PROVINCES = [
    "Beijing","Shanghai","Tianjin","Chongqing",
    "Guangdong","Jiangsu","Zhejiang","Shandong","Hebei","Henan","Hubei","Hunan",
    "Sichuan","Anhui","Jiangxi","Fujian","Liaoning","Jilin","Heilongjiang",
    "Shaanxi","Shanxi","Gansu","Guizhou","Yunnan","Hainan","Qinghai",
    "Inner Mongolia","Xinjiang","Tibet","Ningxia","Guangxi",
    "Taiwan","Hong Kong","Macau",
]
CITY_HINTS = [
    "Shenzhen","Guangzhou","Dongguan","Foshan","Zhongshan","Zhuhai","Shantou","Huizhou",
    "Suzhou","Wuxi","Nanjing","Changzhou","Nantong","Xuzhou","Yancheng",
    "Hangzhou","Ningbo","Wenzhou","Jinhua","Taizhou","Yiwu",
    "Qingdao","Jinan","Yantai","Weihai","Weifang","Linyi","Zibo",
    "Shijiazhuang","Tangshan","Baoding","Handan","Cangzhou","Langfang",
    "Zhengzhou","Luoyang","Xinxiang","Anyang",
    "Wuhan","Xiangyang","Yichang",
    "Changsha","Zhuzhou","Xiangtan",
    "Chengdu","Mianyang","Deyang",
    "Xi'an","Xian","Xianyang",
    "Hefei","Wuhu","Bengbu",
    "Nanchang","Jiujiang",
    "Xiamen","Fuzhou","Quanzhou","Zhangzhou",
    "Shenyang","Dalian","Anshan",
    "Harbin","Daqing",
    "Taiyuan",
    "Lanzhou",
    "Guiyang","Zunyi",
    "Kunming",
    "Haikou","Sanya",
    "Nanning","Liuzhou",
    "Hohhot","Baotou",
    "Urumqi",
    "Yinchuan",
]

def detect_location(text):
    if not text:
        return "", ""
    province = ""
    city = ""
    for p in CHINA_PROVINCES:
        if re.search(r"\b" + re.escape(p) + r"\b", text, re.I):
            province = p
            break
    for c in CITY_HINTS:
        if re.search(r"\b" + re.escape(c) + r"\b", text, re.I):
            city = c
            break
    return province, city

def extract_cas(text):
    m = CAS_RE.search(text or "")
    return m.group(1) if m else ""

def clean(s):
    if s is None:
        return ""
    return re.sub(r"\s+", " ", s).strip()

# ---------- PARSERS ----------
def parse_made_in_china(html_text, query_cas, query_chem):
    """Parse made-in-china.com listings. Real product URLs live on {supplier}.en.made-in-china.com/product/ ."""
    soup = BeautifulSoup(html_text, "html.parser")
    rows, suppliers = [], []

    # Skip pages with "no matches"
    lower_text = html_text.lower()
    if "no matches were found" in lower_text and "en.made-in-china.com/product/" not in lower_text:
        return rows, suppliers

    # Find every supplier-subdomain product URL in the raw HTML (most reliable)
    prod_urls = re.findall(r"https?://([a-z0-9\-]+)\.en\.made-in-china\.com/product/[A-Za-z0-9]+/([^\"' <>]+\.html)", html_text)
    # Pairs: (supplier_slug, path)
    prod_pairs = set()
    full_url_map = {}
    for m in re.finditer(r"(https?://([a-z0-9\-]+)\.en\.made-in-china\.com/product/[A-Za-z0-9]+/([^\"' <>]+\.html))", html_text):
        full_url, sup_slug, title_part = m.group(1), m.group(2), m.group(3)
        prod_pairs.add((sup_slug, full_url, title_part))

    # Also match anchor-based extraction for supplier names / prices
    # Extract <a title="..." href="mic-product-url">
    for a in soup.find_all("a", href=True, title=True):
        href = a["href"]
        if "en.made-in-china.com/product/" not in href:
            continue
        title = clean(a.get("title") or a.get_text(" ", strip=True))
        if len(title) < 5:
            continue
        url = href if href.startswith("http") else ("https:" + href if href.startswith("//") else urljoin("https://www.made-in-china.com/", href))
        # climb up container
        parent = a
        for _ in range(6):
            if parent.parent: parent = parent.parent
            else: break
        ctx = clean(parent.get_text(" ", strip=True)) if parent else ""
        # price
        price_m = re.search(r"US\s*\$\s*[\d\.,]+\s*(?:-\s*[\d\.,]+)?\s*/?\s*[A-Za-z]*", ctx)
        price = price_m.group(0) if price_m else ""
        moq_m = re.search(r"(?:Min\.?\s*Order|MOQ)[:\s]*([^\n\|\.]{1,60})", ctx, re.I)
        moq = clean(moq_m.group(1)) if moq_m else ""
        purity_m = re.search(r"(?:Purity|Assay|Content)[:\s]*[\d\.]+\s*%", ctx, re.I)
        purity = purity_m.group(0) if purity_m else ""
        province, city = detect_location(ctx)
        # supplier slug from subdomain
        sup_match = re.search(r"https?://([a-z0-9\-]+)\.en\.made-in-china\.com/", url)
        sup_slug = sup_match.group(1) if sup_match else ""
        rows.append({
            "source_site": "made_in_china",
            "query_cas": query_cas,
            "query_chemical": query_chem,
            "product_name": title[:500],
            "product_cas": extract_cas(title) or extract_cas(ctx) or query_cas,
            "product_url": url,
            "supplier_name": sup_slug,
            "supplier_country": "China",
            "supplier_city": city,
            "price": price[:200],
            "purity": purity[:100],
            "min_order": moq[:100],
            "raw_snippet": ctx[:600],
            "fetched_at": now_iso(),
        })

    # For each unique supplier slug, emit a supplier record
    seen_sup = set()
    for (sup_slug, full_url, _tp) in prod_pairs:
        if sup_slug in seen_sup: continue
        seen_sup.add(sup_slug)
        sup_url = f"https://{sup_slug}.en.made-in-china.com/"
        # Try to capture supplier display name near a link to that subdomain
        display = ""
        # First occurrence of an <a ...> mentioning subdomain
        aa = soup.find("a", href=re.compile(re.escape(f"{sup_slug}.en.made-in-china.com")))
        if aa:
            display = clean(aa.get_text(" ", strip=True))
        suppliers.append({
            "source_site": "made_in_china",
            "supplier_id": sup_slug,
            "supplier_name": display[:300] or sup_slug,
            "country": "China",
            "province": "",
            "city": "",
            "website": sup_url,
            "years_on_platform": "",
            "verified": "",
            "supplier_url": sup_url,
            "fetched_at": now_iso(),
        })
    return rows, suppliers


def parse_echemi(html_text, query_cas, query_chem):
    soup = BeautifulSoup(html_text, "html.parser")
    rows, suppliers = [], []
    for a in soup.select("a[href*='/productsInformation/'], a[href*='/produce/'], a[href*='/pd-']"):
        href = a.get("href", "")
        title = clean(a.get_text(" ", strip=True))
        if len(title) < 5:
            continue
        url = href if href.startswith("http") else urljoin("https://www.echemi.com/", href)
        rows.append({
            "source_site": "echemi",
            "query_cas": query_cas, "query_chemical": query_chem,
            "product_name": title[:500],
            "product_cas": extract_cas(title) or query_cas,
            "product_url": url,
            "supplier_name": "", "supplier_country": "China", "supplier_city": "",
            "price": "", "purity": "", "min_order": "",
            "raw_snippet": title[:500],
            "fetched_at": now_iso(),
        })
    for a in soup.select("a[href*='/supplier/'], a[href*='/company/'], a[href*='/cp_']"):
        name = clean(a.get_text(" ", strip=True))
        if not name or len(name) < 3:
            continue
        href = a.get("href", "")
        url = href if href.startswith("http") else urljoin("https://www.echemi.com/", href)
        suppliers.append({
            "source_site": "echemi",
            "supplier_id": urlparse(url).path.split("/")[-1][:100],
            "supplier_name": name[:300],
            "country": "", "province": "", "city": "",
            "website": url, "years_on_platform": "", "verified": "",
            "supplier_url": url, "fetched_at": now_iso(),
        })
    return rows, suppliers


def parse_hxchem_directory(html_text, query_cas, query_chem):
    """hxchem.net - search is broken but /productse/ lists products and /English/company.php lists suppliers"""
    soup = BeautifulSoup(html_text, "html.parser")
    rows, suppliers = [], []
    for a in soup.select("a[href*='orderproduct']"):
        href = a.get("href", "")
        title = clean(a.get_text(" ", strip=True))
        if len(title) < 3 or title.lower() == "more":
            continue
        url = href if href.startswith("http") else urljoin("https://www.hxchem.net/productse/", href)
        # supplier is encoded in filename: orderproduct{id}-{company_slug}.html
        m = re.search(r"orderproduct\d+-([^\./]+)\.", url)
        slug = m.group(1) if m else ""
        rows.append({
            "source_site": "hxchem",
            "query_cas": query_cas, "query_chemical": query_chem,
            "product_name": title[:500],
            "product_cas": extract_cas(title) or query_cas,
            "product_url": url,
            "supplier_name": slug[:100],
            "supplier_country": "China", "supplier_city": "",
            "price": "", "purity": "", "min_order": "",
            "raw_snippet": title[:500],
            "fetched_at": now_iso(),
        })
    for a in soup.select("a[href*='companydetail']"):
        href = a.get("href", "")
        title = clean(a.get_text(" ", strip=True))
        if len(title) < 3:
            continue
        url = href if href.startswith("http") else urljoin("https://www.hxchem.net/English/", href)
        suppliers.append({
            "source_site": "hxchem",
            "supplier_id": urlparse(url).path.split("/")[-1][:120],
            "supplier_name": title[:300],
            "country": "China", "province": "", "city": "",
            "website": url, "years_on_platform": "", "verified": "",
            "supplier_url": url, "fetched_at": now_iso(),
        })
    return rows, suppliers


def parse_goldsupplier(html_text, query_cas, query_chem):
    """wap.china.cn redirects to goldsupplier.com."""
    soup = BeautifulSoup(html_text, "html.parser")
    rows, suppliers = [], []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        title = clean(a.get_text(" ", strip=True))
        if len(title) < 6 or len(title) > 400:
            continue
        if any(k in href for k in ["/product", "/offer", "/goods", "/p/", "/item", "/supply"]):
            url = href if href.startswith("http") else urljoin("https://www.goldsupplier.com/", href)
            parent = a.parent
            ctx = clean(parent.get_text(" ", strip=True)) if parent else ""
            province, city = detect_location(ctx)
            rows.append({
                "source_site": "wap_china",
                "query_cas": query_cas, "query_chemical": query_chem,
                "product_name": title[:500],
                "product_cas": extract_cas(title) or query_cas,
                "product_url": url,
                "supplier_name": "", "supplier_country": "China", "supplier_city": city,
                "price": "", "purity": "", "min_order": "",
                "raw_snippet": ctx[:400],
                "fetched_at": now_iso(),
            })
        if any(k in href for k in ["/supplier", "/company", "/cp_", "/shop"]):
            url = href if href.startswith("http") else urljoin("https://www.goldsupplier.com/", href)
            suppliers.append({
                "source_site": "wap_china",
                "supplier_id": urlparse(url).path.split("/")[-1][:120],
                "supplier_name": title[:300],
                "country": "China", "province": "", "city": "",
                "website": url, "years_on_platform": "", "verified": "",
                "supplier_url": url, "fetched_at": now_iso(),
            })
    return rows, suppliers


def parse_ecasb(html_text, query_cas, query_chem):
    soup = BeautifulSoup(html_text, "html.parser")
    rows, suppliers = [], []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        title = clean(a.get_text(" ", strip=True))
        if len(title) < 4:
            continue
        if query_cas in href or query_cas in title or "/cas/" in href or "/product" in href or "/supplier" in href:
            url = href if href.startswith("http") else urljoin("https://www.ecasb.com/", href)
            if "supplier" in href or "company" in href or "manufacturer" in href:
                suppliers.append({
                    "source_site": "ecasb",
                    "supplier_id": urlparse(url).path.split("/")[-1][:100],
                    "supplier_name": title[:300],
                    "country": "", "province": "", "city": "",
                    "website": url, "years_on_platform": "", "verified": "",
                    "supplier_url": url, "fetched_at": now_iso(),
                })
            else:
                rows.append({
                    "source_site": "ecasb",
                    "query_cas": query_cas, "query_chemical": query_chem,
                    "product_name": title[:500],
                    "product_cas": extract_cas(title) or query_cas,
                    "product_url": url,
                    "supplier_name": "", "supplier_country": "", "supplier_city": "",
                    "price": "", "purity": "", "min_order": "",
                    "raw_snippet": title[:500],
                    "fetched_at": now_iso(),
                })
    return rows, suppliers


PARSERS = {
    "made_in_china":  parse_made_in_china,
    "echemi":         parse_echemi,
    "hxchem":         parse_hxchem_directory,
    "wap_china":      parse_goldsupplier,
    "goldsupplier":   parse_goldsupplier,
    "ecasb":          parse_ecasb,
}

# ---------- TARGET URL BUILDERS ----------
def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")

def urls_for_query(cas, chem):
    cas_q = quote_plus(cas)
    sl = slug(chem)
    first_word = chem.split()[0].lower()
    urls = [
        # MIC hot-china-products slug page (richest - server-rendered full list)
        ("made_in_china", f"https://www.made-in-china.com/products-search/hot-china-products/{sl}.html"),
        # MIC with just first word slug (broader coverage)
        ("made_in_china", f"https://www.made-in-china.com/products-search/hot-china-products/{slug(first_word)}.html"),
        # MIC CAS-based multi-search as secondary
        ("made_in_china", f"https://www.made-in-china.com/multi-search/{cas_q}/F1/1.html"),
        # echemi
        ("echemi",        f"https://www.echemi.com/search.html?keyword={cas_q}"),
        # goldsupplier (wap.china.cn redirects here)
        ("wap_china",     f"https://www.goldsupplier.com/search.html?keyword={cas_q}"),
        ("wap_china",     f"https://www.goldsupplier.com/search.html?keyword={quote_plus(first_word)}"),
        # ecasb goes direct to archive (DNS/connect timeout consistently on live site)
        ("archive",       f"https://web.archive.org/web/2024/https://www.ecasb.com/cas/{cas}.html"),
    ]
    return urls

HXCHEM_DIR_URLS = [
    "https://www.hxchem.net/productse/",
    "https://www.hxchem.net/English/company.php",
    "https://www.hxchem.net/English/company.html",
    "https://www.hxchem.net/English/sell.php",
    "https://www.hxchem.net/English/sell.html",
    "https://www.hxchem.net/English/buy.php",
]

# ---------- MAIN ----------
def run():
    con = init_db()
    cur = con.cursor()

    total_prod = 0
    total_sup = 0
    total_q = 0
    fetches = 0
    partial_saves = 0
    site_fail_streak = {s: 0 for s in SITES}

    # 1) hxchem - directory pages once (search broken) - still record under every CAS
    logger.info("=== Phase 1: hxchem directories ===")
    hxchem_rows = []
    hxchem_suppliers = []
    for u in HXCHEM_DIR_URLS:
        status, text, p = fetch(u, "hxchem")
        fetches += 1
        if text:
            rr, ss = parse_hxchem_directory(text, "", "")
            hxchem_rows.extend(rr)
            hxchem_suppliers.extend(ss)
    # dedupe hxchem suppliers
    seen = set()
    hxchem_suppliers_dedup = []
    for s in hxchem_suppliers:
        k = s["supplier_name"].lower()
        if k in seen: continue
        seen.add(k); hxchem_suppliers_dedup.append(s)
    for s in hxchem_suppliers_dedup:
        cur.execute("""INSERT INTO china_suppliers
            (source_site, supplier_id, supplier_name, country, province, city,
             website, years_on_platform, verified, supplier_url, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (s["source_site"], s["supplier_id"], s["supplier_name"], s["country"],
             s["province"], s["city"], s["website"], s["years_on_platform"],
             s["verified"], s["supplier_url"], s["fetched_at"]))
    total_sup += len(hxchem_suppliers_dedup)
    logger.info(f"[hxchem] dir suppliers={len(hxchem_suppliers_dedup)} raw products={len(hxchem_rows)}")
    con.commit()

    # 2) Per-CAS scraping for search-capable sites
    for cas, chem in ALL_TARGETS:
        logger.info(f"=== Target: {cas} {chem} ===")
        for site, url in urls_for_query(cas, chem):
            if site in BLOCKED_SITES:
                continue

            # archive URLs we route to the right parser based on the embedded target domain
            effective_parser_site = site
            if site == "archive":
                if "ecasb.com" in url:
                    effective_parser_site = "ecasb"
                elif "echemi.com" in url:
                    effective_parser_site = "echemi"
                elif "hxchem.net" in url:
                    effective_parser_site = "hxchem"
                elif "made-in-china.com" in url:
                    effective_parser_site = "made_in_china"
                elif "china.cn" in url or "goldsupplier" in url:
                    effective_parser_site = "wap_china"

            status, text, html_path = fetch(url, site)
            fetches += 1

            # archive fallback on hard block or empty text for non-archive primary fetches
            used_archive = False
            if site != "archive" and (status in (0, 202, 403, 429, 503) or not text or len(text) < 500):
                a_status, a_text, a_path = fetch_archive(url)
                if a_status == 200 and a_text and len(a_text) > 500:
                    logger.info(f"[{site}] archive.org fallback hit for {cas}")
                    status = a_status
                    text = a_text
                    html_path = a_path
                    used_archive = True

            parser = PARSERS.get(effective_parser_site)
            rows, suppliers = [], []
            if parser and text:
                try:
                    rows, suppliers = parser(text, cas, chem)
                except Exception as e:
                    logger.warning(f"[{site}] parser error {e}")
                    rows, suppliers = [], []
                # dedupe
                seen_urls = set()
                dedup = []
                for r in rows:
                    key = r["product_url"]
                    if key in seen_urls: continue
                    seen_urls.add(key)
                    dedup.append(r)
                rows = dedup
                sseen = set(); sdedup = []
                for s in suppliers:
                    k = (s["supplier_name"].lower(), s["supplier_url"])
                    if k in sseen: continue
                    sseen.add(k); sdedup.append(s)
                suppliers = sdedup

            # If we're on MIC, annotate rows with site-used tag if archive
            if used_archive:
                for r in rows:
                    r["raw_snippet"] = "(archive) " + r["raw_snippet"][:580]

            # INSERT
            for r in rows:
                cur.execute("""INSERT INTO china_products
                    (source_site, query_cas, query_chemical, product_name, product_cas,
                     product_url, supplier_name, supplier_country, supplier_city,
                     price, purity, min_order, raw_snippet, fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (r["source_site"], r["query_cas"], r["query_chemical"], r["product_name"],
                     r["product_cas"], r["product_url"], r["supplier_name"], r["supplier_country"],
                     r["supplier_city"], r["price"], r["purity"], r["min_order"],
                     r["raw_snippet"], r["fetched_at"]))
            for s in suppliers:
                cur.execute("""INSERT INTO china_suppliers
                    (source_site, supplier_id, supplier_name, country, province, city,
                     website, years_on_platform, verified, supplier_url, fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (s["source_site"], s["supplier_id"], s["supplier_name"], s["country"],
                     s["province"], s["city"], s["website"], s["years_on_platform"],
                     s["verified"], s["supplier_url"], s["fetched_at"]))
            total_prod += len(rows)
            total_sup += len(suppliers)

            cur.execute("""INSERT INTO china_raw_queries
                (source_site, query_cas, query_url, http_status, result_count, html_path, fetched_at)
                VALUES (?,?,?,?,?,?,?)""",
                (effective_parser_site, cas, url, int(status or 0), len(rows), html_path, now_iso()))
            total_q += 1

            # failure tracking - block site if too many consecutive fails
            if status in (0, 403, 429, 503) or not text:
                site_fail_streak[site] = site_fail_streak.get(site, 0) + 1
                if site_fail_streak[site] >= 6:
                    logger.warning(f"[{site}] blocked for remainder after {site_fail_streak[site]} fails")
                    BLOCKED_SITES.add(site)
            else:
                site_fail_streak[site] = 0

            if fetches % 20 == 0:
                con.commit()
                partial_saves += 1
                logger.info(f"[partial save #{partial_saves}] products={total_prod} suppliers={total_sup} queries={total_q}")

    con.commit()

    # ----- EXPORT -----
    prod_df = pd.read_sql_query("SELECT * FROM china_products", con)
    sup_df = pd.read_sql_query("SELECT * FROM china_suppliers", con)
    q_df = pd.read_sql_query("SELECT * FROM china_raw_queries", con)

    prod_csv = os.path.join(CSV_DIR, "chem_china_products.csv")
    sup_csv = os.path.join(CSV_DIR, "chem_china_suppliers.csv")
    q_csv = os.path.join(CSV_DIR, "chem_china_raw_queries.csv")
    prod_df.to_csv(prod_csv, index=False)
    sup_df.to_csv(sup_csv, index=False)
    q_df.to_csv(q_csv, index=False)

    # Per-site row counts
    per_site_products = prod_df.groupby("source_site").size().to_dict() if len(prod_df) else {}
    per_site_suppliers = sup_df.groupby("source_site").size().to_dict() if len(sup_df) else {}

    con.close()
    return {
        "products_total": int(total_prod),
        "suppliers_total": int(total_sup),
        "queries_total": int(total_q),
        "fetches": fetches,
        "per_site_products": per_site_products,
        "per_site_suppliers": per_site_suppliers,
        "blocked_sites": sorted(BLOCKED_SITES),
        "files": {
            "products_csv": prod_csv,
            "suppliers_csv": sup_csv,
            "queries_csv": q_csv,
            "sqlite": DB_PATH,
            "log": LOG_PATH,
        },
    }

if __name__ == "__main__":
    t0 = time.time()
    stats = run()
    elapsed = time.time() - t0
    print("\n=== SUMMARY ===")
    print(json.dumps(stats, indent=2))
    print(f"elapsed: {elapsed:.1f}s")
