from __future__ import annotations

import argparse
import csv
import logging
import os
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

import httpx
from dotenv import load_dotenv

from enrichment import CompanyRecord, CrustdataClient
from scrapers import (
    ChemnetScraper,
    ChinaexporterScraper,
    EcrobotScraper,
    IndiamartExportScraper,
    TradefordScraper,
)
from scrapers.base import Listing, SkipSite, USER_AGENT

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "out"
SAMPLES_DIR = ROOT / "samples"
TARGETS_FILE = ROOT / "targets.txt"
DB_PATH = OUT_DIR / "holocron.sqlite"
HITS_CSV_PATH = OUT_DIR / "hits.csv"
COMPANIES_CSV_PATH = OUT_DIR / "companies.csv"
SKIPPED_LOG_PATH = OUT_DIR / "skipped.log"
CRUSTDATA_CAP = 200


def parse_targets(targets_path: Path) -> tuple[dict[str, str], list[str], list[str]]:
    sites: dict[str, str] = {}
    keywords: list[str] = []
    cas_numbers: list[str] = []
    section = "sites"
    for raw_line in targets_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "key strings" in line.lower():
            section = "keywords"
            continue
        if "cas numbers" in line.lower():
            section = "cas"
            continue
        if line.startswith("http://") or line.startswith("https://"):
            slug = url_to_slug(line)
            sites[slug] = line
            continue
        if section == "keywords":
            keywords.append(line)
        elif section == "cas":
            cas_numbers.append(line)
    return sites, keywords, cas_numbers


def url_to_slug(url: str) -> str:
    host = url.split("//", 1)[-1].strip("/").lower()
    host = host.removeprefix("www.")
    slug = host.split(".", 1)[0]
    if slug == "export":
        return "indiamart_export"
    return slug


def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)


def configure_logging() -> tuple[logging.Logger, logging.Logger]:
    logger = logging.getLogger("holocron")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(stream_handler)

    skipped_logger = logging.getLogger("holocron.skipped")
    skipped_logger.setLevel(logging.INFO)
    skipped_logger.handlers.clear()
    file_handler = logging.FileHandler(SKIPPED_LOG_PATH, mode="w", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(message)s"))
    skipped_logger.addHandler(file_handler)
    return logger, skipped_logger


def make_scrapers(
    logger: logging.Logger,
    skipped_logger: logging.Logger,
) -> dict[str, object]:
    client = httpx.Client(
        timeout=20.0,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    )
    return {
        "tradeford": TradefordScraper(client, SAMPLES_DIR, logger, skipped_logger),
        "chinaexporter": ChinaexporterScraper(client, SAMPLES_DIR, logger, skipped_logger),
        "ecrobot": EcrobotScraper(client, SAMPLES_DIR, logger, skipped_logger),
        "chemnet": ChemnetScraper(client, SAMPLES_DIR, logger, skipped_logger),
        "indiamart_export": IndiamartExportScraper(client, SAMPLES_DIR, logger, skipped_logger),
    }


def init_db(db_path: Path) -> sqlite3.Connection:
    if db_path.exists():
        db_path.unlink()
    connection = sqlite3.connect(db_path)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS hits (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_site TEXT NOT NULL,
          query_type TEXT NOT NULL,
          query TEXT NOT NULL,
          listing_url TEXT NOT NULL,
          listing_title TEXT,
          supplier_name TEXT,
          supplier_country TEXT,
          price TEXT,
          quantity TEXT,
          snippet TEXT,
          scraped_at TEXT NOT NULL,
          raw_html_path TEXT
        )
        """
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_hits_site ON hits(source_site)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_hits_query ON hits(query)")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS companies (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          raw_supplier_name TEXT NOT NULL UNIQUE,
          crustdata_status TEXT NOT NULL,
          company_name TEXT,
          company_website TEXT,
          linkedin_url TEXT,
          hq_country TEXT,
          hq_city TEXT,
          industry TEXT,
          employee_count INTEGER,
          description TEXT,
          raw_response_json TEXT,
          enriched_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_companies_country ON companies(hq_country)"
    )
    connection.commit()
    return connection


def write_hits_csv(path: Path, listings: list[Listing]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=Listing.csv_headers())
        writer.writeheader()
        for listing in listings:
            writer.writerow(listing.as_row())


def write_companies_csv(path: Path, companies: list[CompanyRecord]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CompanyRecord.csv_headers())
        writer.writeheader()
        for company in companies:
            writer.writerow(company.as_row())


def insert_hits(connection: sqlite3.Connection, listings: list[Listing]) -> None:
    connection.executemany(
        """
        INSERT INTO hits (
          source_site, query_type, query, listing_url, listing_title, supplier_name,
          supplier_country, price, quantity, snippet, scraped_at, raw_html_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                item.source_site,
                item.query_type,
                item.query,
                item.listing_url,
                item.listing_title,
                item.supplier_name,
                item.supplier_country,
                item.price,
                item.quantity,
                item.snippet,
                item.scraped_at,
                item.raw_html_path,
            )
            for item in listings
        ],
    )
    connection.commit()


def insert_companies(connection: sqlite3.Connection, companies: list[CompanyRecord]) -> None:
    connection.executemany(
        """
        INSERT INTO companies (
          raw_supplier_name, crustdata_status, company_name, company_website, linkedin_url,
          hq_country, hq_city, industry, employee_count, description, raw_response_json, enriched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                item.raw_supplier_name,
                item.crustdata_status,
                item.company_name,
                item.company_website,
                item.linkedin_url,
                item.hq_country,
                item.hq_city,
                item.industry,
                item.employee_count,
                item.description,
                item.raw_response_json,
                item.enriched_at,
            )
            for item in companies
        ],
    )
    connection.commit()


def load_hits_from_csv(path: Path) -> list[Listing]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [Listing(**row) for row in reader]


def run_scrape(
    selected_sites: list[str] | None,
    selected_query: str | None,
    dry_run: bool,
    logger: logging.Logger,
    skipped_logger: logging.Logger,
) -> tuple[list[Listing], Counter]:
    configured_sites, keywords, cas_numbers = parse_targets(TARGETS_FILE)
    scrapers = make_scrapers(logger, skipped_logger)
    queries = keywords + cas_numbers
    if selected_query:
        queries = [query for query in queries if query == selected_query]
    active_sites = selected_sites or list(configured_sites.keys())
    plan = [(site, query) for site in active_sites for query in queries]

    if dry_run:
        for site, query in plan:
            print(f"{site}\t{query}")
        return [], Counter()

    listings: list[Listing] = []
    per_site_hits: Counter = Counter()
    skipped_sites: set[str] = set()

    for site in active_sites:
        scraper = scrapers.get(site)
        if scraper is None:
            logger.warning("Unknown site slug: %s", site)
            continue
        try:
            scraper.check_robots_txt()
            scraper.is_homepage_accessible()
        except SkipSite as exc:
            scraper.log_skip(str(exc))
            skipped_sites.add(site)
            logger.info("Skipping %s: %s", site, exc)
            continue

        for query in queries:
            try:
                results = scraper.search(query)
            except SkipSite as exc:
                scraper.log_skip(f"query={query}\t{exc}")
                logger.info("%s query skipped for %s: %s", site, query, exc)
                continue
            except Exception as exc:  # pragma: no cover - defensive path
                scraper.log_skip(f"query={query}\tunhandled error: {exc}")
                logger.warning("%s query failed for %s: %s", site, query, exc)
                continue

            listings.extend(results)
            per_site_hits[site] += len(results)
            logger.info("%s %s -> %s hits", site, query, len(results))

    for scraper in scrapers.values():
        scraper.client.close()
    return listings, per_site_hits


def run_enrichment(
    logger: logging.Logger,
    skipped_logger: logging.Logger,
    supplier_names: list[str],
) -> list[CompanyRecord]:
    load_dotenv(ROOT.parent / ".env")
    api_key = os.getenv("CRUSTDATA_API_KEY")
    if not api_key:
        skipped_logger.info("crustdata\tmissing CRUSTDATA_API_KEY")
        logger.info("Skipping enrichment: CRUSTDATA_API_KEY missing")
        return []

    unique_names = []
    seen = set()
    for name in supplier_names:
        cleaned = name.strip()
        if not cleaned or cleaned in seen:
            continue
        unique_names.append(cleaned)
        seen.add(cleaned)

    client = CrustdataClient(api_key, logger)
    companies: list[CompanyRecord] = []
    try:
        for index, supplier_name in enumerate(unique_names):
            if index >= CRUSTDATA_CAP:
                companies.append(client.budget_skipped(supplier_name))
                continue
            companies.append(client.enrich_company(supplier_name))
    finally:
        client.close()
    return companies


def summarize_companies(companies: list[CompanyRecord]) -> Counter:
    return Counter(company.crustdata_status for company in companies)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", choices=[
        "tradeford", "chinaexporter", "ecrobot", "chemnet", "indiamart_export"
    ])
    parser.add_argument("--query")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-enrich", action="store_true")
    parser.add_argument("--enrich-only", action="store_true")
    args = parser.parse_args()

    ensure_dirs()
    logger, skipped_logger = configure_logging()
    started_at = time.monotonic()

    connection = init_db(DB_PATH)
    listings: list[Listing] = []
    per_site_hits: Counter = Counter()

    if not args.enrich_only:
        listings, per_site_hits = run_scrape(
            selected_sites=[args.site] if args.site else None,
            selected_query=args.query,
            dry_run=args.dry_run,
            logger=logger,
            skipped_logger=skipped_logger,
        )
        if args.dry_run:
            connection.close()
            return 0
        write_hits_csv(HITS_CSV_PATH, listings)
        insert_hits(connection, listings)
    else:
        listings = load_hits_from_csv(HITS_CSV_PATH)
        insert_hits(connection, listings)

    companies: list[CompanyRecord] = []
    if not args.no_enrich:
        supplier_names = [item.supplier_name for item in listings if item.supplier_name]
        companies = run_enrichment(logger, skipped_logger, supplier_names)
    write_companies_csv(COMPANIES_CSV_PATH, companies)
    insert_companies(connection, companies)

    company_summary = summarize_companies(companies)
    runtime = time.monotonic() - started_at
    logger.info("Run complete")
    logger.info("Hits per site: %s", dict(per_site_hits))
    logger.info("Unique suppliers enriched: %s", len(companies))
    logger.info("Crustdata summary: %s", dict(company_summary))
    logger.info("Total runtime: %.2fs", runtime)
    connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
