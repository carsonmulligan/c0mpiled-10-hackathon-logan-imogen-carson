from __future__ import annotations

import logging
import re
import time
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

try:
    from selectolax.parser import HTMLParser  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - environment fallback
    from bs4 import BeautifulSoup

    class _SoupNode:
        def __init__(self, node) -> None:
            self._node = node

        @property
        def attributes(self) -> dict[str, str]:
            attrs = {}
            for key, value in self._node.attrs.items():
                if isinstance(value, list):
                    attrs[key] = " ".join(str(item) for item in value)
                else:
                    attrs[key] = str(value)
            return attrs

        def css(self, selector: str) -> list["_SoupNode"]:
            return [_SoupNode(item) for item in self._node.select(selector)]

        def css_first(self, selector: str) -> "_SoupNode | None":
            match = self._node.select_one(selector)
            return _SoupNode(match) if match else None

        def text(self) -> str:
            return self._node.get_text(" ", strip=True)

    class HTMLParser:  # type: ignore[override]
        def __init__(self, html: str) -> None:
            self._soup = BeautifulSoup(html, "lxml")

        def css(self, selector: str) -> list[_SoupNode]:
            return [_SoupNode(item) for item in self._soup.select(selector)]

USER_AGENT = "Holocron-Research-Bot/0.1 (+hackathon)"
SITE_DELAY_SECONDS = 1.5


class SkipSite(Exception):
    pass


@dataclass
class Listing:
    source_site: str
    query_type: str
    query: str
    listing_url: str
    listing_title: str
    supplier_name: str | None
    supplier_country: str | None
    price: str | None
    quantity: str | None
    snippet: str | None
    scraped_at: str
    raw_html_path: str | None

    @classmethod
    def csv_headers(cls) -> list[str]:
        return [field.name for field in fields(cls)]

    def as_row(self) -> dict[str, str | None]:
        return asdict(self)


class BaseScraper:
    slug = ""
    base_url = ""

    def __init__(
        self,
        client: httpx.Client,
        sample_root: Path,
        logger: logging.Logger,
        skipped_logger: logging.Logger,
    ) -> None:
        self.client = client
        self.sample_root = sample_root
        self.logger = logger
        self.skipped_logger = skipped_logger
        self._last_request_at_by_host: dict[str, float] = {}
        self._search_template: str | None = None
        self._search_param: str = "q"

    def homepage(self) -> str:
        return self.base_url

    def search(self, query: str) -> list[Listing]:
        raise NotImplementedError

    def log_skip(self, reason: str) -> None:
        self.skipped_logger.info("%s\t%s", self.slug, reason)

    def is_homepage_accessible(self) -> None:
        self.fetch(self.homepage())

    def check_robots_txt(self) -> None:
        robots_url = urljoin(self.base_url, "/robots.txt")
        try:
            response = self.fetch(robots_url, allow_not_found=True)
        except SkipSite:
            raise
        except Exception as exc:  # pragma: no cover - defensive path
            self.logger.info("%s robots.txt check failed: %s", self.slug, exc)
            return
        if response is None or response.status_code == 404:
            return
        body = response.text.lower()
        if "user-agent: *" in body and "disallow: /" in body:
            raise SkipSite("robots.txt disallows all crawling")

    def build_listing(
        self,
        query: str,
        query_type: str,
        listing_url: str,
        listing_title: str,
        supplier_name: str | None = None,
        supplier_country: str | None = None,
        price: str | None = None,
        quantity: str | None = None,
        snippet: str | None = None,
        raw_html_path: str | None = None,
    ) -> Listing:
        return Listing(
            source_site=self.slug,
            query_type=query_type,
            query=query,
            listing_url=listing_url,
            listing_title=self.clean_text(listing_title) or listing_url,
            supplier_name=self.clean_text(supplier_name),
            supplier_country=self.clean_text(supplier_country),
            price=self.clean_text(price),
            quantity=self.clean_text(quantity),
            snippet=self.clean_text(snippet, limit=200),
            scraped_at=datetime.now(timezone.utc).isoformat(),
            raw_html_path=raw_html_path,
        )

    def save_sample(self, query: str, html: str) -> str:
        site_dir = self.sample_root / self.slug
        site_dir.mkdir(parents=True, exist_ok=True)
        safe_query = re.sub(r"[^A-Za-z0-9._-]+", "_", query.strip()).strip("_") or "query"
        file_path = site_dir / f"{safe_query}.html"
        file_path.write_text(html, encoding="utf-8")
        return str(file_path.relative_to(self.sample_root.parent))

    @retry(
        retry=retry_if_exception_type((httpx.RequestError, httpx.RemoteProtocolError)),
        stop=stop_after_attempt(2),
        wait=wait_fixed(1.0),
        reraise=True,
    )
    def fetch(self, url: str, allow_not_found: bool = False) -> httpx.Response | None:
        host = urlparse(url).netloc
        last_at = self._last_request_at_by_host.get(host)
        if last_at is not None:
            elapsed = time.monotonic() - last_at
            if elapsed < SITE_DELAY_SECONDS:
                time.sleep(SITE_DELAY_SECONDS - elapsed)
        try:
            response = self.client.get(url)
        except httpx.RequestError as exc:
            raise SkipSite(f"network error fetching {url}: {exc}") from exc
        finally:
            self._last_request_at_by_host[host] = time.monotonic()

        if response.status_code == 404 and allow_not_found:
            return response
        if response.status_code == 403:
            raise SkipSite(f"403 for {url}")
        if response.status_code >= 500:
            response.raise_for_status()
        if response.status_code >= 400 and not allow_not_found:
            raise SkipSite(f"http {response.status_code} for {url}")
        if self.looks_js_blocked(response.text):
            raise SkipSite(f"JS or anti-bot wall for {url}")
        return response

    def looks_js_blocked(self, html: str) -> bool:
        lowered = html.lower()
        markers = [
            "enable javascript",
            "javascript required",
            "access denied",
            "captcha",
            "cloudflare",
            "attention required",
        ]
        return any(marker in lowered for marker in markers)

    def clean_text(self, value: str | None, limit: int | None = None) -> str | None:
        if value is None:
            return None
        text = re.sub(r"\s+", " ", value).strip()
        if not text:
            return None
        if limit is not None and len(text) > limit:
            return text[: limit - 1].rstrip() + "…"
        return text

    def absolute_url(self, href: str | None) -> str | None:
        if not href:
            return None
        return urljoin(self.base_url, href)

    def discover_search_template(self) -> tuple[str, str]:
        if self._search_template:
            return self._search_template, self._search_param

        homepage = self.fetch(self.homepage())
        if homepage is None:
            raise SkipSite("homepage unavailable")
        parser = HTMLParser(homepage.text)
        candidates: list[tuple[str, str]] = []

        for form in parser.css("form"):
            action = form.attributes.get("action") or self.homepage()
            method = (form.attributes.get("method") or "get").lower()
            if method != "get":
                continue
            inputs = form.css("input[name], textarea[name]")
            for input_node in inputs:
                name = input_node.attributes.get("name", "")
                lowered = name.lower()
                if lowered in {"q", "query", "keyword", "keywords", "search", "wd", "k"}:
                    candidates.append((urljoin(self.base_url, action), name))

        for node in parser.css("a[href]"):
            href = self.absolute_url(node.attributes.get("href"))
            if not href:
                continue
            parsed = urlparse(href)
            params = parse_qs(parsed.query)
            for param_name in params:
                lowered = param_name.lower()
                if lowered in {"q", "query", "keyword", "keywords", "search", "wd", "k"}:
                    base = parsed._replace(query="").geturl().rstrip("?")
                    candidates.append((base, param_name))

        if not candidates:
            raise SkipSite("could not discover GET search form")

        search_url, param = candidates[0]
        self._search_template = search_url
        self._search_param = param
        return search_url, param

    def build_search_url(self, query: str) -> str:
        template, param = self.discover_search_template()
        separator = "&" if "?" in template else "?"
        return f"{template}{separator}{param}={quote_plus(query)}"

    def parse_generic_results(
        self,
        html: str,
        query: str,
        query_type: str,
        result_selectors: Iterable[str] | None = None,
        max_results: int = 10,
        raw_html_path: str | None = None,
    ) -> list[Listing]:
        parser = HTMLParser(html)
        selectors = list(result_selectors or []) + [
            ".product",
            ".products",
            ".product-item",
            ".result",
            ".results li",
            ".search-result",
            ".company-item",
            ".supplier-item",
            ".offer-item",
            "article",
            "li",
        ]
        nodes = []
        seen = set()
        for selector in selectors:
            for node in parser.css(selector):
                identifier = id(node)
                if identifier not in seen:
                    nodes.append(node)
                    seen.add(identifier)

        listings: list[Listing] = []
        seen_urls: set[str] = set()
        query_lower = query.lower()
        for node in nodes:
            anchor = None
            for selector in ["a[href]", "h1 a[href]", "h2 a[href]", "h3 a[href]"]:
                anchor = node.css_first(selector)
                if anchor:
                    break
            if anchor is None:
                continue
            href = self.absolute_url(anchor.attributes.get("href"))
            title = self.clean_text(anchor.text())
            if not href or not title:
                continue
            text = self.clean_text(node.text(), limit=400) or ""
            if query_lower not in text.lower() and query_lower not in title.lower():
                continue
            if href in seen_urls:
                continue
            supplier_name = self.extract_supplier_name(node)
            supplier_country = self.extract_field(node, ["country", "location", "origin"])
            price = self.extract_field(node, ["price", "$", "usd", "eur"])
            quantity = self.extract_field(node, ["quantity", "qty", "supply", "moq", "kg", "ton"])
            listings.append(
                self.build_listing(
                    query=query,
                    query_type=query_type,
                    listing_url=href,
                    listing_title=title,
                    supplier_name=supplier_name,
                    supplier_country=supplier_country,
                    price=price,
                    quantity=quantity,
                    snippet=text,
                    raw_html_path=raw_html_path,
                )
            )
            seen_urls.add(href)
            if len(listings) >= max_results:
                break
        return listings

    def extract_supplier_name(self, node) -> str | None:
        selectors = [
            ".company a",
            ".company-name a",
            ".company-name",
            ".supplier a",
            ".supplier",
            ".seller a",
            ".seller",
            ".member a",
            ".member",
        ]
        for selector in selectors:
            match = node.css_first(selector)
            if match:
                text = self.clean_text(match.text())
                if text:
                    return text
        text = self.clean_text(node.text()) or ""
        supplier_match = re.search(
            r"(?:Supplier|Company|Seller|Manufacturer)\s*[:|-]\s*([A-Za-z0-9&.,()/' -]{3,120})",
            text,
            flags=re.IGNORECASE,
        )
        if supplier_match:
            return self.clean_text(supplier_match.group(1))
        return None

    def extract_field(self, node, hints: list[str]) -> str | None:
        text = self.clean_text(node.text()) or ""
        lowered = text.lower()
        for hint in hints:
            if hint.lower() in lowered:
                pattern = re.compile(
                    rf"([^\n\r]{{0,40}}{re.escape(hint)}[^\n\r]{{0,80}})",
                    flags=re.IGNORECASE,
                )
                match = pattern.search(text)
                if match:
                    return self.clean_text(match.group(1), limit=120)
        return None
