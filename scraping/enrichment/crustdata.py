from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed


@dataclass
class CompanyRecord:
    raw_supplier_name: str
    crustdata_status: str
    company_name: str | None
    company_website: str | None
    linkedin_url: str | None
    hq_country: str | None
    hq_city: str | None
    industry: str | None
    employee_count: int | None
    description: str | None
    raw_response_json: str | None
    enriched_at: str

    @classmethod
    def csv_headers(cls) -> list[str]:
        return [field.name for field in fields(cls)]

    def as_row(self) -> dict[str, str | int | None]:
        return asdict(self)


class CrustdataClient:
    endpoint = "https://api.crustdata.com/screener/companydb/search"

    def __init__(self, api_key: str, logger: logging.Logger) -> None:
        self.logger = logger
        self.client = httpx.Client(
            timeout=20.0,
            follow_redirects=True,
            headers={
                "Authorization": f"Token {api_key}",
                "Content-Type": "application/json",
                "User-Agent": "Holocron-Research-Bot/0.1 (+hackathon)",
            },
        )

    def close(self) -> None:
        self.client.close()

    @retry(
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
        stop=stop_after_attempt(2),
        wait=wait_fixed(1.0),
        reraise=True,
    )
    def _post(self, payload: dict) -> httpx.Response:
        response = self.client.post(self.endpoint, json=payload)
        if response.status_code >= 500:
            response.raise_for_status()
        return response

    def enrich_company(self, supplier_name: str) -> CompanyRecord:
        payload = {
            "filters": {
                "filter_type": "company_name",
                "type": "=",
                "value": supplier_name,
            },
            "limit": 3,
        }
        try:
            response = self._post(payload)
            time.sleep(0.3)
        except Exception as exc:
            self.logger.warning("Crustdata lookup failed for %s: %s", supplier_name, exc)
            return self._error_record(supplier_name)

        if response.status_code >= 400:
            self.logger.warning(
                "Crustdata returned %s for %s", response.status_code, supplier_name
            )
            return self._error_record(supplier_name)

        data = response.json()
        results = self._extract_results(data)
        if not results:
            return CompanyRecord(
                raw_supplier_name=supplier_name,
                crustdata_status="not_found",
                company_name=None,
                company_website=None,
                linkedin_url=None,
                hq_country=None,
                hq_city=None,
                industry=None,
                employee_count=None,
                description=None,
                raw_response_json=json.dumps(data, ensure_ascii=True),
                enriched_at=datetime.now(timezone.utc).isoformat(),
            )

        top = results[0]
        return CompanyRecord(
            raw_supplier_name=supplier_name,
            crustdata_status="matched",
            company_name=self._pick(top, "company_name", "name"),
            company_website=self._pick(top, "company_website", "website", "domain"),
            linkedin_url=self._pick(top, "linkedin_url", "linkedin_company_url"),
            hq_country=self._pick(top, "hq_country"),
            hq_city=self._pick(top, "hq_city"),
            industry=self._pick(top, "industry", "linkedin_industries"),
            employee_count=self._employee_count(top),
            description=self._pick(top, "description"),
            raw_response_json=json.dumps(top, ensure_ascii=True),
            enriched_at=datetime.now(timezone.utc).isoformat(),
        )

    def budget_skipped(self, supplier_name: str) -> CompanyRecord:
        return CompanyRecord(
            raw_supplier_name=supplier_name,
            crustdata_status="budget_skipped",
            company_name=None,
            company_website=None,
            linkedin_url=None,
            hq_country=None,
            hq_city=None,
            industry=None,
            employee_count=None,
            description=None,
            raw_response_json=None,
            enriched_at=datetime.now(timezone.utc).isoformat(),
        )

    def _error_record(self, supplier_name: str) -> CompanyRecord:
        return CompanyRecord(
            raw_supplier_name=supplier_name,
            crustdata_status="error",
            company_name=None,
            company_website=None,
            linkedin_url=None,
            hq_country=None,
            hq_city=None,
            industry=None,
            employee_count=None,
            description=None,
            raw_response_json=None,
            enriched_at=datetime.now(timezone.utc).isoformat(),
        )

    def _extract_results(self, payload: dict) -> list[dict]:
        for key in ("results", "companies", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []

    def _pick(self, payload: dict, *keys: str) -> str | None:
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                value = ", ".join(str(item) for item in value if item is not None)
            if value not in (None, ""):
                return str(value)
        return None

    def _employee_count(self, payload: dict) -> int | None:
        direct = payload.get("employee_count")
        if isinstance(direct, int):
            return direct
        metrics = payload.get("employee_metrics")
        if isinstance(metrics, dict):
            latest = metrics.get("latest_count")
            if isinstance(latest, int):
                return latest
        return None
