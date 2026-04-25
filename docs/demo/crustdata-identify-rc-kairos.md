# Demo Fixture: Crustdata Company Identification — "RC KAIROS"

**Captured:** 2026-04-24
**Endpoint:** `POST https://api.crustdata.com/screener/identify/`
**Source docs:** [`crustdata-api/company-identification-api-docs.md`](../../crustdata-api/company-identification-api-docs.md) (lines 1429–1627)
**Use:** Cached fixture for the Holocron demo flow. Render this record in the UI from the JSON below — do **not** call the live API during demos.

---

## Demo Narrative — Why this record matters in Holocron

Francis (State Dept / ONDCP-directed analyst) is tracing how drug-precursor chemicals move through global commerce. The scraper sidecar (`scraping/`) pulls supplier names off marketplaces like Tradeford and ChemNet, then enriches each unique supplier via Crustdata so analysts get firmographics — country, industry tags, employee band, LinkedIn URL — instead of just a raw company string.

**RC KAIROS** is a representative hit for this flow:

- **Wholesale Chemical and Allied Products** industry tag — exactly the category an analyst tracking precursor flows would triage first.
- Headquartered in **Garza García, Nuevo León, Mexico** — geographically relevant to the cartel-adjacent precursor-supply story Francis is building.
- Small operation (**11–50 employees**, ~$1–2.5M est. revenue) — the "long-tail brokerage" pattern that's harder to surface without enrichment.
- Has a verified LinkedIn record but **no website on file** — illustrates the gap that makes manual analyst review necessary even after enrichment.

In the demo flow, this record sits at the seam between the scraper output (a raw supplier name) and the case workspace (a structured investigation row analysts can share). Use it as the "click into a hit" example.

---

## 1. Request

### Curl command (reproducible)

```bash
curl 'https://api.crustdata.com/screener/identify/' \
  --header 'Accept: application/json, text/plain, */*' \
  --header "Authorization: Token $CRUSTDATA_API_KEY" \
  --header 'Content-Type: application/json' \
  --data '{"query_company_name": "RC Kairos", "count": 5}'
```

> `CRUSTDATA_API_KEY` is loaded from `.env` — never inline the raw token in docs or fixtures.

### Request payload

```json
{
  "query_company_name": "RC Kairos",
  "count": 5
}
```

### Notes on the request

- `exact_match` was **not** sent, so the API used the default (`false` / fuzzy trigram match). RC Kairos was matched on the first try.
- `count: 5` was requested but only **1** record was returned — there is no near-name collision in Crustdata's index for this query.
- **No credits consumed** — the identification endpoint is free per the docs (line 1473).

---

## 2. Full JSON Response (cache this verbatim)

```json
[
  {
    "company_id": 27357628,
    "company_name": "RC KAIROS",
    "linkedin_profile_name": "RC KAIROS",
    "company_slug": "rc-kairos-91b7cdbd",
    "is_full_domain_match": false,
    "total_rows": 1,
    "company_website_domain": null,
    "company_website": null,
    "linkedin_profile_url": "https://www.linkedin.com/company/rc-kairos",
    "linkedin_profile_id": "87200798",
    "linkedin_headcount": 0,
    "employee_count_range": "11-50",
    "estimated_revenue_lower_bound_usd": 1000000,
    "estimated_revenue_upper_bound_usd": 2500000,
    "hq_country": null,
    "headquarters": "Garza García, Nuevo León, Mexico",
    "linkedin_industries": [
      "Wholesale Chemical and Allied Products",
      "Wholesale"
    ],
    "acquisition_status": null,
    "linkedin_logo_url": "https://crustdata-media.s3.us-east-2.amazonaws.com/company/065e8bd22d2a3af4fae71812b2a1212b3b0083c7b422ccb5b721aa0efc5b4f95.jpg",
    "crunchbase_profile_url": null,
    "crunchbase_total_investment_usd": null
  }
]
```

---

## 3. Field-by-Field Table (UI / slide-ready)

| API field | Demo label | Value | Notes for UI |
|---|---|---|---|
| `company_id` | Crustdata ID | `27357628` | Use as the canonical join key for any cached enrichment data |
| `company_name` | Company | `RC KAIROS` | Display in title case (`RC Kairos`) for readability |
| `linkedin_profile_name` | LinkedIn name | `RC KAIROS` | Matches `company_name` here — collapse into one field in UI |
| `company_slug` | Slug | `rc-kairos-91b7cdbd` | Internal — not for analyst display |
| `is_full_domain_match` | Domain match | `false` | We didn't query by domain; render as "—" not as a red flag |
| `total_rows` | Result count | `1` | Confidence signal: only one match exists |
| `company_website_domain` | Website | `null` | **Gap** — show "Not on file" in the UI |
| `company_website` | Website URL | `null` | Same as above |
| `linkedin_profile_url` | LinkedIn | `https://www.linkedin.com/company/rc-kairos` | Make this a clickable link |
| `linkedin_profile_id` | LinkedIn ID | `87200798` | For deep-linking to LinkedIn's numeric URL form |
| `linkedin_headcount` | Live LinkedIn headcount | `0` | Likely stale — prefer `employee_count_range` for display |
| `employee_count_range` | Size band | `11–50` | Primary "company size" chip in the UI |
| `estimated_revenue_lower_bound_usd` | Est. revenue (low) | `$1,000,000` | Render as `$1M – $2.5M` range chip |
| `estimated_revenue_upper_bound_usd` | Est. revenue (high) | `$2,500,000` | (paired with lower bound) |
| `hq_country` | HQ country | `null` | **Gap** — derive from `headquarters` string for display: Mexico |
| `headquarters` | HQ location | `Garza García, Nuevo León, Mexico` | Primary location chip |
| `linkedin_industries` | Industries | `Wholesale Chemical and Allied Products`, `Wholesale` | Render as tag chips; "Wholesale Chemicals" is the salient one for the precursor-flow demo |
| `acquisition_status` | M&A status | `null` | Hide if null in the UI |
| `linkedin_logo_url` | Logo | (S3 URL above) | Use as the avatar in the row/card |
| `crunchbase_profile_url` | Crunchbase | `null` | Hide if null |
| `crunchbase_total_investment_usd` | Total funding | `null` | Hide if null |

---

## 4. Suggested cache wiring

When implementing the cached demo flow, treat this file as the source-of-truth fixture:

- **Rails:** Drop the JSON above into `db/seeds/crustdata/rc_kairos.json` (or similar) and load it in a seed task; controller serves from the cache, never from the live API.
- **Lookup key:** Use the literal query string `"RC Kairos"` (case-insensitive) → return the JSON array above. If the demo is keyed by `company_id`, use `27357628`.
- **Consistency:** The `company_id` here (`27357628`) should be reused if/when we layer Company Enrichment data onto this same record — that endpoint accepts `company_id` directly.
