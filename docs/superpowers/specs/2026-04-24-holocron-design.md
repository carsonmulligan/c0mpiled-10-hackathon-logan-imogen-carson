# Holocron — Design Spec
**Date:** 2026-04-24  
**Status:** Approved

---

## Overview

Holocron is a hackathon demo app for counter-narcotics investigators. Two government offices (DEA and FBI) share a live data war room showing precursor chemical seizure records. The primary demo moment is: an analyst in one office flags or annotates a record, and the other office sees it instantly — no page reload.

All data is seeded dummy data. Nothing is real. Speed of demo impact over correctness of implementation.

---

## Stack

| Layer | Choice | Reason |
|-------|--------|--------|
| Framework | Rails 8 | Ships with Hotwire by default |
| Ruby | 3.3 | LTS, matches Rails 8 recommendation |
| Database | SQLite | Zero-config for demo |
| Real-time | Action Cable + Turbo Streams | Native Hotwire — no extra deps |
| Frontend | Hotwire (Turbo + Stimulus) + Tailwind CDN | No build step, ships fast |
| Map | Leaflet.js via CDN | Lightweight, free, no API key needed |
| Auth | `has_secure_password` | No Devise — minimal deps |

---

## Models

### User
```
id, name, agency (DEA | FBI), email, password_digest
```
Seeded with two hardcoded accounts:
- `dea@holocron.gov` / `password` → DEA (Office A)
- `fbi@holocron.gov` / `password` → Office B (FBI)

### ChemicalRecord
```
id, name, compound, quantity_kg, origin_country, supplier,
status (active | seized | under_review), flagged (boolean),
lat, lng, notes, created_at
```
Seeded with ~20 dummy precursor chemical records with realistic-sounding dummy data and global coordinates.

### Annotation
```
id, chemical_record_id, user_id, body, created_at
```
The action that drives the live war room feed. On create, broadcasts a Turbo Stream to all connected sessions.

---

## Real-Time Architecture

Following `bcast-model-broadcasts` and `stream-action-selection` rules:

- `Annotation` model calls `broadcasts_to` scoped to a shared channel (all users)
- `ChemicalRecord` uses `broadcasts_refreshes` when `flagged` is toggled
- War room feed is a `<turbo-stream-source>` connected to the shared channel
- Stream actions used: `prepend` (new annotations), `replace` (flag status on a row)

Per `bcast-scope-streams`: stream name is `war_room` — shared across all sessions (intentional for demo, both offices see everything).

Per `bcast-debounce-n1`: no batch operations that would trigger storms; each annotation is a single create.

---

## UI Layout

```
┌─────────────────────────────────────────────────────────┐
│  HOLOCRON    [DEA — Office A]              [Logout]      │  top nav
├──────────────────────────┬──────────────────────────────┤
│                          │                              │
│   Leaflet Map            │   ChemicalRecord grid        │
│   Pins = seizure locs    │   Columns: Name, Compound,   │
│   Click pin = highlight  │   Qty, Origin, Status, Flag  │
│   matching grid row      │   [Flag] button per row      │
│                          │   Flagged rows = red accent  │
│                          │   Sortable by column header  │
├──────────────────────────┴──────────────────────────────┤
│  WAR ROOM FEED  (Turbo Stream target: war_room_feed)    │
│  ● DEA flagged "Acetic Anhydride" — 14:32               │
│  ● FBI added note: "Confirmed supplier link" — 14:28    │
└─────────────────────────────────────────────────────────┘
```

**Theme:** Dark background (`#0a0e1a`), monospace data font (JetBrains Mono via Google Fonts), government-grey panel borders, red/amber alert accents, subtle grid lines. Palantir-adjacent without copying.

---

## Pages / Routes

| Route | Controller#Action | Description |
|-------|-------------------|-------------|
| `GET /` | `sessions#new` | Login page |
| `POST /sessions` | `sessions#create` | Authenticate |
| `DELETE /sessions` | `sessions#destroy` | Logout |
| `GET /dashboard` | `dashboard#index` | Main war room (map + grid + feed) |
| `GET /chemical_records` | `chemical_records#index` | JSON for grid (Turbo Frame) |
| `POST /chemical_records/:id/flag` | `chemical_records#flag` | Toggle flagged, broadcasts refresh |
| `POST /chemical_records/:id/annotations` | `annotations#create` | Add annotation, broadcasts to feed |

---

## Stimulus Controllers

Per `stim-small-reusable-controllers`:

- `map-controller` — initializes Leaflet, renders pins from JSON data attribute, highlights grid row on pin click
- `grid-filter-controller` — client-side column sort and text filter (no server round-trip)
- `annotation-form-controller` — submit annotation inline, clear input after submit

---

## Seed Data

20 precursor chemical records with:
- Realistic compound names (Acetic Anhydride, Ephedrine, Pseudoephedrine, Red Phosphorus, etc.)
- Fictional supplier names
- Coordinates spread across Mexico, Central America, Southeast Asia, West Africa
- Mix of statuses (active, seized, under_review)
- ~5 pre-flagged

---

## What Is NOT Real

Everything. Specifically:
- No actual web scraping (placeholder button only)
- No real agency integration
- No real user management
- No file upload or PDF generation (placeholder buttons)
- Map data is seeded, not live

---

## Success Criteria for Demo

1. Login as DEA → see map + data grid + empty war room feed
2. Flag a record → red highlight appears on row; feed shows "[DEA] flagged X"
3. In another browser tab, login as FBI → same flag update visible without reload
4. Add an annotation → appears in both sessions' war room feeds in real time
