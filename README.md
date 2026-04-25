# Holocron

> A hackathon demo for turning scattered counter-narcotics research into a shared investigation workspace.

`Rails 8` `Hotwire` `Tailwind` `SQLite` `USWDS-inspired UI` `demo dataset`

Holocron is built for an analyst like Frances at State/INL: start with an objective, ingest links and context, map that input onto an existing module, and surface the entities, selectors, related cases, and behavior patterns that matter fastest. The current repo is optimized as a polished demo with seeded data, stubbed scraping/enrichment hooks, and a clear end-to-end workflow.

## Why This Matters

- Analysts start from fragmented context: links, notes, documents, spreadsheets, and marketplace traces.
- Existing workflows are slow, manual, and hard to share across offices.
- Holocron compresses that into one guided flow: intake, triage, enrichment, and investigation handoff.

## Demo Flow

```mermaid
flowchart LR
    A[Investigation objective] --> B[Add links and files]
    B --> C[Select module]
    C --> D[Parse and ingest context]
    D --> E[Match entities and selectors]
    E --> F[Link related cases]
    F --> G[Flag patterns of behavior]
    G --> H[Produce report and share]
```

## What Reviewers Should Look At

1. Start on the intake screen and enter any investigation objective.
2. Watch the setup sequence simulate parsing, scraping, and module matching.
3. Land in `Investigation Home` and click through:
   `Context Entities`, `Selectors`, `Related Cases`, and `Patterns of Behavior`.

## Workflow Screens

### 1. Intake
![Welcome page](design/workflow/1-welcome-page.png)

The analyst defines the mission, attaches context, and selects a prebuilt module to accelerate matching.

### 2. Parsing
![Tool parses input from welcome page](design/workflow/2-tool-parses-input-from-welcome-page.png)

The app stages the work as a pipeline so the user understands what is happening before the workspace opens.

### 3. Investigation Home
![Data hub main investigation](design/workflow/3-data-hub-main-investigation.png)

The core workspace summarizes what was matched and turns the investigation into a browsable operating picture.

### 4. Related Cases
![Investigation home related cases](design/workflow/4-investigation-home-related-cases.png)

Cross-case overlap is surfaced directly so teams can pivot from raw research to active investigations.

### 5. Patterns of Behavior
![Investigation home pattern of behavior](design/workflow/5-investigation-home-pattern-of-behavior.png)

The strongest demo moment is not just data collection, but the system identifying recurring tactics and tradecraft.

## System Snapshot

```mermaid
flowchart TD
    UI[Intake + Investigation UI]
    DB[(SQLite demo data)]
    SRC[Sources]
    SCR[Scrapers]
    MSG[Analyst chat]
    SHR[Office sharing]
    EXT[Future live integrations<br/>Crustdata / web scraping / exports]

    UI --> DB
    UI --> SRC
    UI --> SCR
    UI --> MSG
    UI --> SHR
    SCR -.stubbed hooks.-> EXT
    MSG -.stubbed enrichment.-> EXT
```

## Current Scope

- Live today: intake flow, setup simulation, investigation views, source records, scraper records, sharing model, seeded/demo data.
- Stubbed by design for the hackathon: external scraping, live enrichment, report generation, and publish/export actions.
- Best framing for judges: this is a strong product demo with the right architecture seams already in place.

## Run Locally

```bash
bundle install
bin/rails db:prepare db:seed
bin/dev
```

Open `http://localhost:3000`.

## Stack

- Ruby `3.3.6`
- Rails `8`
- SQLite
- Hotwire + Stimulus
- Tailwind CSS

