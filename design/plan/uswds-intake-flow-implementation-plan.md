# USWDS Implementation Plan For Intake And Processing Screens

## Goal

Implement the two provided screens as the new investigation intake flow in the Rails app, using the local USWDS design assets in `design/usa-design-system/uswds-for-designers-v.3.0.0` as the visual source of truth.

The result should:

- replace the current direct-to-workspace root experience with a structured intake entry point
- preserve the existing investigation workspace as the destination after intake
- use USWDS conventions, spacing, typography, and form components instead of the current custom dark dashboard styling
- support the two states shown in the mockups:
  - intake form
  - processing/progress state

## Current State

The current app routes `root` to `InvestigationsController#show` with `id: "current"`.

Relevant existing pieces:

- [config/routes.rb](/Users/carsonmulligan/Desktop/Workspace/apps/c0mpiled-hackathon/config/routes.rb:1)
- [app/controllers/investigations_controller.rb](/Users/carsonmulligan/Desktop/Workspace/apps/c0mpiled-hackathon/app/controllers/investigations_controller.rb:1)
- [app/views/investigations/show.html.erb](/Users/carsonmulligan/Desktop/Workspace/apps/c0mpiled-hackathon/app/views/investigations/show.html.erb:1)
- [app/views/layouts/application.html.erb](/Users/carsonmulligan/Desktop/Workspace/apps/c0mpiled-hackathon/app/views/layouts/application.html.erb:1)
- [app/assets/tailwind/application.css](/Users/carsonmulligan/Desktop/Workspace/apps/c0mpiled-hackathon/app/assets/tailwind/application.css:1)
- [app/models/investigation.rb](/Users/carsonmulligan/Desktop/Workspace/apps/c0mpiled-hackathon/app/models/investigation.rb:1)
- [app/models/source.rb](/Users/carsonmulligan/Desktop/Workspace/apps/c0mpiled-hackathon/app/models/source.rb:1)

Important constraint: the current schema only supports `investigations.name`, `investigations.description`, and simple `sources` rows. The mockup implies richer intake data than the app currently stores.

## Visual Direction From USWDS

Use the local USWDS kit as guidance, especially these patterns:

- authentication-style centered form layout
- `Form`, `Textarea`, `File input`, `Combo box`, `Button`, `Process list`, and `Step indicator` components
- Public Sans for primary UI text
- neutral background, clear borders, high contrast text, restrained color usage

Do not copy the current app’s dark `Holocron` workspace styles into these screens. The intake flow should feel like a government product using USWDS defaults with minimal customization.

Recommended token direction:

- page background: white or `gray-5`
- text: `ink` or equivalent dark neutral
- border: light neutral gray
- primary action: USWDS primary button blue
- focus states: USWDS default focus ring behavior

## Screen 1: Intake Form

### Purpose

Capture the user’s objective, optional context artifacts, and an optional pre-existing module before starting investigation setup.

### Layout

Build a centered single-column form container with generous vertical spacing.

Recommended structure:

1. page wrapper with centered content and max width around USWDS desktop form widths
2. large heading: `Welcome back, Frances`
3. form section with:
   - objective prompt
   - objective textarea
   - context upload block
   - links input
   - module select
   - primary continue button aligned right

### Component Mapping

Map each visible element to a USWDS-style implementation:

- heading: large display/heading scale using Public Sans
- "What do you need to do?": standard `usa-label`
- large objective field: `usa-textarea`
- file area:
  - prefer native USWDS `File input`
  - if drag-and-drop is needed, wrap the input in a custom dropzone shell while preserving an accessible native file input
- "Paste links here": `usa-input`
- module selector:
  - use a USWDS `Combo box` if searchable behavior is desired
  - otherwise use `usa-select`
- continue: `usa-button`

### Data Mapping

Translate the form into persisted app data with the smallest viable schema extension.

Recommended initial mapping:

- objective textarea -> `investigations.description`
- heading/name:
  - derive a short name from the first line or first sentence of the objective
  - or add a dedicated `title` field later if naming quality becomes a problem
- pasted links:
  - create `Source` rows of `kind: "url"`
- uploaded files:
  - if file storage is implemented now, attach files through Active Storage and create corresponding `Source` records
  - if file storage is deferred, still plan the UI and controller boundary so this can be added cleanly
- selected module:
  - short-term: store in `investigations.metadata` only if a JSON field is added
  - fallback: add a hidden note `Source` row describing the selected module

### Recommended Validation

- objective is required
- links are optional
- module is optional
- files are optional
- continue button disabled only when objective is blank or submission is in progress

## Screen 2: Processing State

### Purpose

Show the user that the system is turning the intake into an investigation workspace and ingesting supporting material.

### Layout

Use a centered narrow content column with strong whitespace and a simple vertical progression.

Preferred visual implementation:

- one vertical spine line
- numbered step circles aligned left
- step title aligned right of each marker
- optional sub-bullets under active or completed steps

This should be implemented with USWDS `Process list` styling, lightly adapted to match the mockup.

### Suggested Step Model

Persist or compute a transient step list like:

1. `Parsing objective`
2. `Ingesting context`
3. `Connecting to module`

Optional later expansion:

4. `Creating source records`
5. `Preparing workspace`

### Behavior

Short-term implementation can be synchronous with a temporary status page:

1. submit intake form
2. create investigation and related source records
3. render processing view immediately
4. redirect to `investigation_path(@investigation)` after a short simulated delay or Turbo polling cycle

Preferred implementation if time allows:

- create an `InvestigationSetupJob`
- store status on the investigation
- poll from the processing screen until the job reaches complete
- redirect to the workspace when done

## Route And Flow Changes

Recommended route design:

- `root` -> new welcome/intake action
- `GET /investigations/new` -> intake form
- `POST /investigations` -> creates intake payload and redirects to processing
- `GET /investigations/:id/setup` -> processing screen
- existing `GET /investigations/:id` remains the workspace

Controller approach:

- keep `InvestigationsController`
- add `setup` member action
- update `create` so it builds the investigation from intake inputs rather than creating a blank investigation

Suggested route sketch:

```ruby
resources :investigations do
  member do
    get :setup
  end
end

root "investigations#new"
```

## Files To Create Or Change

Expected implementation touch points:

- `config/routes.rb`
- `app/controllers/investigations_controller.rb`
- `app/views/investigations/new.html.erb`
- `app/views/investigations/setup.html.erb`
- `app/views/layouts/application.html.erb`
- `app/assets/tailwind/application.css`

Possible additional touch points:

- `app/models/investigation.rb`
- `app/models/source.rb`
- new migration if adding metadata/status fields
- Active Storage install/migrations if file uploads are implemented now

## CSS Strategy

Do not hand-build a one-off visual system for these screens. Create a thin layer of reusable utility classes that express USWDS-like structure.

Recommended approach:

- keep Tailwind as the implementation mechanism
- replace Google `Inter`/`JetBrains Mono` usage in the intake flow with USWDS-appropriate typography, ideally Public Sans
- add a small set of classes such as:
  - `.usa-page`
  - `.usa-form-card`
  - `.usa-field-group`
  - `.usa-process-stack`
  - `.usa-step-marker`

If the rest of the app remains dark for now, scope the new intake styles to these views so the workspace can be migrated later without blocking this work.

## Accessibility Requirements

The implementing agent should treat these as required, not optional:

- every form control has a visible label
- helper text is associated properly
- file upload has keyboard-accessible native input behavior
- focus outlines remain visible
- color is not the only indicator of progress
- processing step numbers are readable to screen readers
- button and form controls meet target size expectations

## Implementation Order

Recommended sequence for the agent:

1. Add the new route structure and controller actions.
2. Build the intake form view with static options and no upload persistence yet.
3. Wire `create` to persist objective, links, and module selection in the simplest stable format.
4. Build the processing screen using a USWDS-style process list.
5. Add redirect logic from processing to the existing workspace.
6. Refine typography, spacing, and alignment against the mockups.
7. Add file persistence if time and schema allow.

## Explicit Decisions To Keep Scope Tight

These choices keep implementation practical for one agent pass:

- do not redesign the existing investigation workspace in the same change
- do not block on a full USWDS package install if the repo already relies on Tailwind; mimic USWDS faithfully in structure and tokens using local assets as reference
- do not over-model modules before there is a stable module domain model
- do not make drag-and-drop the only upload path

## Acceptance Criteria

The implementation is done when:

- visiting `/` shows a USWDS-style intake form matching Image 1 closely
- submitting a valid objective creates an investigation from the form input
- the user lands on a processing screen matching Image 2 closely
- the processing screen transitions into the existing investigation workspace
- the new views use USWDS typography, spacing, and form conventions rather than the current dark dashboard styling
- the flow works on desktop and mobile widths without layout breakage

## Notes For The Implementing Agent

- Prefer the USWDS Figma kit in `design/usa-design-system/uswds-for-designers-v.3.0.0/figma/uswds-design-kit-beta.fig` as the most current local reference.
- Use `Process list` rather than inventing a custom progress widget from scratch.
- Preserve the current workspace behavior after setup completes.
- If schema expansion is required, keep it minimal and justify each field in the PR summary.
