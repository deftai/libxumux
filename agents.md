<!-- deft:managed-section v1 -->
# Deft — AI Development Framework

Deft is installed in directive/. Full guidelines: directive/main.md

! If any directive/skills/ path referenced in this file cannot be read (missing file, stale path from a previous framework version, or a deprecation redirect stub), read directive/QUICK-START.md instead and follow it. QUICK-START refreshes this section idempotently for the current framework version.

## Pre-Cutover Check (run before First Session / Returning Sessions)

! Before the First Session / Returning Sessions checks below, detect whether this project pre-dates the v0.20 vBRIEF-centric model. If it does, migration MUST happen before any Phase 1, Phase 2, or Returning-Sessions routing fires.

**Pre-cutover detected** if ANY of the following are true:

- ./SPECIFICATION.md exists and its first 200 characters do NOT contain <!-- deft:deprecated-redirect -->
- ./PROJECT.md exists and its first 200 characters do NOT contain <!-- deft:deprecated-redirect -->
- ./vbrief/ exists but any of the five lifecycle subfolders (proposed/, pending/, active/, completed/, cancelled/) is missing

→ On detection: read directive/skills/deft-directive-setup/SKILL.md "Pre-Cutover Detection Guard" section and follow the migration path BEFORE any other action.

⊗ Start Phase 1, Phase 2, or a Returning-Sessions workflow while pre-cutover artifacts are present — run migration first.

## First Session

Check what exists before doing anything else:

**USER.md missing** (~/.config/deft/USER.md or %APPDATA%\deft\USER.md):
→ Read directive/skills/deft-directive-setup/SKILL.md and start Phase 1 (user preferences)

**USER.md exists, PROJECT-DEFINITION.vbrief.json missing** (./vbrief/):
→ Read directive/skills/deft-directive-setup/SKILL.md and start Phase 2 (project definition)

## Returning Sessions

When all config exists: read the guidelines, your USER.md preferences, and PROJECT-DEFINITION.vbrief.json, then continue with your task.

~ Run directive/skills/deft-directive-sync/SKILL.md to pull latest framework updates and validate project files.

## Branch Policy Disclosure (#746)

When the active project's `vbrief/PROJECT-DEFINITION.vbrief.json` has `plan.policy.allowDirectCommitsToMaster = true`, the agent MUST surface the policy state at the start of any interactive session:

> "[deft policy] Direct commits to the default branch are ENABLED (source: typed). Branch-protection policy is OFF."

Override paths:
- `task policy:show` -- inspect resolved policy
- `task policy:enforce-branches` -- re-enable branch protection
- `task policy:allow-direct-commits -- --confirm` -- re-confirm opt-out (audited)
- `DEFT_ALLOW_DEFAULT_BRANCH_COMMIT=1` -- emergency env-var bypass

⊗ Begin a session that will commit/push without surfacing the policy state when allowDirectCommitsToMaster=true.

## Development Process

### Implementation Intent Gate (#810)

- ! Run `task vbrief:preflight -- <path>` before any code-writing tool call or `start_agent` dispatch.
- ! Require an explicit action-verb directive (`build`, `implement`, `ship`, `swarm`, `run agents`, `start agent`) from the user before invoking the preflight gate or `start_agent` for implementation.
- ⊗ Infer implementation intent from lifecycle vocabulary or workflow shape.
- ⊗ Treat affirmative continuation phrases (`yes`, `go`, `proceed`, `do it`) as implementation authorization unless the prior turn explicitly proposed implementation.

## Commands

- /deft:change <name>        — Propose a scoped change
- /deft:run:interview        — Structured spec interview
- /deft:run:speckit          — Five-phase spec workflow (large projects)
- /deft:run:discuss <topic>  — Feynman-style alignment
- /deft:run:research <topic> — Research before planning
- /deft:run:map              — Map an existing codebase
- directive/run bootstrap    — CLI setup (terminal users)
- directive/run spec         — CLI spec generation
<!-- /deft:managed-section -->
