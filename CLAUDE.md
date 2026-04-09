# GridPulse — Claude Code Instructions

## Start of Every Session
1. Read `CONTINUITY.md` first — this is your memory of current state
2. Read `GRIDPULSE_BUILD_PLAN_v5.md` before writing any code
3. Read existing code files before modifying or extending them
4. Never answer questions about the codebase without reading the actual files first

## Phase Discipline
- Work on the current phase only — do not work ahead
- Implement only what the current phase specifies
- After writing code, run the phase test file(s) and fix all failures before stopping
- All prior phase tests must continue to pass when extending existing files

## Code Rules
- Prefer simple, explicit code over clever abstractions
- Prefer plain functions over base classes, mixins, or strategy patterns
- No async FastAPI endpoints unless the plan explicitly requires it
- No new dependencies not already in `requirements.txt`
- No features, tables, or endpoints not specified in the build plan
- No file longer than 300 lines — split into modules if needed
- Do not add empty try/except blocks
- Do not add default fallbacks that hide failures — let errors surface

## `run_ingestion()` Rule
Never rewrite `run_ingestion()` from scratch. Only extend it with what the current phase requires. Rewriting it will break prior phase tests.

## Tests
- Never hit the live EIA API in tests — use fixtures and monkeypatch
- Never hit AWS in tests — mock S3
- All tests must pass without live network access

## Before Stopping
- Run the current phase test file
- Fix any failures
- Confirm all prior phase tests still pass
- Update `CONTINUITY.md` with current state

---

## Continuity Ledger

Maintain a file called `CONTINUITY.md` in the project root.
This file is your working memory. It survives context compaction. Always read it first. Always update it when something meaningful changes.

### What to track

```
## Goal
One sentence — what this project is and what done looks like.

## Current Phase
Phase X — <name>

## State
- Done: <bullet list of completed phases>
- Now: <what is actively being worked on>
- Next: <next phase or step>

## Decisions
D001: <a durable decision that should never be second-guessed, e.g. "use requests not httpx">
D002: ...

## Open Questions
- anything unresolved that needs a human decision

## Working Set
Key files currently relevant to the active phase (max 12 paths).

## Recent Changes
Last 5-10 meaningful things that happened (date + what changed).
```

### Rules
- Read it at the start of every turn
- Update it only when something meaningful changes — phase completion, a decision made, a blocker found
- Keep it short and factual — no transcripts, no raw logs
- If you don't know something, write UNCONFIRMED — never guess
- Compress older entries when sections get too long

### Initial state
If `CONTINUITY.md` does not exist yet, create it now with the current state before doing anything else.
