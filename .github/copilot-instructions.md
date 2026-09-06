# Copilot instructions

Work on this repository with behavior-preserving, surgical changes.

## Safety

- Do not change trading behavior, order semantics, reconciliation semantics, DB schema, or env-var meanings unless the task explicitly requires it.
- Keep KR and US code paths isolated.
- Never hardcode ticker exceptions or allow/deny lists in production code.
- Prefer thin compatibility wrappers when moving code.
- Add characterization tests before moving code when coverage is weak.

## Ownership boundaries

- KR PB1, KR Infinite, US PB1/standard, TQQQ Infinite, shared execution/reconciliation, and reporting are separate ownership areas.
- Do not mix rules between owners.
- US code stays under `trader/us/**` and `tests/us/**`.
- US tables use `us_` prefixes only.
- KR code must not touch `us_` tables directly.

## Refactor workflow

1. Identify the production entry point, callers, DB reads/writes, broker calls, and state mutation.
2. Preserve signatures and side-effect order where possible.
3. Move code in small checkpoints.
4. Run targeted tests for the moved area, then broader relevant tests.
5. Validate diffs before finalizing.

## Completion expectations

- Keep changes reversible.
- Avoid unrelated cleanup during refactors.
- Record potential behavioral bugs without changing baseline behavior unless explicitly instructed.
