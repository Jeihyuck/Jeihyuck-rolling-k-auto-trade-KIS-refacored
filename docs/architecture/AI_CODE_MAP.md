# AI Code Map

This document maps the main execution ownership boundaries for safe maintenance.

## Shared rules

- Preserve behavior first.
- Treat ACK, FILL, cancel, and balance state transitions as contract-sensitive.
- Keep KR and US implementations separate unless tests prove identical semantics.

## KR PB1

- Primary runtime surfaces: `trader/pb1_engine.py`, `trader/pb1_runner.py`
- Responsibilities: KR entry/exit planning, sell fencing, order submission, lifecycle state, balance handling
- Refactor guidance: extract helpers under `trader/kr/pb1/` with thin wrappers

## KR Infinite / 122630

- Primary runtime surfaces: KR infinite strategy and lifecycle modules
- Responsibilities: cycle state, quote handling, pending reconciliation, durable fencing
- Refactor guidance: preserve invalid-quote and reconciliation behavior exactly

## US PB1 / standard US

- Primary runtime surfaces: `trader/us/runner/*`, `trader/us/execution/*`, `trader/us/db/*`
- Responsibilities: prep, trade-am, trade-pm, trade-close, reporting, routing, ACK/FILL reconciliation
- Boundary: use `us_` tables only; do not read KR orders/positions/signals directly

## TQQQ Infinite

- Primary runtime surfaces: US infinite-style strategy and lifecycle modules
- Responsibilities: open/TTL cancel identity, lifecycle separation between open BUY and other SELL flow

## Shared execution / reconciliation

- Primary runtime surfaces: broker adapter and order routing layers
- Responsibilities: submit mechanics, response interpretation, fill reconciliation, durable state updates
- Rule: preserve broker parameters and event ordering

## Reporting

- Primary runtime surfaces: daily/summary report runners and report generators
- Responsibilities: read-only aggregation, formatting, and delivery
- Rule: reporting must not mutate trading state

## Recommended maintenance order

1. Add characterization tests.
2. Identify owner boundary.
3. Move pure helpers first.
4. Extract submission and lifecycle logic next.
5. Keep compatibility wrappers until callers are migrated.
