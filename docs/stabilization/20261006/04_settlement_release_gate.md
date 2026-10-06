# Phase 4 — KR/US settlement integrity, production replay and manual release gate

Stack: PR164 -> PR165 -> PR166 -> this PR. Neither this PR nor its merge activates production common settlement writes.

## Purpose
Prevent another KIS/DB mismatch from being called a success. Define database-backed audit rules, release requirements, operator-cutover conditions and PostgreSQL race testing. Keep both markets and all strategy owners isolated.

## Evidence requirements to approve future live-writer cutover
- Additive migration 0055 applied and reconciled in isolated PostgreSQL.
- Actual production runner KR/US replay fixtures demonstrate KIS timeout, ACK lost, partial+cancel+later execution, cross-day restart, price unresolved/later price, attribution conflict.
- US synthetic balance deltas never compete with actual KIS individual executions.
- Original epoch, owner, client order key, cycle, and broker trade day bind to the exact existing lifecycle; historical pre-cutover positions are proof-seeded (never guessed).
- One and only one economic position writer exists for any order. The old KR / US position writer must be disabled specifically for migrated orders before common writer can become active.
- Authoritative and complete broker holdings confirmed, zero unattributed actual fills and unresolved claims for the intended cutover scope.
- 5 actual open-market days of KR shadow and 5 of US shadow are compared; after activation observe each for at least 5 more actual trade days.
- Explicit operator approval pinned to an exact deployed revision; no same-session restart and no live trading from tests.

## What this PR actually implements
1. `inspect_settlement_ledger`: read-only scoped consistency check.
2. `assess_release`: fail-closed feature-gated decision that refuses empty unseeded ledgers, unresolved PnL, missing broker truth or absent operator sign-off.
3. `scripts/check_settlement_integrity.py`: read-only diagnostic exit status 2 for not migrated, pending or inconsistent.
4. `tests/test_settlement_postgres_concurrency.py` plus replay/fault regressions.
5. Dedicated GitHub CI for both markets.

## Explicit remaining blocker
**Live-writer cutover is NOT implemented or activated by creating/merging this PR.**
The existing KR/US economic writers still own production orders and fills. After
shadow review, a separate operator-approved market-by-market cutover implementation
must replace them with Connection-bound adapters. Turning on
`NULLIM_SETTLEMENT_WRITER_ENABLED` without such a verified cutover does not route
a real order into this new ledger.

This document is intentionally explicit: code/CI green alone cannot certify a
production rollout, and a draft PR should never be represented as MERGE READY.
