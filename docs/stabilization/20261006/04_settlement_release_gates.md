# Phase 4/4 — settlement integrity, replay and controlled-release gates

Stack: **PR164 -> PR165 -> PR166 -> Phase 4**. This branch targets the Phase 3 branch, *not* `dual-agent`. Never merge it before earlier phases are reviewed and merged.

## Changes implemented

- `trader/settlement/health.py`: read-only account/env/market/epoch-scoped evidence ↔ application consistency check.
- `trader/settlement/release_gate.py`: explicit multi-proof release assessment.
- `trader/settlement/audit.py`: offline account/epoch scoped ledger audit (read-only; exit 2 when unresolved). The new writer is **OFF** by default; merely setting `NULLIM_SETTLEMENT_WRITER_ENABLED=1` does not activate it.
- `tests/test_settlement_release_gates.py`: KR and US unknown initial ledger, quantity-confirmed/price-pending SELL, later-priced replay without qty reapply, mutation/anomaly detection, account/market/epoch isolation, missing-schema fail-closed and activation gate requirements.

## Status: shadow-only, not authorized for economic writer activation

The Phase 3 connectors only project a read-only comparison; they **do not** replace KR `PositionsRepo.apply_fill` or US `mark_order_filled_by_reconcile` / `save_position_snapshot`. Consequently, Phase 4's release gate is verification machinery, **not** a claim that the new writer has been cut over.

### Before activation — independently verify all ten requirements

1. Migration 0055 succeeds on an isolated PostgreSQL DB and is reversible/additive.
2. Concurrent claims for one settlement key yield one economic application under real PostgreSQL row locks, including force termination between economic update and watermark commit.
3. Replayed production KR and US fill paths reproduce known previous failures and end in correct positions, cost basis and execution-claim states.
4. Shadow per-order evidence agrees with authoritative broker executions AND legacy order/fill/position projection; `NOT_MIGRATED` never counts as a match.
5. Every pre-existing applied fill is migrated only with exact epoch, owner, order key, cycle and original broker evidence; no symbol-only or present-day holdings-only backfill.
6. The market-specific new economic writer has been implemented and connected with the SAME SQLAlchemy transaction as watermark and journal; it does not call an existing repository function which begins an independent transaction.
7. The old economic writer for the same economic order is disabled, with a verified single-writer routing switch and rollback plan.
8. KIS order details / balance are fresh, authoritative and unambiguous for the target market, account and trade date. Missing price may remain pending but must not be fabricated.
9. Runtime SHA/revision/epoch match artifacts, runner, DB and prepared release.
10. Explicit operator authorization, after market-specific dry-run and evidence review.

No environment variable, report status or CI green can independently satisfy a gate. Operators must not set `NULLIM_SETTLEMENT_WRITER_ENABLED=1` on an active runner. No real KIS requests, Supabase table changes, DB reset, or strategy modifications are part of this phase.

## Health semantics

| Status | Meaning | Operation |
|---|---|---|
| NOT_MIGRATED | No application/evidence in scope | Not a pass; continue legacy writer |
| LEDGER_UNAVAILABLE | Missing table, SQL permissions/connection failure | Fail closed |
| INTEGRITY_DEGRADED | Orphan evidence, missing evidence, diverging cumulative watermark, unsupported priced holdings evidence | Hold activation, investigate |
| PRICE_PENDING | Confirmed quantity with unresolved price/PnL | Never invent price; retain uncertainty |
| PARTIAL_IN_FLIGHT | Price known, but order's requested quantity is not yet fully executed | Pending action cannot authorize cutover |
| LEDGER_ONLY_OK | New settlement evidence and application agree | **Not sufficient** to prove KIS ↔ existing legacy DB parity |

Do not claim all DB mismatches are solved on the strength of isolated ledger tests. Each PR needs production function and PostgreSQL-backed evidence before MERGE READY.

## Rollout sequence

1. Merge PR164 after its KR live-incident regression and required CI pass.
2. Rebase PR165 to newly merged `dual-agent`; validate migration and PostgreSQL concurrency, merge.
3. Rebase PR166 to new `dual-agent`; validate both market production call-chain probes and shadow safety, merge with flag OFF.
4. Rebase this Phase 4 PR, run KR+US release tests and complete prerequisites. Merge when reviewer-approved, keeping writer OFF.
5. **Follow-up required if proofs not satisfied**: implement exact single-writer economic cutover and certified historical bootstrap in a separately reviewed PR. Do not activate on this PR alone.

No change to PB1, KR Infinite, US Standard or TQQQ Infinite policy thresholds or allocation rules.

## Read-only audit command (for an isolated / approved verification DB only)

```bash
NULLIM_SETTLEMENT_AUDIT_DSN="postgresql+psycopg://..." \\
  python -m trader.settlement.audit --market KR --env practice \\
  --epoch YOUR_TRADING_EPOCH_ID --account-scope HASHED_ACCOUNT_SCOPE
```

The DSN must come from a restricted environment variable, not a command-line password argument. Repeat independently for US. Exit code 0 means ledger-only internal consistency; NOT live broker parity, nor writer authorization. The audit never writes, sends orders, or repairs historical rows.
