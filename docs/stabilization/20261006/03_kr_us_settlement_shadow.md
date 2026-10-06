# Phase 3 — KR/US shadow comparison hooks (no second economic writer)

This PR depends on Phase 2 / PR165; base: `stabilize/06-shared-settlement-ledger-20261006`.

## Default

- `NULLIM_SETTLEMENT_SHADOW_ENABLED=0`; zero shadow SQL/API or order behavior.
- With explicit shadow enablement, Korean holdings promotion and US ACK/balance reconciliation report normalized settlement candidates; **read-only** `preview_decision` only.
- A missing original order owner, cycle, account scope, original trade date, epoch or exclusive holdings proof is `REVIEW_REQUIRED`, not a fabricated settled fill.
- US order cumulative evidence and synthetic balance evidence are distinguished. No synthetic fill price from a limit/order price.
- Reports are never a submit/fill permission and never write `settlement_applications`, `fills`, `positions` or `execution_claims`.

## Validation / MERGE HOLD

1. Verify production KR/US reconciliation calls both shadow probes with flag disabled (no new SQL).
2. Enable only in isolated test/pre-production replay; verify zero row mutations and owner mapping.
3. Expand production-path probes to US individual execution import and KR_INFINITE dedicated sleeve in Phase 4. Owner-specific entry strategy remains unchanged.
4. Compare broker cumulative qty with existing legacy fill accounting and new expected delta; an unseeded settlement application is `NOT_MIGRATED` not a mismatch.
5. Do not enable the common writer until legacy applied balances have provable per-cycle seed and the old writer is disabled for the same economic order.
