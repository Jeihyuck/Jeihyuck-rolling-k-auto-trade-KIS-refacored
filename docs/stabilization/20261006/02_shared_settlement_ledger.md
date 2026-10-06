# Phase 2 — KR/US common settlement ledger (additive, dormant)

Base PR164, do **not** independently merge before PR164 lands. This PR adds migration 0055 and an import-safe SQLAlchemy settlement primitive; it does not route KIS orders or change production position projections.

## Contract

- Preserve PB1, KR_INFINITE, US_STANDARD and TQQQ_INFINITE policy owners and position cycles.
- Order intent and submit attempt fencing remain governed by PR155 `execution_claims`; this PR does not replace them.
- The **local client order key** is the stable economic settlement anchor; KIS order numbers are recorded as evidence, never assumed globally unique. Original trade date + exchange + market are immutable scope dimensions.
- Evidence types: actual execution, order cumulative actual, exclusive/unambiguous holdings delta. A balance snapshot without exclusive order attribution is NOT a fill.
- A quantity-confirmed/price-unresolved SELL is legal: quantity applies once, execution-price PnL remains pending.
- `settle_atomic` applies an economic projection callback on the **same DB Connection** as evidence and applied watermark. It must never wrap legacy repository methods that open their own transaction.
- No fabricated price from a BUY limit or SELL order price. No float arithmetic in monetary decision logic.
- Repeated snapshots cause zero quantity delta; a later actual fill price can settle the pending notional, but conflicting already-confirmed prices are quarantined.

## Gate before Phase 3

1. Run migration 0055 on an isolated PostgreSQL test DB; verify uniqueness, rollback and row locks under contention.
2. Unit and SQLite transaction regressions in `tests/test_shared_settlement_ledger.py`.
3. Existing KR/US order routing and fills remain untouched. No production backfill/DB reset.

## Limitations to carry explicitly to Phase 3/4

- The generic callback deliberately does not mutate KR or US positions. Production adapters must validate the exact existing economic lifecycle and **never** enable two concurrent writers.
- Account scope must be a stable non-reversible account identifier, not an account number.
- Historical watermark migration requires unique order+cycle+epoch proof; ambiguous legacy records remain blocked.
- SQLite tests do not prove production PostgreSQL concurrency or migration quality.
