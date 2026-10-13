# 2026-10-13 — US practice phantom-open TP incidents (MSFT / AMD / MRVL)

Premerge investigation. **No broker cancellations or DB order/claim mutations in this change.**

## Durable evidence, separate from present broker status

| US trade date | Symbol | KIS ACK ODNO | TP stage | Historical KIS CCNL status | Claim |
| --- | --- | --- | --- | --- | --- |
| 2026-10-06 | AMD | 0000035281 | TP1, qty 1 | OPEN/zero fills | IN_FLIGHT |
| 2026-10-06 | MRVL | 0000034237 | TP2, qty 2 | OPEN/zero fills | IN_FLIGHT |
| 2026-10-12 | MSFT | 0000044796 | TP1, qty 1 | OPEN/zero fills | IN_FLIGHT |

The app received KIS success acknowledgements **and** historical order snapshots.
An MTS cancellation reporting that the original order does not exist is conflicting
evidence, **not** by itself proof of broker terminal cancellation or zero fill.

### Verified chronology bug

Overseas inquiry returns `ord_dt` = US session date,
`dmst_ord_dt` = domestic/Korea calendar order date,
`ord_tmd` = KST wall clock.

- MSFT 20261012 / 20261013 / 002821 = **2026-10-12T15:28:21Z**.
  Prior data-provider parser incorrectly emitted 2026-10-11T15:28:21Z
  and `us_trade_date=2026-10-11` (Sunday).
- AMD 20261006 / 20261007 / 001427 = **2026-10-06T15:14:27Z**.
  Prior parser incorrectly emitted 2026-10-05T15:14:27Z.
- MRVL 20261006 / 20261006 / 225917 = **2026-10-06T13:59:17Z**,
  demonstrating that not all orders need the +1 KST-day rollover.

The new helper requires a consistent US-session/KST-clock reconstruction;
when KIS `dmst_ord_dt` is provided it must corroborate that date, otherwise
the row is quarantined rather than used as chronological evidence.

### Cross-day stale-open handling

`us_protective_tp_recovery` retains the existing claim fence but **does not
attempt a live cancellation based solely on a cross-day historical OPEN row**.
Such rows are not a proof that KIS currently accepts cancel on that ODNO.
Fresh fills/cancels from authoritative broker evidence still reconcile normally.

No automatic `CANCELLED`, `NO_ORDER`, or `SATISFIED` transitions based on
age, empty lookup, MTS error, or a dead tick. Broker truth must be verified
across the original order date, exchange, account, owner, epoch, partial fills,
cancel/modify events and current authoritative position.

## Required WSL/KIS practice verification, read-only first

1. Check Windows Scheduled Task deployed SHA and confirm it matches a merged
   commit before attributing its behavior to this patch.
2. Under practice-only read permissions and order disabled settings, inquire
   order history for each original US trade date and the current date,
   including continuation pages and `ODNO`, `ord_dt`, `dmst_ord_dt`, `ord_tmd`,
   `nccs_qty`, `ft_ccld_qty`, `rvse_cncl_dvsn`, `orgn_odno`.
3. Compare KIS practice MTS account and exchange against the program's KIS
   practice account; do not print account IDs, API keys or tokens.
4. Collect authoritative current practice holdings for MSFT/AMD/MRVL. A
   current holding by itself cannot establish the exact prior order's terminal
   status; keep an unresolved claim fenced without unambiguous evidence.
5. Only after broker terminal evidence and deduplicated fill events agree,
   reconcile prior execution claims. Do not delete them manually.
6. Measure baseline and changed `order_reconcile_ms`,
   `exit_engine_ms`, `sell_signal_to_broker_boundary_ms`, quote REST
   calls and overall tick p95. Do not assume this fix alone meets the 60s target.

### Classification

- **IMPLEMENTATION BUG:** historical KIS US order timestamp incorrectly
  combined as the same KST day after midnight; now corrected and tested.
- **EXPECTED safety behavior:** ambiguous/OPEN broker receipt preserves
  execution fence to prevent duplicate sells.
- **POLICY REVIEW / operational gap:** historical OPEN cannot indefinitely
  substitute for the current, cancelable KIS order state. Need explicit broker
  terminal truth, not repeated stale CCNL scans or speculative claim deletion.

This incident may amplify REST reconciliation delays and risk-gate conflicts;
it is not the demonstrated sole cause of five-minute trading ticks.
