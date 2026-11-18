# US_STANDARD legacy 9: current practice account + active trading epoch only

## Purpose

For ABBV, DHR, HCA, KO, KR, MCK, PFE, PG, TMO, keep original V2 BUY
contracts immutable while enabling **strategy-generated** protective exits
using already-durable Supabase BUY/FILL/lifecycle evidence.

**Do not use a retired KIS account, a prior `trading_epoch_id`, another
portfolio/sleeve, `NULL`-epoch order, or an older symbol-level snapshot as a
fallback.** A previous calendar date is not automatically a previous epoch:
the original BUY may have occurred on an older date **inside the active
trading epoch**. It is valid only in that exact scope.

## Sources of truth

- Runtime scope: `KIS_ENV=practice`, `WSL_RUN_MARKET=US`, authenticated
  *current practice account*, active `_active_us_epoch(..., required=True)`,
  and one `portfolio_epochs` ACTIVE row for that account/epoch/US_STANDARD.
- Signed historical facts (same **active** trading epoch only): `us_positions`,
  `us_orders`, `us_fills`, `us_position_risk_state`,
  `us_profit_capture_lifecycle`. Verify frozen V2 policy and digest,
  owner, original BUY fill/order number, lifecycle account and epoch,
  net filled quantity, and prior TP state. A populated account/epoch
  identity on any selected row must match the runtime current scope.
- No retroactive UPDATE of original BUY/FILL/POSITION portfolio epoch/cycle.
  Assign only a distinct **prospective** management cycle to the new SELL
  after all proof and safety checks succeed.
- No retrospective KIS per-order/day GET in an on-demand exit flow.
  An authenticated KIS client still must use the **current practice account**
  for fresh holdings/orderable balance and **current-day** competing orders;
  repeat immediately before the native SELL HTTP request. KIS has a broker
  account, but **no PostgreSQL trading-epoch field**; the epoch boundary is
  enforced by the application DB/claim/intent gate.
- Unresolved same-active-epoch historical SELL, unknown ACK/cancel,
  unverified zero-fill, wrong owner/account/epoch, absent DB originals,
  quote staleness, risk gate or competing KIS order => **local fail-closed**.
  AM/CLOSE reconciliation may establish a terminal historical status. Do
  not call the historical KIS GET in each trading tick; do not treat empty
  broker results as proof of cancellation.
- `TQQQ_INFINITE`, AMGN's unrelated signed-contract issue, new-position
  BUY rules, and original US_STANDARD exit thresholds remain unchanged.

## Expected present-day incident behavior

From 2026-11-17 Supabase read-only data:
- ABBV/DHR/HCA/KO/KR/MCK/PFE/PG/TMO each have exactly one BUY
  `us_orders` and one BUY `us_fills` in the holding's active trading
  epoch, and their net filled quantities equal `us_positions.qty`.
- KO's three historical OPEN/ACK rows and KR's one OPEN row have **other
  trading epochs**. They must not enter this legacy position's
  `HISTORICAL_PENDING_SQL`.
- TMO has one 2026-10-30 OPEN SELL with **the current epoch**. Until
  reconciled to broker-proven terminal status, TMO remains fenced; do
  not synthesize a cancellation, fill, or position repair.
- A KIS current-day competing order or holding mismatch still blocks
  any of the nine symbols even when the DB original is valid.

## Acceptance

1. Unit fixtures: all nine symbols; TP and hard-stop; old KIS original
   history missing (irrelevant); wrong account/epoch; old-epoch pending
   ignored; same-epoch pending fenced; zero-fill source certification;
   changed KIS current holding/orderable/competing order; pre-HTTP recheck.
2. Real PostgreSQL read-only checks: active account + active
   `trading_epoch_id` + owner + contract hash; BUY/SELL cumulative net
   and historical pending counts. Never print secrets.
3. WSL KIS practice: proof statuses and reasons for nine symbols;
   current KIS holdings and same-day order completeness; source SHA
   equals the deployed, pinned `dual-agent` SHA.
4. An actual SELL is only complete after a genuine strategy signal,
   broker ACK/fill and CLOSE reconciliation. No forced SELL.
5. Do not merge/deploy until required CI and active-epoch runtime
   validation; keep a verified rollback. No code hot swap mid-session.

## PR relationship

This changes only the legacy US_STANDARD SELL evidence policy and scope.
The independent PREWARM removal PR #364 changes scheduling; neither PR
on its own restores a historical `NULL` field in the DB.
