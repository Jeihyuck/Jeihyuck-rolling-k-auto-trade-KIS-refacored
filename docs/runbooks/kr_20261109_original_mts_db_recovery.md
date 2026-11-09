# 2026-11-09 KR MTS–DB original-trade recovery runbook

**Scope:** KIS practice only, original account and trading epoch; historical broker
economic evidence. This document does not authorize DB changes or broker orders.

## Evidence and matching

| Symbol | Owner | Original order day | Side / qty | KIS ODNO | MTS qty, user confirmed |
| --- | --- | --- | --- | --- | --- |
| 293490 KakaoGames | PB1 | 2026-10-27 | BUY 174 | 0000011280 | 174 |
| 059090 Mico | PB1 | 2026-10-29 | BUY 79 | 0000011865 | 0 (subsequent SELL) |
| 122630 KODEX leverage | KR_INFINITE_V1 | 2026-10-26 | BUY 11 | 0000011438 | 0 |
| 122630 KODEX leverage | KR_INFINITE_V1 | 2026-10-27 | SELL 5 | 0000008799 | 0 |
| 122630 KODEX leverage | KR_INFINITE_V1 | 2026-10-27 | SELL 6 | 0000017474 | 0 |

MTS current holding only proves the **latest balance**, never a specific
historical execution or price. Internal `entry_contract_sha256` copied into
`_kr_buy_entry_contract` is an internally generated contract echo, not a
broker signature. Do not backfill fills from it.

## Step 1: read-only source checks

Before any remediation, from the original WSL practice account:

```bash
FORCE_HTTP=1 NO_TRADE=1 DRY_RUN=1 KIS_ENV=practice STRATEGY_ENV=practice \
 python -m scripts.kr_20261109_original_kis_proof
```

The script requires all-pages original day KIS orders and exactly scoped
order/strategy owner evidence. It does not modify state. Preserve KIS
response hashes, exact exchange trade date, orders, quantity, executed
notional/price, account identity and proof timestamps in secure logs.

If the original practice account cannot be accessed, or KIS no longer
offers historical detail, return `RECOVERY_UNRESOLVED_ORIGINAL_KIS_PROOF`;
do not manufacture a price/quantity row.

## Step 2: per-owner repair decision

**293490 / PB1**: Original BUY day ODNO 0000011280 must show 174
broker executions and priced notional, or another uniquely proven origin
must explain the account's 174 held shares. The existing imported position
must be shown to be the residual of this exact order, with no later manual
buy/add/sell, and the original frozen `SWING_STAGED_EXIT` contract. The
source order cycle and imported position cycle currently differ. Never
equate matching ticker/quantity to the same lifecycle. Existing
`trader/kr/frozen_contract_recovery_260.py` contains strict recovery
rules and must not be bypassed.

**059090 / PB1**: MTS is flat. The 2026-10-29 BUY79 and
2026-10-30 SELL79 must be proven against KIS originals before linking one
closed cycle. A later distinct BUY91/SELL91 cycle must remain separate.
Ordinary `PositionsRepo.apply_fill(BUY)` replay must **not** recreate
a position now that the original shares have all been sold; any future
economic backfill must be transactionally idempotent, reconcile existing
SELL/P&L and account Epoch, and preserve audit history. If no safe
backfill code path proves this end-to-end, stop and report
`HISTORICAL_CLOSED_CYCLE_REPAIR_NOT_IMPLEMENTED` rather than applying
an unsafe mutation.

**122630 / KR_INFINITE_V1**: The dedicated strategy owns the
11-share cycle; PB1's imported 11 shares are an unproven mirror until
all three KIS orders and latest fresh flat balance are verified. Use
the already implemented strict certification before creating a new repair:

```bash
FORCE_HTTP=1 NO_TRADE=1 DRY_RUN=1 KIS_ENV=practice \
 python -m scripts.kr_infinite_certify_frozen_exit \
 --buy-day 20261026 --sell-day 20261027
```

Run `--apply` only after explicit operator approval of the proof and
code review. `CERTIFIED` must retire the legacy PB1 mirror with **zero
duplicate PB1 P&L** and preserve the dedicated KR_INFINITE cycle.

## Step 3: acceptance and status

Record, per order: `original proof available | complete execution |
price economics | account+epoch | owner | client key | contract hash |
source cycle | position cycle | final account balance | post-repair
double-count risk`. All must agree for VERIFIED.

The correct target MTS parity is: Kakao 174, Mico 0, KODEX 0.
But parity alone is not proof of missing historical fills. Any
unresolved KIS evidence remains a visible historical debt, not a daily
global order block (PR #326 handles trade isolation).

**Do not change production Supabase rows as part of building or testing
these Draft PRs.** No unapproved broker order, manual fill fabrication,
automatic merge, or policy changes.
