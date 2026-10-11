# KR/US four-family BUY → SELL pre-merge evidence — 2026-10-11

## Scope and provenance

- **Code:** `dual-agent` HEAD `e08b69ad1fb5cbb0cec67e43432701a5178c609b` was the operating baseline before #204/#205/#206. #206 is pre-merge, **not** the running order engine.
- **Read-only Supabase practice records:** 2026-08-01 through 2026-10-09. Tables: `pb1_watchlist`, `us_watchlist`, `orders`, `fills`, `positions`, `us_orders`, `us_fills`, `us_positions`, `us_profit_capture_lifecycle`, and `price_daily`.
- Exclude mock/manual/TQQQ owner records from the `US_STANDARD` 4-family BUY verdict. Report only genuinely observed trade-session dates, never turn missing quotes into a positive strategy signal.
- Old orders do **not** prove the pre-merge #204/#205/#206 engine was active. Archived OHLCV replay and signal-only tests are not brokerage fills.

## Baseline cohort (distinct Final30 stock/day)

| Market | Days having Final30 **and** daily market evidence | Pullback | Momentum | Breakout | VCP |
| --- | ---: | ---: | ---: | ---: | ---: |
| KR | 39 | 1,060 | 83 | 27 | 0 |
| US | 47 | 1,300 | 85 | 0 | 0 |

US also had 14 `momentum_pullback` entries, which are **not** standalone Momentum/BREAKOUT. KR had four other Final30 dates without matching `price_daily` market evidence and they are excluded from the confirmed-session cohort rather than presumed real trading days.

No KR standalone Momentum or Breakout BUY fill was observed. Five same-date Final30 Momentum/Breakout candidate-to-order comparisons showed `ENTRY_PULLBACK` for an EXPIRED/ERROR BUY attempt; direct causal trace ID was missing. This is a concrete provenance regression to guard against, not proof that each attempt should have filled.

## Historical BUY-to-SELL example ledger (original frozen buy windows)

| US symbol | Original Final30 type | BUY session | BUY qty | Verified SELL qty before next BUY | Exit evidence |
| --- | --- | --- | ---: | ---: | --- |
| MSFT | momentum (legacy; no BUY style metadata) | 2026-08-06 | 6 | 6 | TP1 and other exit fills; older PnL missing |
| PLTR | momentum (legacy; no BUY style metadata) | 2026-08-06 | 21 | 21 | TP1/TP2/TP3 and other exit fills; older PnL missing |
| MRVL | momentum_pullback (not standalone) | 2026-08-28 | 14 | 14 | Defense/soft-stop exit fills; older PnL missing |
| SNOW | momentum_pullback (not standalone) | 2026-09-10 | 9 | 9 | Defense/soft-stop exit fills; older PnL missing |
| AMD | **ENTRY_MOMENTUM**, US_STANDARD | 2026-09-23 | 4 | 4 | TP1 1, trailing 3, PnL −$9.603 |
| INTC | **ENTRY_MOMENTUM**, US_STANDARD | 2026-09-23 | 29 | 29 | TP1 7, TP2 5, giveback 17, PnL +$138.266 |

AMD/INTC BUY row provenance and signed `entry_exit_contract` are populated; `us_profit_capture_lifecycle` confirms AMD TP1 (1/1), INTC TP1 (7/7) and TP2 (5/5) `DONE`. TP3 was not observed for the September 23 buys; do not invent TP3 fills. Later October 2 AMD/INTC Pullback BUYs are different entries and excluded from the September exit allocation.

Older US 4 example exits had null `realized_pnl_usd`; `NULL` must not be reported as real $0 profit.

## Independent acceptance invariants

1. Candidates with genuine, **as-of-consistent completed-bar** family evidence compete via proof-relative quality, not a unconditional Pullback advantage or arbitrary per-family quota. Log **all** qualified-but-capacity-rejected candidates.
2. Selected Final30 winner, order `entry_reason`, order `entry_style_selected`, position, and **frozen** `EntryExitPlan` must not be relabeled Pullback by an old `entry_reason`.
3. A score, the mere existence of a Final30 row, or missing VCP proof **must never** authorize a BUY. The prior risk gates, cash gates, duplicate fences, market-state policy and broker ACK/fill checks retain precedence.
4. Each accepted `US_STANDARD` style (Pullback/Momentum/Breakout/VCP) signs an immutable owner-specific BUY→SELL contract. TP1/2/3 obey the original contract thresholds/quantities. Neither an ACK nor an unknown fill quantity may mark a stage DONE. An existing position stays fenced until broker truth.
5. TQQQ Infinite is separate and cannot inherit the generic `US_STANDARD` four-family policy. A pending SELL is not considered a confirmed fill.

## Additional KR reconciliation findings (separate from strategy-selection patch)

- October SELL status `FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED` covers four orders, 50 shares. The positions/frozen exit evaluation prove reduction or closure, **not** complete broker price/cost/profit reconciliation.
- 2026-08-04 KR `018260` BUY: `orders.status=FILLED` for five shares while matching `fills` rows show two shares. Do not repair historical practice data by speculative deletes/insertions; fetch broker actual or quarantine safely.
- These are **not** evidence that a Pullback/non-Pullback BUY must occur in a Risk-Off session. They require separate broker-truth reconciliation before asserting fully proven real-trading E2E.

## Pre-merge evidence level

- New code: #204 KR proven-family entry/exit identity and #205 US qualified-family arbitration; #206 combined acceptance and TP settlement regression.
- CI replays archived real daily OHLCV and uses local isolated PostgreSQL/mock broker SIGNAL_ONLY. It never emits a KIS order.
- **Still required for live activation:** sequential dependency merge, final combined HEAD CI, readonly WSL/KIS preflight, real feature flags/environment verification, broker account/order state and Risk Gate observation on an actual future trading session.

**No running branch, broker data, database schema, position, or strategy exit policy was modified by this audit.**
