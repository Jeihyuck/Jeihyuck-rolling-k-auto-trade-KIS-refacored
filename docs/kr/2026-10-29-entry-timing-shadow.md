# KR PB1 entry timing separation — 2026-10-29 Mico incident
Status: **DRAFT / DIAGNOSTIC SHADOW ONLY / DO NOT MERGE FOR LIVE TIMING ENFORCEMENT**

## Why this change matters
- The KR PB1 09:00–09:30 opening BUY block is only a submission clock gate.
- The existing entry condition \`setup_filters_ok OR breakout_trigger_ok\` permits setup-only MOMENTUM_CONTINUATION buys once 09:30 arrives; the clock gate does **not** certify an intraday reversal.
- 2026-10-29 059090 Mico was bought 79 shares at KRW 26,050 around 09:31, with \`trigger_ok=false\`, \`above_vwap=false\`, \`above_open=false\`, \`relative_volume_at_time=null\`.
- Earlier timestamped candidate snapshots showed both a sharp decline (about KRW 26,200 to 25,400) and a rebound before 09:30; selecting an attractive candidate did not imply the 09:30 entry was attractive.
- This is a **POLICY REVIEW**, not proof that the existing \`OR\` code violated its configured policy.

## This PR does now (scope 1)
- Adds a **read-only, opt-in KR standard PB1 timing shadow** behind \`KR_PB1_ENTRY_TIMING_SHADOW_ENABLED=1\` (default **0**, disabled).
- Retains historical candidate price snapshots from 09:00 onwards across scheduled ticks in a day/account/symbol-scoped JSONL file under \`TRADER_RUNTIME_DIR\` (or existing default runtime root).
- Computes observed opening-high/low *from observed quotes*, not from fabricated complete 1m candles.
- Requires coverage of both the early and late opening interval; unknown, incomplete or stale data cannot yield \`READY_SHADOW\`.
- Separately records VWAP and **time-adjusted** RVOL; raw \`volume_expansion_ratio\` and \`VERIFIED\` (data integrity only) never count as confirmed buy volume.
- Reports \`WAIT\`, \`UNKNOWN\`, \`READY_SHADOW\` or \`NOT_APPLICABLE\`, plus explicit reasons, via \`[KR][ENTRY][TIMING_SHADOW]\` and \`cf.features.entry_timing_shadow\`.
- Handles MOMENTUM and BREAKOUT as initial hypothesis-only shadow paths; PULLBACK explicitly returns UNKNOWN pending verified intraday completed candles/support.
- No DB migration; no new broker API requests; no changes to buying windows, sizing, pre-submit safety gates, risk thresholds, strategy ownership, EOD exit or any KIS order.

## This PR does NOT do (must not be described as accomplished)
- It does **not** switch production entry authorization to the new shadow state, or change 09:30 trading.
- It does **not** demonstrate better fills, return, MDD or win rate.
- It does **not** implement completed 1/5-minute bars, validated support/retest or complete quote tape. Sampled snapshots are *not* those bars.
- It does **not** repair 2026-10-29 P0 reconciliation, fills.TEXT JSON operator failure, missing owner, position/exit-policy binding or economic settlement. Those must be fixed in an **independent P0 PR**, not rolled into this policy experiment.
- It does **not** apply KR PB1 timing to KR_INFINITE, Codex leverage, US or another owner. Their respective strategy code remains authoritative.

## Important known limitations for review
1. Only eligible order candidates reaching the current PB1 pre-submit building path are observed. A dedicated opening collector across *all* Final30, including filtered-out candidates, is still needed before promoting timing to any blocking policy.
2. Price snapshots lack completed 1/5m OHLCV; missing VWAP and historical intraday volume profiles leave the shadow in WAIT/UNKNOWN. No imputation may turn a missing signal into a pass.
3. Runtime JSONL survives process restarts only when its runtime directory is shared and persists. After reboot/cleanup the result must remain UNKNOWN and not be inferred from partial observations. Dedicated persisted DB proof must be considered before enforcement.
4. Intraday observed extrema are sampled and can miss true highs/lows. Never claim they are exchange-reported opening range.
5. All READY_SHADOW thresholds (e.g. RVOL >=1.2, consecutive observations) are **untested hypotheses**, not approved production settings. No entry-enforcement flag is provided by this PR.
6. File read/write costs, concurrent writers, exception handling and missing file access need realistic WSL task acceptance tests. Shadow failure does not change order safety or block protective SELL.
7. Risk-off/defense status must only be handled according to the actual owner strategy. No global veto inferred from a regime label.

## Required continuation: P1 policy PR (separate from this shadow)
1. Map existing KR owners / strategy sleeves (PB1 PULLBACK, MOMENTUM, BREAKOUT, Minervini/VCP as signal features, and any separate KR owner) to their authoritative entry and exit configurations.
2. Separate the four contracts:
   - eligibility/selection (Final30, scores, daily setup)
   - intraday timing permission (owner/style-specific)
   - broker execution (price, spread, liquidity, account/risk, >180s tick deadline)
   - exit and settlement (original frozen DAY/SWING/CORE policy).
3. Build reliable 09:00–09:30 observations from a bounded price/volume stream with per-field provenance and freshness. Do not fetch 30 symbols' long OHLCV on the hot pre-submit path.
4. For MOMENTUM, test pullback/reclaim **and** validated breakout paths; buying late after a prior rebound without new confirmation is disallowed by the *new candidate policy only*, never by mutating old history.
5. For PULLBACK, test support area/low stability, completed-bar higher low and rebound confirmation; do **not** force a momentum breakout or VWAP-above gate on all PB1 pullbacks.
6. For BREAKOUT, use real pivot and expansion proof. Do not reuse momentum's OR bypass to call failed breakout a confirmed breakout.
7. States: WATCH / WAIT / READY / INVALID / UNKNOWN. UNKNOWN is *not* READY; explicate expiry and no stale carry-over. NEW BUY should be the only future gating target: existing protective SELL remains independent.
8. Order pre-submit must recheck signal age/current price without introducing network latency; all existing idempotency, budget, KIS order-risk, partial-fill/reconciliation, and forced close policies are preserved.
9. Do not mutate a position's frozen exit contract based on current candidate's entry style.

## Replay & release acceptance
- Compare current engine vs shadow vs proposed policy **using the same historical point-in-time data**, not future bars; include Mico 2026-10-29 09:00–15:15, trend/no pullback, failed rebound, instant breakout, high-spread/gap, missing KIS quote, missing VWAP, stale quote, partial volume profile, changing regime, trading halt.
- Mico 09:30: under proposed cautious momentum timing, insufficient VWAP + time-adjusted RVOL yields WAIT. It does **not** prove *if or when* an afternoon BUY would occur.
- Report candidate coverage, wait length, foregone winners, false negatives, execution-time P50/P95/P99, broker submission within 180s, actual fee/slippage-adjusted PnL, first 15/30/60m adverse excursion, day MDD, no unintended missing/protective SELLs.
- Verify strategy owner isolation; old stop/TP/sizing unchanged; no accidental 09:30 BUY gate regression for disabled shadow.
- P0 incident must be repaired and independently proven before any policy change.
- Start with shadow-only, then practice-only, then roll out per owner/style after explicit approval and trustworthy backtest. No automatic merge, no scheduler/environment modification, no live order generation.
