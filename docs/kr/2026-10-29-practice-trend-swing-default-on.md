# KR practice trend SWING — opt-in to default-ON correction (2026-10-29)

This change is an **approved practice-account trading policy change**, not a repair of the October 29 fills/close corruption. Review it separately from timing and reconciliation PRs.

## Background
PR #264–#265 (merged 2026-10-27) introduced explicit `ENTRY_MOMENTUM_SWING`/`ENTRY_BREAKOUT_SWING` with proven trend eligibility and immutable SWING exit contracts, but the environment flag default was left at 0. On 2026-10-29 the executed Mico 059090 BUY was 79 × KRW 26,050 and recorded `entry_horizon_selection_proof: KEEP_EXISTING_DAY / opt_in_disabled`. This is EXPECTED per deployed flag but not the user's intended practice rollout.

## Code changes
- When KIS environment is **practice** and `PB1_KR_PRACTICE_TREND_SWING` is absent, it now defaults **ON**.
- An explicit `PB1_KR_PRACTICE_TREND_SWING=0` still forces **OFF**; `real` stays excluded regardless of flag.
- Eligibility still requires verified volume evidence, `price >= MA20 >= MA50`, RS >= 60, and additionally pivot+breakout+volume for BREAKOUT; explicitly DAY-designated BUYs remain DAY. Existing holdings are **not** reclassified.
- Every evaluated new entry logs environment, flag source/effective value and selected style.
- No BUY timing, quote freshness, VWAP, risk budget, order size, protective SELL, KIS order, or scheduler changes.

## Safety and known blockers
- **ON does not fix the intraday price timing policy.** At 09:31 the Mico snapshot could pass this trend eligibility despite unverified 09:00–09:30 entry timing. Do not claim that trend eligible means an ideal entry.
- **ON does not fix the Oct29 PostgreSQL fill TEXT/JSON operator error, owner/cycle metadata recovery, or EOD policy failure.** Those P0 defects must be resolved and replayed before treating practice operations as ready. Disabling a missing-data hard-stop is not a permitted substitute.
- **ON changes future order holding periods.** A qualifying momentum position may carry overnight and face a larger overnight gap loss; P&L, MAE/MFE, time-in-market, and overnight risk require shadow/forward comparisons.
- If the WSL scheduler or its environment files export `PB1_KR_PRACTICE_TREND_SWING=0`, GitHub code default **will not override it**. Identify the actual scheduled-task environment and set the variable to 1 there; do not change any scheduler timings/principals.
- Existing DAY_TRADE holdings, including the 2026-10-29 Mico BUY, must retain their frozen DAY contract. Never backdate or silently relabel the position to SWING.

## Readiness checklist (NO AUTO MERGE)
- Unit tests: missing env -> paper ON; explicit 0 -> OFF; real -> OFF; explicit DAY -> DAY; missing/failing evidence -> DAY; Mico-like proven trend -> SWING frozen plan.
- CI suites and PostgreSQL order/fill replay green. Validate exact program behavior with a WSL **dry-run** that cannot submit native broker orders.
- Resolve P0 fills/close defects before the next live-capable practice trading session and explicitly review any already open DAY positions; no automatic carry conversion.
- Verify boot/log `[KR][ENTRY][TREND_SWING_CONFIG]` and new order `entry_exit_plan` on actual paper runtime. No release claim until the running WSL deployment and environment are independently verified.
