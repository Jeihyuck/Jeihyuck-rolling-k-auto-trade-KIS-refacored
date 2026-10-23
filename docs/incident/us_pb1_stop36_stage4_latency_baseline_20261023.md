# US PB1 stop 3/6 and execution latency incident baseline (2026-10-23)

## Scope / audit boundary

- Code baseline: `dual-agent` `490fd0969cd948ab028f1f1b9a6a4b9e264dd24e`.
- Source: `nullim-dual-agent-us-logs-20261023-070001.tar.gz`, containing **2026-10-22 US** AM and Afternoon WSL log files. This log archive was inspected offline; no KIS orders were sent.
- Additional DB source: Supabase `us_order_events`, `us_execution_claims`, `us_orders`, `us_positions` as of 2026-10-22.
- All durations use log-emitted monotonic stage milliseconds. p95 here is nearest-rank among parsed samples; these are **not** actual native KIS HTTP signal-to-submit spans.

## Before-change latency

| Log metric | AM (38 ticks) | Afternoon (36 ticks) |
|---|---:|---:|
| Whole tick p50 (ms) | 143608.4 | 142246.7 |
| Whole tick p95 (ms) | 218239.6 | 180753.9 |
| Whole tick max (ms) | 222098.9 | 204710.9 |
| Historical claim replay p50 (ms) | 14394.2 | 13414.2 |
| Historical claim replay p95 (ms) | 17914.1 | 17360.6 |
| Historical claim replay max (ms) | 21547.3 | 17654.1 |
| Pre-route delay after SELL ready p50 (ms) | 17020.6 (13 signals) | 16819.5 (12 signals) |
| Pre-route delay after SELL ready p95 (ms) | 41844.5 | 39937.1 |
| Pre-route delay after SELL ready max (ms) | 41844.5 | 39937.1 |

These readings establish that `replay_order_journal()` bounded-stage configuration does **not** impose a ten-second elapsed upper bound and that the protected SELL critical path still incurs unrelated work.

The logs explicitly show `US_STAGE123_PAPER_PROFILE=ON`, `US_RECONCILE_WORKER_MODE=RESERVATION_ONLY`, and `US_ACCOUNT_RESERVATION_MODE=ENFORCE`. They **do not** establish Stage 4 authoritative writer cutover.

## Historical order evidence / regression targets

- AMD (4 shares, -21.10%) and MRVL (9 shares, -19.85%) remain below frozen -8% hard stops in 2026-10-22 positions.
- Their historical 2026-10-06 TP claims remain `IN_FLIGHT` with order status `OPEN`; cancellation requests were recorded 2026-10-09. Four other symbols (MSFT, CL, KO, KR) also have historical TP `IN_FLIGHT` claims. Do **not** equate stale local `OPEN` with a currently active KIS order.
- 2026-10-19: META and TMO each had 43 `BROKER_SUBMIT_ABORTED_PRE_IO` and 43 `JOURNAL_REPLAY_UNRESOLVED` events. This is a prior incident addressed in PR #243; guard against regression rather than presuming it still occurs.
- 2026-10-20: NET `BROKER_SUBMIT_RESULT_UNKNOWN` followed by `BROKER_ACK_RECOVERED`. The recovery must not create a duplicate sell.
- TQQQ Infinite remains independently owned. No PB1 3/6 override may apply to its lifecycle.

## Required architectural follow-ups (not implemented by baseline-only work)

1. Durable stop obligation persisted **before** pending-SELL suppression, tied to account/epoch/owner/lifecycle/contract.
2. Broker-evidenced terminal resolution of historical TP claim plus protected re-entry of the pending SELL without double-selling.
3. Account-global fenced authoritative reconciliation writer with single-state-transition ownership and crash recovery.
4. Split SELL and BUY prerequisites; a clean protected SELL must not await unrelated older-claim scans.
5. Repeatable before/after monotonic trace to **native KIS HTTP**: p95 <=10s (N>=20 distinct SELLs), whole tick p95 <=60s (N>=20 comparable ticks). Count blocked/no-HTTP outcomes separately.

This baseline file is not a PASS claim for any architecture/latency milestone.


## Broker HTTP boundary sample audit (AM/Afternoon 2026-10-22)

The archive also contains the **actual native HTTP boundary**, as logged
by `order_router._mark_broker_submit_boundary()` before entering the KIS
order API. Only the two ADM observations have an exact selected-signal ID:

| Symbol | Selected signal to native HTTP (ms) | Tick start to native HTTP (ms) |
|---|---:|---:|
| ADM | 43,950.93 | 176,790.94 |
| ADM | 41,690.31 | 174,398.77 |

Additional `TICK_TO_BROKER_BOUNDARY` observations without a selected
signal identity: AMGN **144,702.37 ms** and GILD **144,514.28 ms**.
Do **not** treat those as complete signal-to-HTTP samples.

There are **two** selected-intent signal-to-HTTP events in this archive,
for a single symbol, and **four** tick-to-HTTP events. This does not meet
the required **20 distinct protective SELL orders** and therefore does
not establish a valid p95 protective SELL pass/fail for the new code.
Both two exact observations exceed the ten-second **target** and show
the baseline bottleneck remains material.

None of these measurements were collected after the PR #250 fixes.
