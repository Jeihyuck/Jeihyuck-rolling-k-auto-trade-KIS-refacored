# Stabilization 5/5 — runtime liveness, production-path replay, owner wiring, and close integrity

Base branch: `stabilize/04-lifecycle-contract-recovery-20261004`.

## Purpose
Complete the stabilization stack by proving the corrected execution/lifecycle contracts survive the real KR/US runner paths, repeated delays, owner-specific routing, PREP/session transitions, and close reconciliation.

Do not solve measured performance problems only by raising timeouts. Measure where time is spent and preserve protective SELL/reconciliation priority.

## Runtime deadline/budget requirements
1. Each tick has one authoritative deadline/budget propagated through expensive KIS/DB/retry/wait operations.
2. Individual external calls/retries must respect remaining budget; a nested call may not outlive the tick worker and submit late afterward.
3. Old/timed-out workers must lose submit authority before a new generation/session can place an order.
4. Reconciliation of unresolved orders and protective SELL evaluation must be scheduled before optional candidate/scoring work when budget is tight.
5. Remove repeated redundant balance/fill/price fetches only when freshness/order-post invalidation guarantees are preserved.
6. Detect candidate evaluation that continually restarts from the front and starves later candidates. If continuation/cursoring is introduced, revalidate price/strategy/risk immediately before submit.
7. Repeated BUY/SELL evaluation deferral under otherwise-valid policy must emit a liveness failure distinct from intentional strategy entry-block states.
8. Review KR149 and US154 timeout changes against measured end-to-end work. Change timeout values only with measured justification and session-overlap analysis.

## US TQQQ Infinite production wiring regression
Reproduce the 2026-10-01 condition through the real production call chain:

`run_trade_session -> run_trade_tick -> TQQQ overlay -> run_sleeve -> route_order`

Scenario:
- PB1/shared PREP has `entry_can_proceed=false` because `trade_block_reason=sector_cap_violation_block`.
- TQQQ Infinite is already held, operational context is healthy, quote is fresh, owner is `TQQQ_INFINITE`, structural policy permits evaluation, and Fast Dip condition is satisfied.

Required:
- PB1 BUY remains blocked.
- TQQQ owner-specific PB1-only policy override receives the authoritative block reason/context and may route the Fast Dip BUY.
- An operational block such as stale quote, reconcile-entry block, missing PREP after failed recovery, or hard system failure must still block TQQQ.

Do not globally promote shared `entry_can_proceed` just to unblock TQQQ.

## Production-path replay suite
Build durable replay fixtures from actual incidents. Logs alone are insufficient: each fixture must record/declare code SHA, relevant settings, PREP result/run ID, initial positions/balance, broker responses, clock/timezone inputs, price snapshots, and any missing inputs synthesized as explicit fixtures.

At minimum preserve these US incidents as continuously executed regressions:
- 2026-09-29: PREP recovery / WebSocket ownership / TQQQ owner runtime gate.
- 2026-09-30: PB1 entry-starvation / PR154 tick budget / cross-day min-hold.
- 2026-10-01: NVDA ambiguous-ACK duplicate SELL / JNJ unresolved-intent broker rebound / TQQQ Fast Dip production wiring / PnL completeness.

Add corresponding KR historical incidents for BUY-contract propagation, SELL liveness, unresolved ACK/partial-cancel recovery, and runtime-integrity paths using available evidence. If an input is unavailable, mark the fixture's reproduction scope explicitly rather than inventing broker truth.

A replay counts as production-path validation only when it executes actual runner/router/repository/reconciliation components; a self-contained policy model is not enough.

## Required fault injection
Across KR/US as applicable:
1. Broker accepts order, client loses response.
2. ACK succeeds, DB persistence fails.
3. Partial fill, cancel ACK, later cumulative fill observation.
4. DB latency and KIS latency spanning multiple ticks then recovering.
5. DNS/REST failure before submit vs after possible submit.
6. WebSocket disconnect/reconnect with stale quote protection.
7. Process restart after unresolved submit.
8. Session-generation overlap / old worker tries to submit late.
9. PREP recovery and session transition with artifact/run-ID validation.
10. Incomplete pagination/stale balance must not delete held positions or approve unsafe BUY.
11. PB1 and Infinite owners held simultaneously.

### Current automated evidence inventory

The following are continuously executed component/production-path regressions;
they are not a claim that all original incident inputs are available or that
the full release gate has passed.

| Fault | Automated evidence | Scope |
|---|---|---|
| Accepted submit with response lost, restart/reconcile | `tests/us/test_us_broker_recovery_incidents.py::test_nvda_ambiguous_ack_recovers_original_claim_across_restart_and_date` | Pass |
| ACK followed by DB persistence failure | `tests/us/test_us_order_ack_db_failure.py::test_ack_db_failure_returns_ack_db_failed` | Pass |
| Partial fill, cancel, later cumulative fill | `tests/us/test_us_broker_recovery_incidents.py::test_cancel_then_late_fill_and_repeated_snapshot_are_monotonic` | Pass |
| Close report rejects broker-truth and recovery-health failures | `tests/us/test_us_daily_report_consistency_status.py::test_close_daily_report_never_returns_ok_for_broker_recovery_failures`; `tests/us/test_us_close_broker_recovery_health.py::test_close_fails_closed_for_broker_recovery_integrity_health` | Production report classifies unresolved/unattributed/rebound failures as `RECONCILE_REQUIRED`; duplicate/mismatch/missing-cost-basis findings as `INTEGRITY_DEGRADED`; all fail closed |
| Close report rejects stale filled exit stages and closed lifecycle with held quantity | `tests/us/test_us_close_broker_recovery_health.py::test_recovery_health_detects_filled_pending_exit_stages_and_closed_lifecycle`; `tests/us/test_us_daily_report_consistency_status.py::test_close_daily_report_never_returns_ok_for_broker_recovery_failures` | Production recovery health compares pending TP/trend stages and lifecycle state with authoritative SELL fills/held positions; report fails with `INTEGRITY_DEGRADED` |
| Concurrent same-action workers and expired old worker | `tests/us/test_us_route_order_claim_boundary.py::test_two_concurrent_route_workers_only_one_submit_same_semantic_action`; `tests/us/test_us_execution_integrity_incidents.py::test_expired_tick_context_loses_submit_authority` | Pass |
| Pre-boundary abort/retry and post-boundary uncertainty fence | `tests/us/test_us_route_order_claim_boundary.py::test_tick_expiry_after_submit_journal_releases_claim_for_fresh_retry`; `tests/us/test_us_route_order_claim_boundary.py::test_possible_post_boundary_dns_failure_remains_fenced` | Pass |
| KIS/DB latency over multiple ticks, safe degradation, and recovery | `tests/us/test_us_exit_first_routing.py::test_sep30_session_services_valid_entry_after_repeated_production_ticks` injects fake KIS fill-fetch and DB position-snapshot latency that consumes the first two shared tick deadlines, asserts `NEXT_TICK_ENTRY_DEFER_INSUFFICIENT_BUDGET`, then verifies valid entry evaluation recovers on a fresh third tick. The offline test explicitly scales the production 20-second minimum execution-tail reserve to fit its synthetic 1-second deadline; production settings are unchanged | Synthesized latency/deadline; not live KIS/DB timing |
| DNS/REST failure before and after submit boundary | `tests/us/test_us_route_order_claim_boundary.py::test_kis_order_post_throttle_failure_is_identified_as_pre_http`; `tests/us/test_us_route_order_claim_boundary.py::test_possible_post_boundary_dns_failure_remains_fenced` | Pass |
| WebSocket ownership, reconnect, and stale quote recovery | `tests/us/test_us_prep_websocket_ownership.py::test_websocket_adapter_reconnects_and_only_serves_fresh_quotes`; `tests/us/test_us_tqqq_infinite_integration.py::test_invalid_or_stale_quote_never_reaches_router` | Reconnect and KIS-format quote parsing are simulated; stale quote blocks and fresh quote routes through the Infinite sleeve |
| PREP recovery/run-ID and owner gate | `tests/us/test_us_sep29_tqqq_owner_runtime_gate.py::test_sep29_executable_session_tick_prep_and_tqqq_replay`; `tests/us/test_us_prep_effective_contract.py::test_locked_watchlist_run_binding_rejects_mixed_or_missing_run_ids` | Synthesized `run_trade_session -> run_trade_tick -> TQQQ sleeve -> router`; real session-running lock rejects an overlapping worker; original broker/AppKey telemetry unavailable |
| Cross-day min-hold through SELL route and repeated session liveness | `tests/us/test_us_exit_first_routing.py::test_sep30_crossday_soft_sell_routes_through_tick_before_optional_entry`; `tests/us/test_us_exit_first_routing.py::test_sep30_session_services_valid_entry_after_repeated_production_ticks`; `tests/us/test_us_20260930_pb1_liveness_crossday_minhold.py::test_sep30_legacy_tick_budget_upgrade_preserves_execution_tail` | Synthesized multi-tick KIS/DB delays and quote/price inputs; actual PB1 SELL route uses authoritative opened-at fixture |
| Incomplete balance/pagination | `tests/us/test_us_execution_integrity_incidents.py::test_partial_exchange_balance_never_overwrites_last_good_authoritative_snapshot`; `tests/us/test_trading_epoch_broker_pagination.py::test_balance_pagination_cannot_silently_hit_page_limit` | Pass |
| PB1 policy block with held TQQQ Infinite and PB1 positions | `tests/us/test_us_sep29_tqqq_owner_runtime_gate.py::test_oct1_simultaneous_pb1_and_infinite_positions_keep_owner_gates_isolated` | Both owners appear in one synthesized authoritative balance snapshot; PB1 entry remains blocked while only the Infinite route is allowed |
| 10/1 incident phase orchestration and close/PnL boundary | `tests/us/test_us_broker_recovery_incidents.py::test_oct1_executable_replay_orchestrates_nvda_jnj_tqqq_and_close` | Recovered fills are combined by the production PnL aggregator and passed through close-runner persistence/report inputs; phase stores remain isolated, so this is not one shared account/database snapshot |

The 9/29 replay uses synthesized PREP, balance, and market inputs; it checks the
real session lock against an overlapping worker and exercises the adapter's
WebSocket reconnect/stale/fresh quote path, but it does not claim original
broker/AppKey telemetry. The 9/30 replay runs three production session/tick
iterations: fake KIS/DB delays exhaust the first two shared deadlines and safely
defer entry, then a fresh third tick evaluates; it separately routes a cross-day
PB1 SELL using fixture `opened_at`. It is not live timing evidence. The 10/1
orchestrator composes NVDA, JNJ, TQQQ, and close/PnL assertions and supplies the
combined recovered fills at the close-runner boundary, but its phases still use
isolated fake stores and do not represent one shared broker/account snapshot.
These replays do not replace the separately listed close-integrity/fault tests
or the required post-deployment KR/US observation.

## Data/source normalization
Verify explicit adapters for KIS/DB position/balance/price evidence preserve:
- original observed/fetched timestamp
- source and endpoint
- completeness/pagination/authority flags
- exchange/market normalization
- code SHA, config version, epoch, PREP run ID in logs/reports

Reinserting cached data must not refresh its source timestamp.

## WebSocket/AppKey/process ownership
Verify PREP and trading processes do not compete for shared WebSocket/AppKey ownership. Preserve the PR150/151 fixes and add process-overlap regression coverage. Do not raise subscription limits as a substitute for ownership correctness.

## Close/integrity release gate
A session must not report `OK` if any of these remain unresolved beyond the existing defined reconciliation window:
- unresolved/uncertain broker submit without terminal truth
- broker execution not attributed to an internal action when a unique recovery should have happened
- multiple internal candidates for one broker fill without explicit degraded state
- duplicate semantic broker execution
- broker cumulative-fill vs local fill mismatch
- broker position delta inconsistent with accounted fills/known transfers
- TP/trend stage pending while authoritative broker fill proves completion
- realized-PnL completeness missing an attributable SELL fill
- closed lifecycle still marked open/pending
- repeated BUY/SELL liveness starvation under otherwise-valid policy

Do not compare fill counts only. Reconcile broker execution/order identity, cumulative quantity, price and fees/provenance where available.

Required degraded outputs should distinguish at least:
- `INTEGRITY_DEGRADED`
- `RECONCILE_REQUIRED`
- `LIVENESS_DEGRADED`
- intentional `POLICY_ENTRY_BLOCKED`

Account-wide `RECONCILE_ONLY` should be reserved for account-truth/integrity failures. A single-symbol unresolved action should normally fence/reconcile that action/symbol without unnecessarily stopping unrelated valid protective SELLs, subject to existing account-risk policy.

## Runtime-integrity module cleanup
Inventory date-specific `runtime_integrity_*` modules and installation/patch order. Migrate only duplicated/conflicting responsibilities that now have canonical tested owners. Keep legacy patch entry points until old runtime paths are explicitly covered.

### Current KR runtime hook inventory

The legacy KR PB1 entry point remains `trader.install_legacy_pb1_runtime_guards()`,
called by `trader/kr/runner/trade_session_runner.py`. The function returns without
installing these hooks when the US import guard is active. Installation order in
`trader/__init__.py` is significant because later wrappers must remain outermost:

1. Legacy PB1 engine guard and broker-truth/review/SELL/final-review guards.
2. `runtime_integrity_20260915`, then `runtime_integrity_20260917`.
3. BUY observability, historical retry/safety, cross-date retry, and pending-fill fence.
4. `runtime_integrity_20260928`, then `runtime_integrity_20260929`.
5. `runtime_integrity_20260929_review`, `_log_review`, then `_side_proof`.
6. `runtime_integrity_20260930`, then `_20260930_freshness_review` last.

The corresponding legacy runtime paths remain covered by
`tests/kr/test_kr_runtime_integrity_20260929.py`,
`tests/kr/test_kr_runtime_integrity_20260930.py`,
`tests/kr/test_kr_runtime_integrity_20260930_cli_session_budget.py`,
and their dated review tests. There are no date-named `runtime_integrity_*`
installers under `trader/us`; US runner guards are owned by the US session/tick
runner modules. Do not reorder or remove an installer based on name similarity:
each wrapper has a distinct source-evidence, freshness, owner, or liveness
responsibility, and old entry points remain compatibility surfaces.

## Final acceptance matrix
The stacked stabilization cannot be declared complete until tests prove:
- no duplicate broker submit after post-submit uncertainty
- no duplicate fill accounting
- valid retry after explicit reject/authoritative zero-fill cancel
- correct remaining target after partial cancel
- BUY contract/lifecycle preserved across restart/date/reconcile-skip/KIS recovery
- TP progression uses actual fills
- PB1 and Infinite owner wiring remains isolated
- no persistent SELL starvation under valid SELL conditions
- no tick worker submits after losing generation/deadline authority
- close/report refuses OK on integrity mismatch

## Operational observation after deployment
Do not claim live stability from CI. After eventual merge/deployment, KR and US each require 5 actual open-market trade days of observation. A day with no occurrence of the relevant scenario does not by itself validate that scenario. Fault-injection/replay coverage remains separately required.

## Non-goals
- No live trading from tests/tools.
- No DB reset/delete.
- No evidence-free holding repair.
- No same-trade-day operational session restart.
- No strategy-policy redesign.
- No PR merge.

## Completion report
Provide measured tick timing, replay/fault-injection results, production-path coverage, owner wiring results, close-integrity results, CI status, remaining unverified scenarios, and operational observation requirements.
