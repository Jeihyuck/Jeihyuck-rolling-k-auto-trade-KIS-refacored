# PR #353 — 2026-11-13 Incident Directive: evidence and implementation status

**Target**: existing PR #353, branch `fix/us-buy-sell-lifecycle-p0-20261112`; base `dual-agent@00f182ae9ff86a5615763f05f7a7e184a2e87277`.

**Safety**: no merge, WSL deployment, KIS order POST, operating DB mutation, historical fill fabrication, policy change, or per-tick historical recovery performed by this directive pass. The uploaded 2026-11-12 ET session archive was inspected and compared to code and read-only Supabase. Runtime manifest SHA = `00f182ae9ff86a5615763f05f7a7e184a2e87277`, **not PR code**.

## Evidence observed

| Event | Archive / persisted evidence | Classification | Remediation |
|---|---|---|---|
| KO TP2 | `[US_PROFIT_CAPTURE][RETRY_FENCED]` 70 observations; stage key exists; last status `US_RECOVERY_SCOPE_UNVERIFIED`, no certified pre-HTTP abort | EVIDENCE INSUFFICIENT for order re-submit | Preserve SELL fence, log exact key/lifecycle/last status and scoped `RECONCILE_REQUIRED`; obtain original broker order evidence |
| KR TP2 | same 70 observations; last status `ORDER_DISABLED_DURABLE_LEDGER_UNAVAILABLE` | EVIDENCE INSUFFICIENT | Same generic stage proof path, no ticker exception |
| HCA TP3 | one RETRY_FENCED; old failed attempts | EVIDENCE INSUFFICIENT | Generic stage-scoped evidence diagnostics |
| AMGN | original BUY6 recorded 11/10; 11/12 risk/profit/exit contract errors; current 11/12 snapshot scoped columns NULL | IMPLEMENTATION BUG, prior PR353 code addresses future provenance | Do not assert recovery without KIS and current account/epoch proof |
| All 10 US_STANDARD holdings | 11/12 snapshot `account_id/portfolio_epoch_id/position_cycle_id` NULL; `ORIGINAL_BUY_SCOPE_UNVERIFIED` | IMPLEMENTATION BUG (persisted provenance) | Existing PR353 lineage/store fixes and AM/Close read-only audit; no DB rewrite without authenticated KIS |
| TQQQ | 2 shares and original fill scope DB, state `READY` | IMPLEMENTATION BUG | Existing PR353 fill-proven READY→ACTIVE CAS. No operating state changed |
| AM startup policy | Normal 09:30–10:00 ET opening BUY restriction; AM entry liveness incorrectly `LIVENESS_DEGRADED`, final AM `entry_degraded=0`; Close `FAILED_RECONCILE` | IMPLEMENTATION BUG (policy/liveness classification) | Code now treats opening-only block as `POLICY_ENTRY_BLOCKED` with guards for session/balance/fills/reconcile/system errors |
| Stage4 | completed AM/afternoon, no writer startup timeout in archived session | EXPECTED | No tick-recovery added |
| New BUY 0 | risk/market gates and minute WAITs recorded; no new KIS BUY | EVIDENCE INSUFFICIENT to label a software bug | Preserve strategies and min-bar gate; new bar-age/opening-coverage diagnostics |
| TMO historic SELL | `OPEN` zero fill in DB | EVIDENCE INSUFFICIENT | Exact source-date authenticated broker GET and page/status verification; no terminalization |
| Historic unresolved count 2 | Close warning distinct from observed individual legacy order rows | EVIDENCE INSUFFICIENT | Do not assume the two warning entries are KO/KR/TMO without immutable identity matching |

## Code changes in this pass

- `trader/us/runner/status_contract.py` — opening 30m BUY block categorized as known policy.
- `trader/us/runner/trade_tick_runner.py` — do not mark opening-only denied BUY as entry liveness failure; preserve degradation when session guard, exit authority, balance, fills, source stream, reconciliation, or system invariant is not healthy.
- `tests/us/test_us_health_status_classification.py` — policy vs unresolved ACK/system invariant/Close regression.
- `trader/us/market_state_overlay.py` — TP retry fence now reports original key/stage/cycle/latest evidence and explicit per-stage operator action; never releases an unproved historical SELL.
- `tests/us/test_tp_pre_http_retry_proof_20261105.py` — KO/KR historical no-proof retry denial, stale abort vs broker-order precedence, TP1/2/3 generic coverage.
- `trader/us/entry_timing/opportunity.py`, `trader/us/pb1/us_entry_engine.py` — log real native completed-minute age/opening coverage for WAIT; no parameter or strategy change.
- `tests/us/test_us_price_aware_opening_entry_20261029.py` — stale/missing minute evidence without relaxing the WAIT.

## Release blockers

1. Current final SHA CI including PostgreSQL and fault-injection is required; queued/running or conditional SKIPPED is **not PASS**.
2. **KO/KR retry cannot be unfenced** from `last_status`, elapsed time, or DB absence alone. No authenticated native KIS order/status evidence available here. No operating data was changed.
3. All 11 current broker holdings, original orders, intervening fills, exchange/page completeness and TQQQ same-cycle READY→ACTIVE activation **NOT VERIFIED** by authenticated KIS. DB copies are not a substitute.
4. WSL installed SHA, active .env/ON+ENFORCE flags, scheduler and actual end-to-end practice BUY→overnight→SELL **NOT VERIFIED**.
5. Any future safe auto-retry must first atomically prove the exact durable attempt ABORT_PRE_IO or full/partial terminal KIS status, CAS-fence stale attempts, persist scoped fill/remaining TP, and demonstrate concurrency/restart/late-fill PG scenarios. This pass does not claim to have implemented that new live KIS reconciliation.

**Decision**: PR remains DRAFT and MERGE/DEPLOY HOLD. No blanket SHADOW/OFF or trading-policy edits.


## 2026-11-13 follow-up: proof precedence / router safety

- GitHub PR #353 base remains `00f182ae9ff86a5615763f05f7a7e184a2e87277`; the PR branch is **not deployed**.
- `trader/us/runner/status_contract.py`: policy BUY restrictions must not mask an independent failed protective SELL, actual entry fault, or reconciliation requirement. Regression tests cover policy+SELL, policy+unknown ACK, and policy+invariant failure. `trade_tick_runner.py` now preserves the session's inbound permission before applying opening hours, avoiding false EXPECTED classification of a denied upstream gate.
- `trader/us/execution/order_router.py::_tp_unsubmitted_attempt_may_release_stage`: empty DB queries and a clean current broker balance are **not** evidence of no historical HTTP POST. A release candidate requires an explicit immutable same-key/same-attempt PRE_IO abort, no broker-possible contradictory attempt, and absence of unresolved active claims/other open SELL; the exact ledger row is independently CAS/locked on release. Regression includes empty event list, wrong attempt, unknown late event, actual ACK, and balance reserved by another SELL.
- **Consequence / pending design step:** Before the router creates a submit attempt, it may be unable to certify a same-attempt PRE_IO abort. Such a fresh pre-submit rejection remains fenced until an atomic intent-only guard-abort lifecycle is implemented and verified. Do **not** count this stricter fail-closed behavior as completed safe automatic retries.
- Read-only Supabase evidence for **KO/KR TP2** using the full original TP2 client-key pattern returned zero matching `us_order_events`, zero `us_execution_attempts`, and zero `us_orders` rows, despite original `us_profit_capture_lifecycle` TP2 attempt-key metadata. This is **missing evidence**, not an authenticated KIS denial or guaranteed pre-HTTP abort. No PostgreSQL status mutations or KIS GET/POST were performed.
- GitHub US Harness includes full `tests/us` pytest plus real PostgreSQL integration and an offline KIS-HTTP prohibition check; use final commit workflow conclusions before any merge. Current tests are code-backed but KIS and WSL end-to-end execution remain NOT_VERIFIED.


## 2026-11-13 follow-up: atomic PG TP release gate

**Additional patch**:
- `trader/us/db/repos.py::release_us_profit_capture_pre_http_abort`: acquire exact TP row `FOR UPDATE`, require the active trading epoch, require immutable `us_order_events` `BROKER_SUBMIT_ABORTED_PRE_IO` or `BROKER_SUBMIT_ABORTED_BEFORE_BOUNDARY` for the exact key/stage/epoch and a single attempt, reject any conflicting or broker-possible event, lock the matching `us_execution_claims` row `FOR UPDATE OF c`, prove the `us_execution_attempts` row has `ABANDONED_PRE_SUBMIT` and `authoritative=true` while the Claim is `OPEN` without an active attempt, and reject any matching `us_orders` SELL row. Only then change the TP stage in the same transaction. No KIS POST and no operating DB change.
- Updated migrated-PG test: a `PENDING` TP with no journal or Claim must stay `PENDING`; a new migrated-PG integration case creates an exact durable abort and releases the matching Claim, then proves a single stage-release and duplicate-abort idempotency.
- **Remaining**: authenticated KIS historical original orders (KO/KR/TMO) and existing holdings have not been certified; the missing TP2 rows are not proof of unsent HTTP. Fresh router-side pre-submit invalid TP still requires a separate atomically evidenced intent-only abort state so the stricter fence does not become permanent. No code change to strategic thresholds or TQQQ owner.
