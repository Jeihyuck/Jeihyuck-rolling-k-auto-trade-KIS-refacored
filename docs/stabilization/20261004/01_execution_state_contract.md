# Stabilization 1/5 — KR/US execution state contract

Baseline: `dual-agent` @ `5a2c3fb86f01ad30fd8dc22f3fe2eb001abc5626`.

## Goal
Create one explicit execution-state contract shared by KR/US semantics while preserving market-specific KIS adapters. This PR must not change strategy philosophy, TP thresholds, sell fractions, market-state policy, PB1/TQQQ/KR Infinite ownership, or existing frozen BUY contracts.

This is an execution-safety PR, not a strategy PR.

## Required semantics
Separate **semantic action** from **submit attempt**.

A semantic action identifies the economic goal for one position lifecycle/cycle (for example TP1, TP2, DEFENSE_TRIM, TREND_TRIM, HARD_STOP, full liquidation). A submit attempt identifies one broker submission used to satisfy that action.

The contract must distinguish at minimum:

### Submit attempt states
- CREATED
- SUBMITTED
- ACKED
- UNRESOLVED / AMBIGUOUS_ACK
- PARTIALLY_FILLED
- FILLED
- REJECTED_EXPLICIT
- CANCELLED_ZERO_FILL
- CANCELLED_PARTIAL_FILL
- RECONCILE_ERROR / UNKNOWN_BROKER_TRUTH

### Semantic action states
- OPEN
- IN_FLIGHT
- UNCERTAIN
- PARTIALLY_SATISFIED
- SATISFIED
- RETRYABLE
- SUPERSEDED

Do not claim exactly-once delivery to KIS. The required guarantee is:

> At most one unresolved broker submit per semantic action; broker executions are accounted idempotently; a retry is allowed only after prior broker truth becomes terminal/retryable.

## Invariants
1. Failure before broker HTTP submission is retry-safe and must not be treated like post-submit uncertainty.
2. If broker submission may have occurred but result is unknown, preserve the same attempt and block a new submit for the same semantic action until broker truth is established.
3. ACK is not fill.
4. Cancel ACK is not zero-fill evidence by itself.
5. Missing fill quantity is unknown, never silently converted to zero.
6. Cumulative filled quantity must never regress and repeated observation must not create additional fills.
7. Partial fill + cancel must preserve filled quantity and make only the remaining semantic target eligible for later evaluation.
8. Explicit rejection or authoritative zero-fill cancellation may release the attempt fence and make the semantic action RETRYABLE, subject to current strategy/balance/risk conditions.
9. The unresolved fence must survive process restart, session transition, and trade-date rollover until terminal broker truth exists.
10. A completed TP1 must not block TP2/TP3 or a distinct emergency liquidation action.
11. A pending lower-priority SELL must not be bypassed by submitting another SELL blindly. Higher-priority exits must reconcile/cancel/confirm remaining sellable quantity first.
12. Owner identity and lifecycle/cycle identity are mandatory fence dimensions; PB1, TQQQ Infinite and KR Infinite must never share a semantic action accidentally.

## Concurrency requirement
Do not implement `SELECT then INSERT` as the sole submit guard. Provide an atomic durable execution claim/lease or equivalent DB constraint/transaction so two workers racing for the same semantic action cannot both acquire submit rights.

The key must include the necessary account/env/market/epoch + strategy owner + lifecycle/cycle + semantic action dimensions. `trade_date` is audit metadata, not the only fence lifetime boundary.

## Existing code to inspect before editing
- `trader/execution_state.py`
- `trader/kr/pb1_stability.py`
- `trader/us/execution/order_router.py`
- `trader/us/execution/order_journal.py`
- KR/US DB repositories and reconciliation paths
- PR135–154 changes relevant to BUY contract, epoch, unresolved ACK, cancel/fill recovery, runtime integrity

Do not replace working components wholesale. Reuse existing order journal, unresolved-ACK handling, reconciliation and epoch identity where possible.

## Required tests
Use real production functions, not only a self-contained model.

1. Broker accepts order, client receives timeout -> next tick -> process restart -> next trade date: same semantic action produces zero additional broker submits until broker truth is terminal.
2. Explicit broker reject -> semantic action becomes retryable and a later valid attempt can submit.
3. Authoritative zero-fill cancel -> retry allowed; cancel ACK without fill evidence -> still uncertain.
4. Partial fill + cancel -> cumulative fill is preserved; only remaining target can be retried.
5. Two concurrent workers contend for the same semantic action -> exactly one obtains submit authority.
6. TP1 satisfied -> TP2 remains independently eligible.
7. Existing emergency exit cannot blindly bypass an unresolved SELL; reconciliation establishes confirmed remaining quantity first.
8. KR and US adapters map equivalent broker outcomes to the same contract states without losing market-specific fields.

Where a PostgreSQL-backed execution-claim test is required, use the project test DB/test fixture; never send a real KIS order.

## Health visibility required in this PR
From this first stabilization PR onward, expose unresolved/uncertain action counts and execution-claim conflicts in health/reporting. Do not wait for the final close-integrity PR to make uncertainty visible.

## Non-goals
- No DB reset/delete.
- No live trading.
- No same-day session restart.
- No strategy threshold/ratio changes.
- No PR merge.
- Do not weaken tests or guards just to obtain green CI.

## Completion report
Report changed functions/files, old behavior, new state transition, tests that failed before and pass after, CI status, and any behavior not yet migrated to this contract. CI green alone is not proof of live stability.
