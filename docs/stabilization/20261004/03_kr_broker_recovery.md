# Stabilization 3/5 — KR unresolved ACK, retry semantics, and sell liveness

Base branch: `stabilize/02-us-broker-recovery-20261004`.

## Purpose
Apply the common execution-state contract to KR broker submission/recovery without changing KR strategy policy. Fix the current semantic-fence asymmetry and preserve SELL liveness.

## Confirmed current-code defect to reproduce first
`trader/kr/pb1_stability.py::same_day_semantic_sell_exists()` currently treats statuses in `ATTEMPT_STATUSES` as blocking, but `UNRESOLVED_ACK` and `RECONCILE_ERROR` are absent while `REJECTED` and `CANCELLED` are included.

Required classification:
- `UNRESOLVED_ACK` / broker-truth-unknown path not fenced by this semantic guard: **IMPLEMENTATION BUG**.
- Blanket same-day blocking of explicit REJECT / authoritative zero-fill CANCEL: review current policy and convert to state-driven retry semantics rather than permanent blocking; do not silently change strategy thresholds or SELL reasons.

## Required behavior
1. Pre-submit failure: retry-safe; no unresolved broker fence.
2. Post-submit uncertainty: durable semantic-action fence; no duplicate submit until broker truth.
3. Explicit rejection: terminal attempt, semantic action may become RETRYABLE after fresh strategy/risk/balance validation.
4. Authoritative zero-fill cancel: release reserved quantity and allow a later valid attempt.
5. Partial fill + cancel: preserve actual filled quantity, reserve/re-evaluate only remaining semantic target.
6. Cancel ACK without authoritative fill evidence: do not assume zero fill.
7. Cumulative fill observations are monotonic/idempotent.
8. Restart/session/date rollover does not clear unresolved broker truth.
9. Different semantic actions for the same lifecycle remain independent: completing TP1 must not block TP2/TP3 or a distinct hard-stop/full-exit action.
10. If a lower-priority SELL is unresolved and a higher-priority emergency SELL appears, reconcile/cancel/confirm remaining sellable quantity first; do not blindly submit another SELL.

## Pending/reserved quantity
Implement or normalize reservation semantics so an in-flight SELL reserves only its unresolved remaining quantity. Partial fill reduces reservation; terminal zero-fill cancellation releases it; confirmed full satisfaction clears it.

The reservation must be based on broker-confirmed/known cumulative fill, not requested quantity alone.

## Database/guard failures
A semantic-fence DB lookup failure must not silently become `no prior order` if that can permit a duplicate broker submit. Reuse PR1 execution-claim/ledger behavior and fail closed only for the affected semantic action unless account-wide broker truth is unavailable.

Do not freeze unrelated symbols/owners unnecessarily.

## Owner separation
Preserve KR_STANDARD / KR Infinite or any dedicated owner contracts. Do not attach a broker fill to a lifecycle by symbol name alone.

## Required tests
Use production KR submit/reconcile/repository functions with a fake KIS adapter and PostgreSQL test persistence where relevant.

1. Existing `ACK` same semantic action -> duplicate submit blocked.
2. Existing `UNRESOLVED_ACK` -> duplicate submit blocked and reconciliation runs first.
3. Existing `RECONCILE_ERROR` with unresolved broker truth -> duplicate submit blocked.
4. Explicit REJECT with no broker fill -> later fresh valid SELL may retry.
5. Authoritative zero-fill CANCEL -> later fresh valid SELL may retry.
6. Partial fill + CANCEL -> fill accounted exactly once; only remaining target is retryable.
7. Cancel ACK with missing fill qty -> still uncertain, no retry.
8. Worker restart/date rollover while unresolved -> no duplicate submit.
9. Two competing workers -> one execution claim only.
10. TP1 satisfied -> TP2 remains eligible; separate hard-stop/full-exit action is not blocked by same-day semantic family logic.
11. Normal balance + valid SELL condition + no unresolved attempt -> SELL evaluation reaches intent/router; duplicate protection must not create permanent SELL starvation.
12. DB semantic-lookup failure -> affected action is protected and health exposes degradation.

## Regression responsibility
Review relevant PR135–154 KR changes, especially BUY-contract propagation, epoch recovery, runtime integrity and SELL-liveness fixes. Do not remove proven guards unless the same production path is covered by replacement tests.

## Health
Expose KR unresolved semantic actions, reservation mismatch, execution-claim conflict, broker/local cumulative-fill mismatch and reconciliation failure counts.

## Non-goals
- No TP/stop/defense thresholds or sell fractions changed.
- No live trading.
- No DB reset/delete.
- No evidence-free repair of historical holdings.
- No PR merge.

## Completion evidence
For every changed status transition, show before-failing/after-passing tests and explain why duplicate prevention does not block valid future SELL attempts.
