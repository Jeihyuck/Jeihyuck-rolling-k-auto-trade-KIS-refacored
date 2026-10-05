# Stabilization 2/5 — US ambiguous ACK, broker-truth recovery, and live incident regressions

Base branch: `stabilize/01-execution-state-contract-20261004`.

## Purpose
Implement the common execution contract for US order submission/recovery and fix the 2026-10-01 live-practice failures without changing strategy policy.

## Live incidents that must become regression tests
### NVDA duplicate DEFENSE_RISK_OFF_TRIM
Observed economic sequence:
- Existing US_STANDARD NVDA position had 10 shares.
- Strategy correctly requested SELL 3 `DEFENSE_RISK_OFF_TRIM`.
- Broker accepted/filled, but client path ended `BROKER_SUBMIT_RESULT_UNKNOWN` / `AMBIGUOUS_ACK` after a ReadTimeout.
- Next tick broker position was already 7, yet the same semantic action was submitted again for 3 and also filled.
- Result: 10 -> 7 -> 4 instead of one intended trim.

Required result:
- First post-submit uncertainty places the NVDA semantic action in UNCERTAIN with a durable submit fence.
- Next tick/restart/date rollover performs reconciliation only for that action until broker truth is established.
- No second broker submit occurs for the same semantic action.
- Once broker fill is confirmed, the action becomes SATISFIED with the confirmed cumulative fill quantity.

### JNJ orphan/rebound failure
Observed economic sequence:
- US_STANDARD `us_pb1_exit` trend trim generated SELL 2.
- Broker filled order 36025 around 262.905, but the original local intent remained PENDING after an ambiguous submit.
- Reconciliation created `KIS_IMPORTED_..._JNJ_SELL` instead of rebinding the broker execution to the existing local intent.
- Trend state stayed `trend_trim_pending=true`, `trend_trim_done=false` despite broker position moving 8 -> 6.
- Imported fill lacked the original sell attribution/avg-cost lineage, causing realized-PnL incompleteness.

Required result:
- Exact broker order/client key match first.
- Otherwise inspect durable submit events/unresolved intents and accept only a unique high-confidence match.
- If exactly one candidate matches, rebound broker order/fill to the existing intent and preserve owner/lifecycle/reason/entry contract/avg-cost attribution.
- If multiple candidates remain, preserve the broker fill as UNATTRIBUTED and raise integrity/degraded state; do not guess owner/lifecycle.
- Create `KIS_IMPORTED` only after proving there is no local unresolved/ambiguous candidate.
- Recovered JNJ fill must drive `trend_trim_pending=false`, `trend_trim_done=true` using actual fill evidence.

## KIS order timestamp normalization
Investigate endpoint/source semantics of `ord_dt` + `ord_tmd`. Live evidence from 2026-10-01 shows values matching KST while current normalization can produce a 13-hour-shifted UTC timestamp. Do not blindly hardcode a timezone globally; identify endpoint/source contract and preserve source timezone/provenance. Add EDT/EST and midnight-boundary tests.

This timestamp issue may contribute to ambiguous-order matching but must not be declared the sole root cause unless reproduced.

## Broker fill accounting
- Broker execution IDs/order numbers and cumulative quantities are authoritative.
- Duplicate broker observations must not create duplicate fills.
- Cumulative filled quantity cannot regress.
- Missing fill quantity is unknown, not zero.
- Cancel ACK alone cannot close a missing-fill ambiguity.
- Partial fill followed by cancel and later fill observation must converge without double counting.

## US semantic fence behavior
Use PR1's action/attempt contract. The fence must distinguish:
- UNCERTAIN/UNRESOLVED -> no resubmit; reconcile first.
- explicit REJECT -> retryable after current strategy/risk/balance revalidation.
- authoritative zero-fill CANCEL -> retryable.
- partial-fill CANCEL -> remaining target only.
- FILLED/SATISFIED -> same action not repeated.

Do not use a blanket same-day SELL block that would prevent TP2/TP3, later hard-stop liquidation, or a distinct action.

## TQQQ ownership
Do not attribute US_STANDARD/PB1 broker executions to TQQQ Infinite and do not reuse TQQQ state for PB1. Preserve strategy_owner/sleeve/lifecycle identity during recovery.

## Required production-path tests
Tests must include actual `order_router`/journal/repository/reconciliation functions and real PostgreSQL test persistence where relevant; KIS must be a fake/test adapter only.

1. NVDA: broker accepts SELL 3, client ReadTimeout -> next tick -> restart -> next trade date -> submit count remains 1 -> broker fill recovery -> semantic action SATISFIED.
2. JNJ: local PENDING/UNRESOLVED + broker FILLED -> existing intent is rebound -> no imported duplicate order -> trend stage marked done.
3. Multiple ambiguous local candidates for one broker fill -> no guessed rebound; integrity error/degraded state.
4. No local candidate -> imported-order fallback remains supported and auditable.
5. Partial fill -> cancel ACK -> late cumulative fill observation -> no duplicate fill and correct remaining target.
6. ACK DB persistence failure -> recover from journal/broker evidence without duplicate submit.
7. Same semantic action from two session workers -> PR1 execution claim permits only one submit.
8. Recovered fills preserve PnL attribution fields; close/report can account them.
9. KIS timestamp normalization tests cover EDT, EST and KST-midnight edge cases.

## Health requirements
Expose at minimum:
- unresolved US semantic actions
- unattributed broker fills
- rebound success/failure counts
- duplicate semantic submit detection
- broker/local cumulative-fill mismatch

## Non-goals
- No strategy threshold/ratio changes.
- No live order.
- No DB reset/delete or evidence-free position mutation.
- No PR merge.

## Completion evidence
Show the same NVDA/JNJ scenarios failing before the code change and passing after it. Report changed execution path, CI, any unresolved recovery edge cases, and any separate operational data repair required for historical 2026-10-01 rows.
