# KR PB1 AM/afternoon 2026-10-19: budget recovery

Base branch: `dual-agent` at `28ceff3c79ac67de870d0b8dbf4b1051164283dc`.
Strategy owner: `pb1_pullback_close` only. KR_INFINITE/122630 and protective SELL use their existing owner paths. PR #214–#216 are already merged into this base.

## Supabase evidence (2026-10-19, KST)

- PREP `PREP_DONE` succeeded before market open.
- Of 20 completed PB1 morning ticks through 10:16, 13 finalized as `RECOVERABLE_DB_TIMEOUT` with note `TICK_TIMEOUT_KRX`; this describes the whole-tick watchdog, **not** proof that a SQL statement ran for 180 seconds.
- Three ticks had `ORDER_CANDIDATE_BUT_ZERO_API_SUBMIT`.
- Example: BUY intent for 096530 (10:01 KST) and 251970 (10:09 KST) ended at `KR_TICK_DEADLINE_EXHAUSTED_BEFORE_QUOTE_RETRY`, followed by `PRETRADE_CHECK_FAILED`. Local response evidence says `broker_submit: false` for known `ERROR` rows.
- As of 10:30 KST, the orders table has `CREATED` BUY 096530 from 10:05:34 KST with no `submitted_at`, `acked_at`, or broker ID. It may be an incomplete pre-broker attempt, **but cannot be declared absent at KIS without broker-side reconciliation**. Do not expire or retry this row merely because its status is `CREATED`.
- 192820 had an `EXPIRED` historical same-day row. Duplicate-intent and open-order gates must be verified against the current epoch and broker truth before any change.
- `fills` showed zero for this morning. Broker acknowledgement and fill are independent proofs.

## Code changes

1. Retire the legacy 90/180s live AM/afternoon defaults and migrate to **360s tick watchdog**. Preserve explicit non-legacy operator overrides (e.g. 210s). Keep the close session unchanged.
2. Retire the legacy 75/105/200s minimum session-end budget for AM/afternoon; require **380s** remaining before starting a new tick. Existing KIS submit minimum reserve 20s and post-engine reserve 20s are untouched.
3. Synchronize Windows/WSL AM and afternoon shell defaults with the Python runtime. Avoid `.env`-induced 180s fallback.
4. Keep strict PB1 pretrade quote freshness/validity but bound the **outer REST retry rounds only for PB1 BUY** to two by default (`KR_PB1_PRETRADE_QUOTE_RETRY_CAP`). Normal quote callers keep their previous `KIS_RETRY_MAX`. The PB1 pretrade fails closed if no fresh, valid quote is obtained.
5. Do not change sizing, entry signals, SELL, KR_INFINITE, duplicate protection, reconciliation, KIS order endpoint minimum budget, or per-session timing.

## Safety / known limitations

- The increased deadline mitigates timeout-triggered dropped candidates but is **not proof of a faster tick**. A tick that still exceeds 360s will time out safely.
- `CREATED`/duplicate-intent residual orders are not silently cleared. Broker-side reconcile is required before determining whether they were submitted. This PR does not make unsafe assumptions from missing DB broker IDs.
- Because the local deployment pins the first KR session's SHA per trading date, merging this PR **does not automatically update the 2026-10-19 afternoon process**. A separately approved, quiescent, verified SHA/pin transition is required for same-day deployment.
- Do not change Windows Scheduler, skip KIS submit guards, force a test order, or merge automatically.

## Verification

Unit regression coverage:
- 360s full tick and session end grace arbitration.
- 380s minimum budget and migration of 180/200s legacy `.env` values.
- User overrides remain intact, close still uses short timeout.
- Strict PB1 REST fallback requests only two outer quote retries; non-PB1 callers retain old retries.
- Missing/stale/halted quotes still do not authorize broker submission.

Required before merge:
- Run focused KR test suite, run existing safety tests, and verify GitHub CI.
- Record change in end-to-end simulated tick with candidate -> valid quote -> protected KIS submit stub. Test invalid quote, broker ambiguous ACK, close handoff, orphan CREATED, and zero-fill reconciliation.
- Real KIS/practice brokerage verification requires explicit separate approval.

## Oct-19 afternoon follow-up and final mitigation scope

- As checked during the afternoon, the current KR runtime was still on the original pinned SHA `28ceff3c79ac67de870d0b8dbf4b1051164283dc`. Recent afternoon PB1 rows continued to show `TICK_TIMEOUT_KRX`; this PR was not running live at that point.
- One 096530 pre-broker `CREATED` row subsequently transitioned to `EXPIRED` in Supabase; no assumption is made that KIS was never contacted solely from status and missing timestamps. **Do not perform mass expiry or disable duplicate guards.**
- The new PB1 BUY pre-intent budget gate (`KR_PB1_BUY_PREINTENT_MIN_REMAINING_SEC`, default 45s, floor 25s) avoids writing a blocking `CREATED` row when time is already insufficient for quote and broker submit reserve.
- If ledger intent recording fails after a durable row was written, or no KIS client exists on a known pre-broker path, the row is best-effort marked retryable `ERROR` with `broker_submit:false`. A failed DB cleanup is logged and remains blocked for reconciliation; any path that may have crossed KIS submit is excluded.
- Canonical PB1 pretrade quote probes only the normalized six-digit KR symbol and KIS domestic `J` market for its at-most-two REST retry rounds. The generic quote API retains its original market/code variant fallback and `KIS_RETRY_MAX` behavior.
- The offline acceptance suite includes a 240s candidate simulation under the 360s shared tick deadline with no overlapping ticks and no KIS network calls, as well as prebroker cleanup, quote retry and source freshness tests.

### Expected run-level evidence after deployment

```text
[KR_P1][BUDGET_CONTRACT] ... tick_hard_timeout_sec=360 min_tick_budget_sec=380 ...
[KR_TICK][DEADLINE] ... budget_sec=360.000 timeout_sec=360 ...
[KIS][QUOTE][BOUNDED_PRETRADE_RETRY] ... max_attempts=2 ... canonical_kr=1
[ORDER][PRE_INTENT][LOW_TICK_BUDGET] ... action=DEFER_WITHOUT_CREATED_ROW  # only if remaining time is insufficient
```

**Do not infer a successful KIS order from those budget logs.** Prove `ORDER_INTENT -> attempt -> KIS submit -> broker ACK -> reconciliation -> fill` independently and retain broker-ambiguous protections.

## Full Oct 19 archive follow-up — additional blockers

Source archive: `nullim-dual-agent-kr-logs-20261019-160004.tar.gz`. This is **2026-10-19 old runtime SHA `28ceff3c...`**, not verification of this PR. AM + PM had zero confirmed broker submissions despite valid candidates; PREP Final30 scored 30 rows.

### P0: completed-daily Final30 evidence dropped in runtime projection

- Final30 artifact rows store `as_of=2026-10-16`, `candidate_family_proof_as_of=2026-10-16`, `candidate_family_screens`, family percentiles and breakout proof fields.
- `PB1Engine._map_precomputed_candidate_row` merged those fields, but `_compute_candidates_from_codes` rebuilt `features` with a limited dictionary and omitted the proof fields before calling `_evaluate_final30_entry_setup`. That produced false `completed_daily_proof_date_mismatch` for many families.
- Fix: copy only matching-code original scored Final30 PREP-row proof fields into runtime features. Missing or mismatched proof remains missing and fails closed. No synthetic pass flags, score changes, or new strategy owner.

### P0: Oct13 historical protective SELL misclassification

- Supabase ties the blocked October 13 stop SELLs on `036540,039030,042700,066570,067310` to exact git SHA `eb924b380ace9a5e2a0d9e7b65fa1ca15a379d81`.
- **Code-path proof:** in that exact SHA, `sell_stock_market` called `get_positions` **before** `_order_cash` (the first broker order HTTP path). The uniquely thrown `KisBalanceUnavailable('KR_BALANCE_BUDGET_EXHAUSTED_BEFORE_FETCH')` therefore occurred before submitting any broker order. The then-current ambiguity classifier did not recognize that exception, incorrectly writing `UNRESOLVED_ACK`, `broker_submit:true` and an optimistic `submitted_at` timestamp. No order number/ACK was stored.
- Added a one-time, transactional **source-SHA + original run + 2026-10-13 date + active epoch + hashed account + practice-only + exact five-code + action + response-code + no-fill** recovery. Preserve original misclassified response inside the corrected order evidence. Release ONLY those proven pre-HTTP claims to retryable OPEN and mark old order ERROR; retain everything else fenced. This is a legacy incident migration, not a generalized release of uncertain ACKs.
- If the historical proof or healthy daily-ccld reconciliation is unavailable, the claim **remains blocked**; do not bypass its protection.

### P1: KR_INFINITE owner timing

- Old runner invoked `run_kr_infinite_sleeve_tick` only after PB1 entry, yielding 12 `KR_INF_TICK_BUDGET_DEFERRED` across AM + PM; unrelated PB1 BUY should not consume all owner evaluation time.
- New runner calls owner once via existing `_kr_after_protective_exit` handoff, **after PB1 protective SELL evaluation and before unrelated PB1 entry**. Uses the same authoritative balance snapshot and `allow_entry` gate. KR_INFINITE logic/sizing/capital controls unchanged. If handoff fails, the sleeve is not re-run after a partial engine exception.

### P1: poisoned psycopg transaction during trading-epoch checkout

- The old timestamped traceback shows a `_HardTickAbort` interrupting psycopg `pool_pre_ping` before `active_trading_epoch_id` entered its exception handler, causing `can't change 'autocommit' now: connection in transaction status ACTIVE` during driver ping cleanup.
- The new epoch lookup wraps connection checkout, disposes poisoned pools and fails closed on unknown epoch, using a fresh pool on next tick. This does not weaken epoch or duplicate-order safety.

### Known separate broker-truth issue, not repairable from this archive alone

- A **different** 2026-10-14 066570 SELL has genuine KIS broker order `0000002941`, and holdings-based confirmation of quantity 5, but missing execution price and unresolved realized PnL. This order **must not** be treated as the October 13 false-ambiguous one. Never fabricate price/fills/PnL from the limit price or average cost.
- Existing read-only `python -m trader.kr.unpriced_sell_audit --env practice --trade-date 2026-10-14` reports the missing broker price. Resolution requires authoritative order/fill-price proof from KIS, then immutable audited reconciliation. Do not turn such unpriced orders into new broker SELL intents.

### Release gate for next market session

- GitHub CI for **final PR head**, including targeted Final30 family-provenance tests, exact-historical-SHA SELL recovery tests, epoch poisoned-checkout tests and KR_INFINITE ordering tests.
- Deploy only after the trading-day SHA is quiescent; no mid-session hot replacement of code or pin. Confirm AM/PM `360/380` effective runtime logs.
- Reconcile 10/13 source-proven prebroker claims *before* new protective SELL signals; verify `OPEN` claim state and no active attempt, then require full KIS order submit → ACK → fill/reconciliation evidence for any new SELL.
- Validate PB1 candidate setup proof provenance, KIS quote, broker submit timing and no duplicate intents. Monitor KR_INFINITE timing and owner capital gates separately.
- CI success does not prove that tomorrow's live KIS API, market conditions, or fills will succeed. No test orders are authorized.
