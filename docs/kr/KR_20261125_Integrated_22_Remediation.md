# KR 2026-11-25 통합 수정 지시서 — 22개 필수 작업

Base: dual-agent @ 6846994f3e157ad2d841ae280d72fe7f0d11c4be. Prior #395/#396 merged. All 22 requested items are mandatory, including the unchecked exit verification item. This is a draft implementation branch, NOT an assertion that all defects have been fixed. No production orders, DB mutations, deployment or automatic merge.

## Evidence
11:08 KST: 61 PB1 runs (36 policy WAIT, 9 retryable data errors, 15 opening blocks, 1 running), zero new orders/fills in queried DB. KODEX 122630 owner KR_INFINITE: 15 @ 47070, hashkey HTTP 500, intent REJECTED, claim RETRYABLE, zero fill and no broker id, no new intent; state ACTIVE, 39 shares. PB1 minute conflicts 6, stale 3; 357870 BREAKOUT missing exchange quote time 34 (also trigger false, no automatic BUY). 028050 11/19 Runner Final 8 RETRYABLE, 33 bought and 33 sold according to prior PR evidence. 293490 10/27 BUY 174 UNCERTAIN, active attempt remains. Pumtech 09:40:57 actual ask 76500, four closes 76100/76300/76400/76400, lows 76000/76000/76300/76400, volume 502/407, opening high 79000; 09:44:56 ask 77400, stop 76300, RR 1.4545, cap 77300. Full original KIS and WSL logs not available. No causal claim for historical claim blocking today's candidate.

## 22 item implementation and acceptance matrix
Statuses: PARTIAL = some implementation on this branch, OPEN = remaining work, VERIFY = existing implementation needs evidence, not a completion mark.

| ID | Required work | Status at draft creation | Acceptance |
|---|---|---|---|
| 01 P0 | KR_INFINITE retry | OPEN | runner/repository/strategy share durable retry semantics; pre-submit hashkey failure proof + exact broker nonacceptance, new attempt under same unit/cycle; re-evaluate current owner gates, remaining quantity, capital; no duplicate. |
| 02 P0 | Native bar conflict auto repair | VERIFY/OPEN | #395 60s targeted repair, three confirmations and quarantine retained; actual conflicting rows/source/recovery timestamp, no synthetic bar, independent verified 5+3 only. |
| 03 P0 | Native bar freshness | VERIFY/OPEN | HOT priority, bounded per-symbol deadlines, correct timestamp and exact missing minute; stale source stays UNKNOWN; p95 <=90s target with original KIS evidence. |
| 04 P0 | KIS volume proof | OPEN | 357870 native exchange timestamp/trade date + cumulative volume validated, REST/WS identity; do not substitute local receipt clock; separate trigger false. |
| 05 P0 | 028050 closed SELL claim | VERIFY/OPEN | trace existing #395 closed_cycle_claim_recovery predicate and production caller. Match all scoped native orders/fills/attempts; if proved, SUPERSEDED not fabricated SATISFIED; no resend 8. |
| 06 P0 | 293490 historical BUY claim | OPEN | recover 10/27 174 UNCERTAIN via original KIS order/cancel/fill/account/epoch/cycle evidence; no forced deletion or unproved terminalization. |
| 07 P0 | Original SST BUY contract | VERIFY | immutable owner/entry style/exit policy/contract hash and account+trading/portfolio epoch+cycle survive restart and reconciliation; no cross-owner substitution. |
| 08 P0 | Partial fill and remaining retry | VERIFY | same semantic action, new attempt only after authoritative terminality; cumulative broker fill dedup, remaining only, no overlapping attempts or notional inflation. |
| 09 P0 | TP/SL/final close | VERIFY | TP1/2/3/runner and current -3/-6 policy plus protective sell, partial/cancel/retry, final fill and DB/KIS exact match; active runtime integration evidence. |
| 10 P1 | V3 rise-and-hold | PARTIAL | standalone candidate geometry + reconstructed fixture; integrate into canonical practice PB1 path after full proof, do not accept all-flat or lower lows. |
| 11 P1 | Intraday rolling peak | PARTIAL | standalone causal confirmed local peak; validate complete dependency/conflict window and ordered peak/trough, no lookahead, no artificial target, owner breakout preserved. |
| 12 P1 | Price ceiling first | PARTIAL | V2 computes diagnostic ceiling before RR rejection; standalone V3 returns price interval. Actual quote/plan integration added after draft creation; see progress below. No WAIT authorization. |
| 13 P1 | HOT minute decision | OPEN | existing HOT/WARM observer 45/180s not same as decision cadence; 1 minute priority evaluation, single writer, no duplicate claims or broker rate breach. |
| 14 P1 | Tick latency | VERIFY/OPEN | stage timing p50/p95/max, bounded lock/I/O/DB, actual 180s submit fence and separate watchdog, protect SELL priority, AM/PM continue. |
| 15 P1 | V2 label | PARTIAL | production early UNKNOWN/WAIT policy identity fixed, regression pending; keep true owner fallback legacy; check emitted ledger serialization. |
| 16 P1 | Monitoring and mail | OPEN | independent heartbeat and stale AM/PM/close/observer, error and mail delivery evidence, dedup alerts, watchdog recovery without overlapping orders. |
| 17 final | Final30 regression | VERIFY | identical PREP universe/ranking/RS/VCP/Minervini, contracts, score and risk eligibility before timing. |
| 18 final | PB1/KR_INFINITE/US isolation | VERIFY | correct owner/cycle/account/market, no US or real change, no ETF tick mixing. |
| 19 final | -3/-6 + TP unchanged | VERIFY | compare existing active policies and config, no new structural reference used as live stop. |
| 20 final | Duplicate order prevention | VERIFY | race/restart, ambiguous response, partial fill/cancel, idempotency, broker unknown; fail closed. |
| 21 final | BUY through final SELL | VERIFY | production entrypoint integration with offline broker/clock/DB fixture, then authorized actual practice evidence. |
| 22 final | Practice ACTIVE | OPEN | approved ENFORCE profile and actual deployed SHA, first eligible READY->intent->claim->KIS->fill->SST->position->exit; no forced trades. Do not activate real account or silently leave final accepted policy shadow. |

## Implementation details / hard boundaries
- KR_INFINITE: `intent_keys()` currently includes REJECTED, `evaluate()` rejects duplicate key, `create_intent()` unique insert; do not merely exclude REJECTED or generate new semantic keys. Need durable reentry after verified zero-fill terminal attempt, atomic claim, replay existing intent, retain history and immutable scope. Recheck price/regime, distinguish pre-POST hashkey from post-submit uncertainty and require authoritative reconciliation as applicable. If previous order terminal with partial fill, remaining only per owner policy.
- Minute observer: capture source/timezone/session, exact request anchors, bar identity/OHLCV versions and retry outcome; keep previous quarantine audit. Current 4 consecutive V2 native post-open or independent 8; no global 25-bar BUY block. Do not manufacture missing bars. V3 wider dependencies must explicitly validate all consumed bars and quarantine. Existing recovery can already return to normal; do not label every safe UNKNOWN as implementation defect.
- Volume: native exchange event time versus local receipt must be distinguished; not bypass proof. Current 357870 candidate has BREAKOUT trigger false independently. Preserve volume/risk/owner gates.
- Closed cycle and historical claim: read-only evidence acquisition first, explicit approval required for operational DB repairs. Existing #395 implementation should be traced, not duplicated blindly. Any unresolved evidence remains fenced only as appropriate to scope; never remove claim history.
- V3: compare separate signal and price feasibility. Rising three closes retained; rise-and-hold requires at least one increase and nondecreasing lows + volume >=0.8; local high precedes trough, no future information. Keep 0.5% depth, 1.5 RR, 2.5% structural risk, 1% price deviation and fresh native quote/spread gates. Use KRX correct tick rounding. V3 geometry intentionally returns SIGNAL (not READY); the subsequent implementation now adapts it to the practice-only V2 enforcement path and rechecks the actual order price. Current V2 diagnostic ceiling is not authorization. For genuine ask above cap, no fake executable ask; optional passive LIMIT policy requires explicit quote verification, no upward chase, TTL/cancel/partial fill/reconcile contract, final guard and integration tests. No quantity increase on repricing. Evaluate separately by PULLBACK/MOMENTUM; keep BREAKOUT owner path.
- Latency: do not parallelize actual submitters; observer read-only, priority polling and single writer. Existing DB timeout settings retained; no empty-list fail-open on unknown orders. Actual 180s broker deadline distinct from watchdog. Capture stage latencies before setting targets; 60s HOT decision and 5–15s signal-to-submit are design objectives, not achieved.
- All requested items including item 09 mandatory. Draft remains blocked until every item has code or verified existing implementation and evidence. Preserve Final30, existing exits, allocation and KR_INFINITE policy. No automatic merge or operation.

## Test and release gates
1. Pin current base, audit diff and verify existing code paths, failing-before/passing-after tests per defect. Reconstructed fixtures must be labeled, not asserted as raw replay.
2. Run new `tests/kr/test_kr_pb1_minute_20261125_adaptive_entry.py`, existing #395/#396 and owner/claim/SST suites; compile and full regression. CI no KIS order/network calls. Check CI results at latest head. Add missing tests for passive cancel, source conflicts, terminal retries, owner isolation, TP/SL, historical recovery and latency.
3. Replay original KIS data with causal point-in-time quotes where available, at least 20 trading days and out-of-sample where possible; compare opportunity, actual executable fill model (not candle-only assumed), costs, forward MAE/MFE, realized return, MDD. Do not optimize for order count alone. No proof of Pumtech profit.
4. Ops evidence: WSL SHA, profile, observer, AM/PM guard, close/report mail receipt; native KIS 10/27 and 11/19 + 11/25 evidence. No live mutation from PR CI. Approved practice rollout only after tests pass; no force fill, no automatic merge. Report unverified actual operating trade separately.
5. Draft implementation at creation includes V2 label, early diagnostic ceiling, standalone V3 candidate geometry, reconstructed test fixtures. Remaining code changes and all tests/CI/runtime verification are not complete. Keep Draft.


## 2026-11-25 implementation update (supersedes initial draft statuses)

| ID | Latest disposition | Remaining verification |
|---|---|---|
| 01 | Implemented narrow same-day pre-hashkey zero-fill retry under same semantic unit and atomic new attempt | Fresh native all-pages no-order + forced balance and actual retry; no ambiguous or partial retry bypass |
| 02 | Existing quarantine retained; prevent incomplete-at-request minute from maturing during I/O | Original native conflict/repair timestamps; CI |
| 03 | Explicit stale-minute anchor added | Actual HOT freshness p95 |
| 04 | Authenticated native retry can prove data while preserving failed breakout trigger | CI and native ETF session data |
| 05 | Fixed exact legacy local client-key alias stored in broker_order_id on explicit reject; native ID still blocks | Actual historical reconciliation and scoped DB readback |
| 06 | Read-only diagnosed: 10/27 order 0000011280, 174 shares, accepted and RECONCILED; original BUY fill row absent, separate imported cycle later sold 174 | Original-day native KIS execution proof needed; do not join unrelated cycle or force terminalization. Existing scoped recovery code not sufficient evidence of recovery |
| 07 | Existing SST contract preserved | Latest CI and actual BUY-to-position/restart |
| 08 | Same-unit zero-fill retry added; existing partial-fill/remaining action policy untouched | Latest partial fill/cancel/retry integration |
| 09 | Existing TP/SL/final close unchanged | Latest regression and actual lifecycle |
| 10 | V3 rise-and-hold integrated in PB1 practice ENFORCE adapter | Latest CI; original-minute replay and real quote proof |
| 11 | Causal local peak/trough integrated | Latest CI and out-of-sample replay |
| 12 | Diagnostic cap first; bounded genuine fresh ASK repricing integrated, quantity invariant; no fake passive ASK | Latest CI and full submit integration |
| 13 | Existing observer priority unchanged; 60s loop target not independently proven | Further decision-latency implementation and observed timing |
| 14 | Incomplete-minute and stale-repair changes; no relaxation of existing deadline | Stage profiling, actual tick duration; additional work if targets missed |
| 15 | V2 early error label fixed, V3 revision and cap logged | Latest CI / emitted ledger |
| 16 | Not completed | Independent heartbeat and actual mail delivery verification |
| 17–20 | Existing ranking/owners/exit thresholds/duplicate fences preserved; new offline counterexamples included | Latest full regression |
| 21 | No live trade or DB mutation performed | End-to-end regression + practice execution evidence |
| 22 | Practice WSL profile selects V3 inside existing V2 ENFORCE; explicit flag 0 rollback | Not merged or deployed; latest CI and operating evidence required |

The original draft head c72bf4df passed 10 workflows. Subsequent code commits invalidate that as final acceptance. Latest-head workflows must pass; no claim of 22/22 completion. The Pumtech fixtures reconstruct logged closes/lows/volumes and are not complete native KIS replay. No original KIS/WSL access, no account mutations, no forced trades. Keep Draft and do not merge until remaining mandatory work and evidence are resolved.
