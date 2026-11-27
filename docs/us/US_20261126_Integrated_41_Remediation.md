# PR400 — 41-item remediation ledger (2026-11-27 V2 supersession)

**The operator-approved FINAL V2 overrides previous HOT5/WARM5, HOT-only polling, and US independent 5+3 production entry instructions. Historical notes below describe prior revisions, not current policy. Existing SELL/SST/recovery/portfolio requirements and TP 90s remain.**
Base 851fe3b9100287ea73ca39a7be690f34f5a05643. User explicitly approved TP decision maximum age 90 seconds instead of 30. User checklist [x] denotes requested scope, not completed validation. Practice only. No deployment, merge, live orders or DB mutations.
## Scope and acceptance
IMPLEMENTED = source changed, CI/runtime not yet certified. OPEN = required outstanding. REVIEW = strategy research, no activation without approval.
| ID | Priority | Task and acceptance | Status |
|---|---|---|---|
|01|P0|V2 bounded valid-manifest HOT/WARM/COLD 4:1:1; no invalid-manifest bulk fallback|CODE updated; actual API quota/latency unverified|
|02|P0|US HOT4, WARM8 publisher/reader limits|CODE updated|
|03|P0|WARM official minutes, bounded COLD, HOT promotion backfill|PARTIAL tier schedule and HOT backfill; dynamic promotion open|
|04|P0|Separate PREP COLLECTION_PRIORITY and short-lived execution shortlist; no erroneous bulk fallback|PARTIAL PREP contract/day scoped manifest and fallback added; independent dynamic refresh open|
|05|P0|US 30 completed opening bars remain mandatory; independent 5+3 research only; preserve KR #401|CODE US fallback removed and negative tests; KR regression pending|
|06|P0|Verify overseas KIS continuation, bounded targeted historical backfill|PARTIAL authenticated KIS page API, monotone KEYB, max four HOT pages, no synthetic bars; native read unverified|
|07|P0|Latest completed bar gap/stale recovery and deadline priority|PARTIAL elapsed opening and recent four minute target recovery added; deadline scheduling open|
|08|P0|Conflict quarantine and three distinct authenticated native observations >=60s apart, >=120s total, preserve history|IMPLEMENTED durable conflict quarantine, 3 distinct HTTP responses >=60s apart and >=120s span; synthetic tests added|
|09|P0|Freeze completed-minute eligibility at request start|IMPLEMENTED observer path|
|10|P0|Independent full-session observer, AM/PM continuity, no overlap|PARTIAL day-scoped singleton and pre-open subscription path; actual 09:25 launcher and WSL proof open|
|11|P0|SPY QQQ SMH independent verified data recovery; retain crash gate|PARTIAL benchmark prewarm and per-quote age fix; independent refresh/recovery of previous close open|
|12|P1|Per-symbol poll/age/gap/conflict/API budget diagnostics|PARTIAL opening intelligence and shared REST transport logs; full telemetry open|
|13|P1|HOT final decision <=60s and quote range event P95 <=5s|OPEN independent rank refresh and bounded consumer scheduling|
|14|P1|US 5+3 and KR Adaptive V3 economic replay, no unauthorized policy activation|REVIEW|
|15|P1|Final30 eligible candidate replenishment without relaxing scores|REVIEW|
|16|P1|Defensive eligible candidate replenishment; no risk bypass|REVIEW|
|17|P0|AMGN data-failure recovery, fresh final risk evaluation; actual crash block expected|OPEN|
|18|P0|BUY->intent->claim->KIS->fill->SST->position regression and practice proof|OPEN|
|19|P0|Immutable original BUY owner/account/epochs/cycle/contract hash across restart|OPEN|
|20|P0|Partial sell preserves original lineage, cumulative fills and residual qty|OPEN|
|21|P0|SELL decision evidence linked to actual original BUY and selected owner|OPEN|
|22|P0|V2/V3/legacy original frozen policy and HWM preserved|OPEN|
|23|P0|DHR 16 soft-stop blocks: prove old action status; restore protective order liveness without duplicate|OPEN DB root confirmed: prior Nov20 one-share UNCERTAIN active attempt, BROKER_SUBMIT_RESULT_UNKNOWN, RESERVED; original-day KIS and balance proof required|
|24|P0|KO TP2 68 blocks: prove previous attempt terminal/unsent, same stage remaining qty|OPEN DB root confirmed: Nov25 TP2 PENDING intent, no durable attempt/journal; Nov11 previously repaired; Nov25 KIS proof required|
|25|P0|Single-writer candidate queue, canonical same-symbol arbitration, per-symbol fresh decision->reservation->native POST immediately, do not wait for fills; TP decision max age 90s|PARTIAL 90s and selected-symbol fresh decision at serial router turn with identity/qty/contract checks; initial arbitration list remains advisory; measured runtime open|
|26|P0|Exact reservation identity and lease valid through HTTP; diagnostic root error|OPEN reservation release after abort confirmed for 5 records; exact pre-HTTP authority failure not resolved|
|27|P0|Stage4 writer authority stable at HTTP; no bypass|OPEN Stage4 replay/NO_MATCH interaction identified; writer race/lease proof outstanding|
|28|P0|Proven pre-HTTP abort journal + claim/attempt/intent/TP/reservation atomic terminal release; uncertain POST remains fenced|PARTIAL exact unsent certificate + same-attempt local NO_MATCH allowed; no broker order/fill, no active claim, exact original/forward scope; atomic intent+TP repair; DB runtime proof open|
|29|P0|Fresh executable CME/U quote path and bounded retries|OPEN|
|30|P0|Partial fill/cancel/terminal proof before residual retry; no overfill/duplicate|OPEN|
|31|P0|TMO original scoped KIS order identity|OPEN|
|32|P0|12 TP fence records exact native proof, no blind repair|PARTIAL five exact pre-HTTP-abort records diagnosed (ABBV/CME/ICE/PG/U); others require scoped KIS proof; no production rows changed|
|33|P0|Historical recovery at AM/CLOSE only; unrelated current orders not globally fenced; same lifecycle unknown remains blocked|OPEN|
|34|P0|Stage4 health, writer and reconciliation liveness|OPEN|
|35|P0|#390 approved signed ACTIVE portfolio policy HWM and execution proof; no unauthorized activation|OPEN|
|36|P0|Verified equity/cash/flows and valuation; UNKNOWN not fabricated|OPEN|
|37|P1|Swing V3 U case causal replay; keep existing policy|REVIEW|
|38|P0|CLOSE five independent axes truthful, no fabricated PASS|OPEN|
|39|P0|Tick latency, duplicate reads, SELL priority, one submitter|PARTIAL shared single-flight core reused from #401 for canonical US quotes; shared priority budget and measured latency open|
|40|P0|Latest HEAD all CI, exact WSL SHA/config, real practice lifecycle evidence|OPEN|
|41|P0|Exchange calendar holiday and early close scheduler, observer and order cutoff|PARTIAL observer holiday/early-close cutoff and AM/PM session deadline clamped; scheduler and native order boundary WSL acceptance open|
## Nonnegotiable
Preserve PREP ranking, owner isolation, -3/-6 stops, TP thresholds, TQQQ Infinite, account/cycle/duplicate fences, verified executable quote and market crash checks. 90s is TP decision freshness only; do not change separate quote freshness, BUY signal TTL, worker leases, or all order timeouts. Re-evaluate actual owner policy and quantity after expiry; no timestamp rewriting. Verify runtime env US_TP_PRE_HTTP_MAX_DECISION_AGE_SEC=90 or no override. Default rollback explicit value 30. Separate unknown order from proven no POST, do not infer from missing order number alone. Protect SELL budget and no concurrent native submitter. KR references #367 #395 #396 #398; US #390 #397. Use native US KIS endpoint spec, no KR timestamp parameter copy.
## Required tests / release gates
Offline: valid-manifest HOT4/WARM8/COLD 4:1:1; stale/invalid/empty never bulk poll; PREP manifest scope and preopen expiry; request-start completion; missing/conflict; US incomplete opening blocks even with 5+3; KR 5+3 preserved; session switch/early close; 4 simultaneous TP candidates fresh per-symbol; 90s inclusive 91s reject plus live quote threshold and stale/future/unknown timestamps; DHR/KO prior action exact proof; PRE-HTTP atomicity and ambiguous POST crash/race; partial/cancel/retry and owner/qty; restart SST; signed portfolio policy and close axes. All latest-head CI. Do not declare tests passed before observed results. Practice acceptance after deploy only: WSL SHA/config, actual observer age/calls, qualifying BUY/SELL and fills, all close axes. No forced trades. Thanksgiving/early close means actual later trading evidence not available in this change. Historical unresolved records report separately. Draft until required code and offline acceptance complete; no automatic merge.


## 11/26 follow-up implementation and evidence
- Commits b2c4b85e/44daf3c7: independent 5+3 and synthetic regressions; no claim that this increases returns.
- a9ddd21c/fbd54ecd: just-in-time selected TP revalidation; four-symbol serial decision/route regression. No timestamp rewriting or fresh-quote override.
- 2fd9af46/5ce9dd1b: exact pre-HTTP proof repair. Read-only active-epoch DB inspection: ABBV, CME, ICE, PG, U have PENDING TP/intent, authoritative ABANDONED_PRE_SUBMIT attempts, OPEN claims without active attempt, RELEASED reservations, no order/fill, exact PRE_IO abort plus local replay NO_MATCH. PG has verified forward management attribution to its original V2 stage lifecycle. Code distinguishes these from genuine broker ambiguity. No production repair executed.
- DHR Nov20 has a one-share active UNCERTAIN attempt, unresolved broker result, reserved shares and no proven no-POST certificate; old stop fence cannot be cleared safely from DB absence. KO Nov25 TP2 has pending intent but no durable attempt/journal; its old Nov11 row was separately repaired. Both remain blocked pending actual original-day KIS order/fill pagination and balance parity.
- 3385c36f/ea760d2c/0df874a8/a5580801: KIS native pagination, bounded HOT historical repair and independent conflict observations with regression cases.
- 31fbbfe2/07bc6da0/036073f2: day-scoped observer continuity, epoch check, process lock, holiday and early close; synthetic singleton/calendar tests. HOT TTL fail-closed still applies; no claim of uninterrupted minute polling when shortlist invalid.
- Previous head a5580801: US Historical TP Original-Day Proof offline contract job success, native KIS read-only job SKIPPED; US Four Strategy Contract CI success. These are not latest-head acceptance.
- All current tests must finish on final HEAD; no merge or deployment. Remaining P0s include independent priority refresh/latency, index recovery, original BUY/SELL chain acceptance, DHR/KO/TMO native proof, reservation/Stage4 liveness, partial retry and historical recovery boundaries, signed portfolio guard, close axes and actual WSL execution.


## FINAL V2 follow-up, 2026-11-27
Latest base #401 5aa3c047 integrated as merge commit 90e9a065 (no overlapping changed paths at integration). Four previous failing workflows shared one regression test; each reported 4485 passed, 1 failed, 2 skipped. Same-batch conflicting minute remains quarantined; a single unattested reread now correctly does not publish an authoritative row. No test weakening of quarantine. Later full CI still required.

| V2 ID | Required scope | Actual implementation / remaining |
|---|---|---|
|V2-01|PREP COLLECTION_PRIORITY distinct from EXECUTION_PRIORITY|PARTIAL: US PREP publish after completed DB run, original rank and exact Final30/contract hash, account+epoch+day, validity to exchange close; valid empty execution shortlist remains empty; KR PREP integration and dynamic ranking/verified quantity/holdings evidence outstanding.|
|V2-02|Open collection and pre-open warmup|PARTIAL: US session-independent observer and early subscription, HOT4/WARM8 4:1:1, invalid scope empty; actual 09:25 launcher/first bar SLA not verified.|
|V2-03|Opening range intelligence and provisional price|PARTIAL: shared read-only analysis and US observer logging, native completed bar/turnover and conflict checks, explicit support/target/cap/tick formula. Actual strategy-owned provisional price adapter and KR hook outstanding; do not infer executable price.|
|V2-04|Dynamic promotion, backfill, hysteresis|PARTIAL: elapsed opening and recent four missing minutes, bounded HOT repair; independent dynamic promotion, hysteresis and deadline admission outstanding.|
|V2-05|Shared data layer|PARTIAL: extracted #401 REST single-flight core for KR and US canonical prices; original receipt retained, no error cache, protective/native proof bypass; benchmark prewarm and per-quote age fix. Shared WS day ownership, shared priority API budget, full market recovery and operational metrics outstanding.|
|V2-06|Bounded event-driven final re-evaluation|OPEN: no parallel order authority or direct observer order.|
|V2-07|Strategy preservation|PARTIAL: US mandatory 30 restored; KR #401 merged base retained; latest KR/US regression pending.|
|V2-08|Operational data evidence|PARTIAL: opening analysis and REST logs; unified structured telemetry and report wiring outstanding.|
|V2-09|Causal replay and opportunity loss|OPEN: 20 trading days target, actual native evidence and economic comparison missing.|
|V2-10|Activation/rollback and performance|OPEN: explicit US REST sharing rollback US_REST_SHARED_QUOTE_ENABLED=0; all feature controls, real practice/latency, WSL and CI outstanding.|

New commits: a4ced7ab policy+regression, 57004491 PREP manifest, d481ba99 opening intelligence and gaps, efefc510 shared REST, ec3ff112 benchmarks and protective quote isolation. Existing DHR/KO/TMO, exact historical proofs, reservation/Stage4, full SST and partial retry, portfolio and CLOSE gates remain mandatory. No broker order, operating DB mutation, WSL deployment, or merge into dual-agent. Draft until all required source work and current-head offline acceptance complete. Operational acceptance and any practice activation must be separately evidenced.
