# US 2026-11-26 integrated remediation (41 items)
Base 851fe3b9100287ea73ca39a7be690f34f5a05643. User explicitly approved TP decision maximum age 90 seconds instead of 30. User checklist [x] denotes requested scope, not completed validation. Practice only. No deployment, merge, live orders or DB mutations.
## Scope and acceptance
IMPLEMENTED = source changed, CI/runtime not yet certified. OPEN = required outstanding. REVIEW = strategy research, no activation without approval.
| ID | Priority | Task and acceptance | Status |
|---|---|---|---|
|01|P0|No Final30 native minute polling; COLD zero native REST|IMPLEMENTED observer path; audit all callers|
|02|P0|HOT maximum 5, enforce on publisher and reader|IMPLEMENTED|
|03|P0|WARM max 5 quote discovery only, backfill on HOT promotion|PARTIAL caps and no minute polling; promotion backfill open|
|04|P0|Expired/invalid/missing shortlist never polls Final30; independent refresh|PARTIAL no fallback polling; refresh open|
|05|P0|Independent verified post-opening 5+3 path for eligible pullback/reclaim; breakout retains opening high|OPEN|
|06|P0|Verify overseas KIS continuation, bounded targeted historical backfill|OPEN|
|07|P0|Latest completed bar gap/stale recovery, deadline priority|OPEN|
|08|P0|Conflict quarantine and three distinct authenticated native observations >=60s apart, >=120s total, preserve history|OPEN|
|09|P0|Freeze completed-minute eligibility at request start|IMPLEMENTED observer path|
|10|P0|Independent market-session observer; AM/PM continuity, no overlap|OPEN|
|11|P0|SPY QQQ SMH independent verified data recovery; retain crash gate|OPEN|
|12|P1|Per-symbol poll/age/gap/conflict/API budget diagnostics|OPEN|
|13|P1|HOT final decision <=60s measured objective, independent priority refresh|OPEN|
|14|P1|KR adaptive V3 separate causal replay and economics, no automatic policy activation|REVIEW|
|15|P1|Final30 eligible candidate replenishment without relaxing scores|REVIEW|
|16|P1|Defensive eligible candidate replenishment; no risk bypass|REVIEW|
|17|P0|AMGN data-failure recovery, fresh final risk evaluation; actual crash block expected|OPEN|
|18|P0|BUY->intent->claim->KIS->fill->SST->position regression and practice proof|OPEN|
|19|P0|Immutable original BUY owner/account/epochs/cycle/contract hash across restart|OPEN|
|20|P0|Partial sell preserves original lineage, cumulative fills and residual qty|OPEN|
|21|P0|SELL decision evidence linked to actual original BUY and selected owner|OPEN|
|22|P0|V2/V3/legacy original frozen policy and HWM preserved|OPEN|
|23|P0|DHR 16 soft-stop blocks: prove old action status; restore protective order liveness without duplicate|OPEN|
|24|P0|KO TP2 68 blocks: prove previous attempt terminal/unsent, same stage remaining qty|OPEN|
|25|P0|Single-writer candidate queue, canonical same-symbol arbitration, per-symbol fresh decision->reservation->native POST immediately, do not wait for fills; TP decision max age 90s|PARTIAL default 90s changed; serial just-in-time routing open|
|26|P0|Exact reservation identity and lease valid through HTTP; diagnostic root error|OPEN|
|27|P0|Stage4 writer authority stable at HTTP; no bypass|OPEN|
|28|P0|Proven pre-HTTP abort journal + claim/attempt/intent/TP/reservation atomic terminal release; uncertain POST remains fenced|OPEN|
|29|P0|Fresh executable CME/U quote path and bounded retries|OPEN|
|30|P0|Partial fill/cancel/terminal proof before residual retry; no overfill/duplicate|OPEN|
|31|P0|TMO original scoped KIS order identity|OPEN|
|32|P0|12 TP fence records exact native proof, no blind repair|OPEN|
|33|P0|Historical recovery at AM/CLOSE only; unrelated current orders not globally fenced; same lifecycle unknown remains blocked|OPEN|
|34|P0|Stage4 health, writer and reconciliation liveness|OPEN|
|35|P0|#390 approved signed ACTIVE portfolio policy HWM and execution proof; no unauthorized activation|OPEN|
|36|P0|Verified equity/cash/flows and valuation; UNKNOWN not fabricated|OPEN|
|37|P1|Swing V3 U case causal replay; keep existing policy|REVIEW|
|38|P0|CLOSE five independent axes truthful, no fabricated PASS|OPEN|
|39|P0|Tick latency and duplicate reads; SELL priority, single native submitter|OPEN|
|40|P0|Latest HEAD all CI, exact WSL SHA/config, real practice lifecycle evidence|OPEN|
|41|P0|Exchange calendar holiday and early close scheduler, observer and order cutoff|OPEN|
## Nonnegotiable
Preserve PREP ranking, owner isolation, -3/-6 stops, TP thresholds, TQQQ Infinite, account/cycle/duplicate fences, verified executable quote and market crash checks. 90s is TP decision freshness only; do not change separate quote freshness, BUY signal TTL, worker leases, or all order timeouts. Re-evaluate actual owner policy and quantity after expiry; no timestamp rewriting. Verify runtime env US_TP_PRE_HTTP_MAX_DECISION_AGE_SEC=90 or no override. Default rollback explicit value 30. Separate unknown order from proven no POST, do not infer from missing order number alone. Protect SELL budget and no concurrent native submitter. KR references #367 #395 #396 #398; US #390 #397. Use native US KIS endpoint spec, no KR timestamp parameter copy.
## Required tests / release gates
Offline: HOT-only calls with valid/stale/invalid/empty manifests, 5 cap and WARM/COLD zero; request-start completion; missing/conflict/5+3; session switch/early close; 4 simultaneous TP candidates fresh per-symbol; 90s inclusive 91s reject plus live quote threshold and stale/future/unknown timestamps; DHR/KO prior action exact proof; PRE-HTTP atomicity and ambiguous POST crash/race; partial/cancel/retry and owner/qty; restart SST; signed portfolio policy and close axes. All latest-head CI. Do not declare tests passed before observed results. Practice acceptance after deploy only: WSL SHA/config, actual observer age/calls, qualifying BUY/SELL and fills, all close axes. No forced trades. Thanksgiving/early close means actual later trading evidence not available in this change. Historical unresolved records report separately. Draft until required code and offline acceptance complete; no automatic merge.
