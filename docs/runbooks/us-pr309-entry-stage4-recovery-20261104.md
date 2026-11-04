# US 2026-11-03 execution incident — PR #309 acceptance and KIS-safe recovery

**Account:** KIS PRACTICE only. **Owner:** US_STANDARD except separately noted TQQQ. **Base:** dual-agent, event 2026-11-03 ET.  
**Never merge on CI alone.** Read-only KIS evidence must be obtained from the original account/epoch before claims, stops, or orders are marked terminal. This document authorizes **no** broker orders, cancellations, Supabase writes, scheduler changes, or trade-policy relaxation.

## Error ownership and decisions

| Incident | Evidence | Classification | PR behavior |
|---|---|---|---|
| `US_ENTRY_TIMING_SIGNAL_EXPIRED` | 6 US_STANDARD pre-HTTP BUY attempts, broker_submit=0, observed 124–149s age | IMPLEMENTATION BUG: tick-start time was treated as signal observation; POLICY REVIEW: 180s is user-selected trial threshold | New 180s proof and signed max-age check; per-symbol wall-clock price assessment in approved live practice; original 120s signed proof stays 120s |
| `US_BUY_ENTRY_EXIT_CONTRACT_PERSISTENCE_MISMATCH` | 6 DB save errors and same symbol/day key across independent Tick decisions | IMPLEMENTATION BUG | Reevaluate only after all same-symbol/day/epoch attempts are verified `ABANDONED_PRE_SUBMIT`, no `us_orders` row. Mint fresh key and distinct semantic Claim instance for new economic target; revalidate at router; no overwrite |
| `PREFLIGHT_ROUTER_MISMATCH` | 6 incidents resulted from `intent_persistence_rejected`; changed_fields may be empty | IMPLEMENTATION BUG: misleading root classification | Report `BUY_INTENT_PERSISTENCE_REJECTED` separately; keep entry degraded/fail closed |
| `US_STAGE4_BROKER_RECOVERY_NOT_VERIFIED` | 42 worker cycle errors, often historical TMO replay; incomplete broker replay proof | IMPLEMENTATION BUG **only if** recovery path loses available KIS original evidence; unresolved broker status itself remains a legitimate safety fence | Structured replay status/failure counters logged; never grant READY after ERROR |
| `HISTORICAL_CLAIM_SET_CHANGED`, `HISTORICAL_READONLY_UNAVAILABLE`, `HISTORICAL_BROKER_EVIDENCE_STALE` | 4/2/2 Stage4 failures | IMPLEMENTATION BUG for avoidable stale polling; true scope/broker failures must remain fenced | Prioritize missing/stale/changed original-date read-only book over less-urgent balance polling. Keep 15s evidence TTL and fail-closed validation |
| `US_STAGE4_WRITER_FENCED_BEFORE_HTTP` | 2 legitimate KIS pre-submit blocks (SYY initial SELL; AJG BUY) | EXPECTED safety action, degraded worker dependency requires recovery | No disabled order fence; re-evaluate worker readiness with original account/generation |
| TMO 2026-10-30 OPEN TP1 SELL | Broker order 0000036019, requested=1, filled=0, still an active historical Claim as at 11/03 close | PENDING_KIS_EVIDENCE, not safe to auto-settle | **No automatic cancellation, claim reset, new SELL, or inferred fill**. Operator must use original KIS authenticated same-account original-day order and execution inquiries |
| SYY 2026-11-03 protective hard-stop and Close `FAILED_RECONCILE` | Actual KIS SELL 41 filled, `us_protective_stop_obligations` still PENDING in 11/03 DB snapshot, close integrity failed and 5W1H order-chain audit incomplete | IMPLEMENTATION BUG candidate pending exact lifecycle/claim matching | **No automatic obligation satisfaction** without exact order/epoch/qty/fill match. Separate stop-obligation repair and audit reconciliation required; Close failures are not waived |

## Required offline / read-only validation

1. Verify deployed `US_PINNED_CODE_SHA` exactly matches PR HEAD after a controlled merge; current PR is **Draft**.
2. Original KIS 2026-10-30 TMO order 0000036019: verify trading epoch, account/product, trade-date bridge, symbol, side, original requested quantity, canonical order number, cumulative filled quantity, broker remaining quantity, terminal status and timestamp. `OPEN` or uncertain is **not** terminal proof.
3. 2026-11-03 SYY 0000038487: independently match all 41 KIS SELL fills, account scope, claimed hard-stop lifecycle, frozen exit contract, fill quantity and original 5W1H provenance. Do not equate a mere holdings delta with execution proof.
4. Re-run deterministic 120/180/181s signed proof cases and direct router canonical risk, WS freshness <=5s, price cap, reversal invalidation, TQQQ/VCP isolation, ordinary NEW BUY, and ADD inherited exit contract.
5. Run 2+ same-symbol consecutive Tick retries: first `ABANDONED_PRE_SUBMIT` with original `us_order_intents` still present, then a changed quantity + new READY signal. Require new key/Claim and at most **one** actual KIS BUY. A missing/uncertain/ACKED/filled prior attempt must prohibit retry.
6. Run 20+ distinct Stage4 READY intervals from fresh verified original date KIS books, current fills and ACK, including claim-set change, stale cache, read-only source failure and worker restart. Reject READY if account, epoch, generation, original broker facts or 15s freshness differ.
7. Complete Windows Task Scheduler → WSL session → PREP → preflight → AM/afternoon → Close → report/email with KIS original order and Supabase fill/Claim consistency, including an independent reconciliation of TMO and SYY debts.
8. Confirm **no** new BUY/SELL policy alterations to `SWING_BOOK/SWING_CARRY/US_SWING_DEFAULT`, no TQQQ Infinite inheritance, no cross-day key reuse, and no changes to real KIS default authorization.

## Completion grading

- **CODE PASS:** exact HEAD syntax/unit/PostgreSQL/US+KR CI all SUCCESS; SKIPPED native KIS proof is *not* PASS.
- **OPERATION PASS:** original WSL + KIS practice source proof, broker ACK/fill and Close reconciliation accepted; TMO uncertainty separately identified and fenced if still unresolved.
- **MERGE:** only after review of safety impact; no automatic merge/deploy from this PR.

Historical raw logs and read-only DB evidence do not prove broker terminality as of the next session. Any future operational signoff must include a new timestamped evidence bundle.


## PR309 SYY evidence-driven stop-only reconciliation amendment (2026-11-04)

**Verified read-only Supabase source facts (NOT a new KIS GET):**
- Original SYY 2026-11-03 SELL order `0000038487`: requested=41, filled=41, local status FILLED, owner US_STANDARD, exact lifecycle and account epoch.
- KIS cumulative-actual `us_fills`: 41 SELL, matching client_order_key, original order_no, immutable Claim key, lifecycle, epoch, provenance `KIS_ORDER_CUMULATIVE_ACTUAL`.
- Actual SELL Claim: SATISFIED, 41 filled/0 remaining; first pre-submit aborted Claim: OPEN/0 fill/41 target (NOT a broker order).
- Durable `us_protective_stop_obligations` row remained PENDING/0 confirmed; legacy terminal settlement incorrectly required a TP1/TP2/TP3 original order even for a fully executed stop-only SELL.

**Code remediation:** Exact matched `US_STANDARD` full-stop orders now count as eligible original broker sources alongside original TP (never substitute for TP in a different lifecycle). Both the independent HistoricalReadOnlyReader source-day scan and Stage4 terminal settlement use the same strictly scoped existence proof:
- original account+epoch+symbol+side+strategy owner+lifecycle+execution action key;
- satisfied claim and fully filled order (41/41), plus non-synthetic `KIS_ORDER_CUMULATIVE_ACTUAL` / `KIS_EXECUTION_ACTUAL` fill linked by original order number and client_order_key;
- then **fresh authenticated original-day KIS order** terminal quantity/status with zero broker-open shares, current KIS-authoritative zero holding balance and no active/uncertain related Claim;
- the writer alone updates the obligation state, broker terminal receipt and `confirmed_filled_qty` from **verified full-stop executions**. No synthetic fill. Old OPEN claim is not manually rewritten, SYY protection is not bypassed.
- A KIS GET unavailable/OPEN/quantity mismatch/other epoch yields BLOCK and preserves the debt. This is a code-level **IMPLEMENTATION BUG** fix; it is **not** proof that the existing SYY row was changed.

**TMO is a separate genuine broker state:** historical `0000036019` is confirmed original KIS OPEN/0 filled/1 remaining in 11/03 archived reads. OPEN is not a terminal state; no patch may auto-cancel, zero the remaining quantity, release IN_FLIGHT or use a DB-only fabricated terminal order. Existing read-only host CLI:
```bash
PYTHONPATH=. .venv/bin/python scripts/us_kis_original_day_evidence.py \
  --from-date 2026-10-30 --to-date 2026-10-30
```
An OPEN result is an **EXPECTED fence**. Repeated 500/timeouts on KIS GET are separate `BROKER_EVIDENCE_UNAVAILABLE` incidents; the writer must fail-closed until actual evidence is available. Existing native read-only utility may not change TMO itself.

**PR merge blockers:** all exact HEAD CI green; the native KIS gate remains SKIPPED until WSL practice session, KIS original-day proof, Stage4 20 READY interval acceptance and Close. Verify SYY obligation transitions from PENDING/0 to SATISFIED/41 *only after fresh KIS proof*. If the external broker still shows TMO OPEN, preserve it as unresolved with a transparent audit status and no duplicate SELL; do not falsify CLOSE clean if other errors remain.
