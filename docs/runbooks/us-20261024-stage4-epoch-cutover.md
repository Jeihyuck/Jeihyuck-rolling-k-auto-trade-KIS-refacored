# US 2026-10-23 incident repair / next practice account epoch cutover

This is a **code-only, no-order** follow-up. Creating a new KIS practice
account and trading epoch is **deferred to the operator's next-day window**.
The PR must not run migrations, update Supabase, change secrets, change Task
Scheduler, send a KIS order/cancel, or mark historical Claims terminal.

## Source evidence (2026-10-23 NY trade date)

- The 01:30 and 02:00 KST Afternoon sessions started the Stage4 WORKER child
  and then ended at first Tick with `stage4_worker_evidence_unavailable`.
  The Tick logged only `RuntimeError`, hiding the precise health fence.
- The supplied archive omitted the worker's dedicated
  `runtime/logs/us-stage4-worker-<trade-date>-<session-run-id>.log`.
  Its actual Python exception is **not known**. Do not claim the root cause
  is fixed until this log and PostgreSQL health are inspected.
- On the incident account, `us_stage4_reconcile_health` had 0 rows.
  Historical US Claims included 6 `IN_FLIGHT`, 48 `OPEN` and 1
  `RETRYABLE`. These are local states, **not** KIS terminality evidence.
- AAPL `US_STANDARD` TP1 SELL 1 filled. No TP or market-state policy
  change is authorized.
- Close `EXECUTION_CLAIM_CONFLICT_BLOCKED:348` and
  `UNRESOLVED_EXECUTION_ACTIONS:6` are intentionally fatal under the
  current reporting contract. Do not suppress or relabel them to get PASS.

## What this PR changes

1. In authorized Stage4 WORKER mode only, wait a bounded interval for
   **matching account+epoch+generation READY health** before the first Tick.
   Exit without a Tick or KIS order if the child dies or health stays
   missing/stale/unverified. No fallback to legacy reconciliation.
2. Preserve all existing Tick and native HTTP rechecks after initial READY.
   The startup check is not an unconditional session-wide order permit.
3. Log the precise fail-closed health code, rather than just `RuntimeError`.
4. Regression-test missing/stale/FAILED/wrong-generation health, worker
   crash, timeout, successful handoff and epoch-scoped Claim health.

Default startup wait: 75 seconds, bounded to 120 seconds via
`US_STAGE4_STARTUP_WAIT_SEC`. This wait is **outside** the Tick latency
sample; report it separately. A wait is not a remedy for a crashing worker.

## Before next-day epoch creation (operator only)

1. Keep US AM/Preflight/Afternoon scheduled tasks disabled and prove no
   KR/US WSL trade wrapper is alive. Never change the code pin mid-session.
2. Verify the **old account backup archive and checksum**. Do not delete
   historical orders/fills/Claims to make the new account appear clean.
3. Set the new account's `KIS_APP_KEY`, `KIS_APP_SECRET`, `CANO`,
   `ACNT_PRDT_CD` in the actual WSL secret environment. Do not paste
   secrets into GitHub, chat, logs or PR comments.
4. Confirm `KIS_ENV=practice` and `STRATEGY_ENV=practice`. Resolve the
   masked canonical account identity and verify it is the intended **new**
   account. A new KIS account does **not** itself create a DB Epoch.
5. Run the existing **read-only** `verify_broker_flat(env="practice")`
   against the new account: zero KR/US holdings and zero pending orders.
   Do not trust a mere operator boolean without this broker check.
6. Only then, with the scheduled tasks still disabled, run
   `scripts/start_new_trading_epoch.py` with its documented explicit
   confirmation environment and a unique reason. Do not call this from CI.
7. Run `scripts/verify_active_trading_epoch.py` for the new account.
   Record the new account's epoch ID, verify that previous account/epoch
   history remains unchanged and current Claim health is scoped to the new
   account+epoch. Existing `load_execution_claim_health()` uses
   `_us_execution_claim_scope()` and must **not** read the old 348
   Claim-conflict count.
8. Run next trading day's PREP and Preflight without orders. Stage4
   `US_RECONCILE_WORKER_MODE=WORKER` needs explicit authorization and
   `US_PROTECTIVE_STOP_LEDGER_MODE=ENFORCE`; never force these just to
   bypass a failed readiness check.
9. For a real practice session, inspect the **actual**
   `runtime/logs/us-stage4-worker-*.log` on WSL and the matching
   `us_stage4_reconcile_health` row. READY must match new
   account/epoch/generation and remain fresh. Broker GET evidence and
   POST/ACK/fill reconciliation are separate acceptance gates.
10. Do not resume scheduled trading until the next trade date, all safety
    gates pass, and the operator explicitly authorizes it.

## Acceptance / rollback

- Offline CI: Stage4 startup matrix and existing Stage4 pre-HTTP tests PASS.
- Operational: one writer, fresh READY, no old-epoch leakage, KIS
  read-only original-date terminal checks, correct protective SELL gates.
- Latency: at least 20 distinct US_STANDARD protective HTTP boundaries
  and 20 distinct full Ticks measured **after** the patch; no invented p95.
- Failure: keep tasks disabled, preserve the full worker log, health row,
  session run ID, epoch and masked account. No manual Claim clearing,
  no broker cancel and no forced rerun of an ambiguous order.

Classification: AAPL TP1 = EXPECTED; missing writer evidence blocks orders
as designed (EXPECTED safety gate), while the unready worker/session
handoff is an IMPLEMENTATION BUG candidate pending the actual worker log;
historical Claim terminality and close FAIL remain **NOT PROVEN** until
original-date broker evidence is available.
