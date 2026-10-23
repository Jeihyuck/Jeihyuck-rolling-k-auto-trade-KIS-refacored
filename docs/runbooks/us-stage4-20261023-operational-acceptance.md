# US Stage4 operational acceptance (2026-10-23, KIS practice only)

## Truth and scope

This guide does not constitute a PASS. PR #250/252/253 are already merged; PR
#254 fixes native HTTP and UUID fencing. All code changes are separate from
**actual** authenticated WSL/KIS operating evidence. Do not force a trading
session or broker cancellation to manufacture a pass.

Never clear stale `OPEN` / `IN_FLIGHT` execution claims merely because a
local TP status is old. Never re-POST an ambiguous KIS order. Distinct strategy
owners (especially `TQQQ_INFINITE` vs `US_STANDARD`) remain isolated.

## Gates (all must be evidenced)

| Gate | PASS criterion | Failure |
|---|---|---|
| A: deployment | WSL canonical `dual-agent` HEAD equals dated US code pin and reviewed PR SHA | block |
| B: configuration | KIS practice, authenticated US stage profile, account reservations/outbox ON, Stage4 `WORKER`, explicit writer + stop-ledger authorization | block |
| C: schema | migrations 0056-0059, RLS and canonical account/epoch validated with read-only preflight | block |
| D: writer lifecycle | precisely one account-scoped advisory lock, live worker log, DB READY heartbeat + fresh session report, matching UUID generation | block |
| E: stale broker orders | original trade-date KIS GET proves the *actual* old TP status, qty and terminality; local `OPEN` or cancel ACK does not suffice | keep fence |
| F: stop lifecycle | genuine PB1 US_STANDARD fresh/persisted 3%/6% stop obligation, subsequent protected SELL only after nonduplicate broker authority; ACK, fills and DB match | block |
| G: speed | >=20 **distinct selected US_STANDARD hard/soft stop order keys**, native HTTP boundary p95 <=10s, and >=20 distinct session ticks p95 <=60s | NOT_PROVEN |
| H: cleanup and rollback | session exits stop worker; verified open claims are never dropped; manual operator can disable AM without altering active KIS orders | block |

Stages A-D establish **writer readiness only**, not E-H.

## Read-only checks — no KIS orders, no scheduler changes

Do not run during a live session if you intend to modify code or reset the
working tree. Use the existing canonical WSL repository and a real session
`session_run_id` from its log:

```bash
cd ~/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored
git branch --show-current
git rev-parse HEAD
git status --short
# Existing wrapper sets the session env, profile, code pin and DB context.
# In the same already-authorized runtime environment, use the actual values:
python scripts/us_stage4_operational_readiness.py \
  --trade-date 2026-10-23 \
  --session-run-id 'REPLACE_WITH_ACTUAL_SESSION_RUN_ID'
# The checker only performs DB SELECT, local code-pin/report reads and git
# inspection. Exit 43 means BLOCK, not a reason to relax safety gates.
```

Independent latency proof from **actual after-patch** session logs:

```bash
python scripts/us_stage4_latency_acceptance.py \
  runtime/logs/us-am-2026-10-23.log \
  runtime/logs/us-afternoon-2026-10-23.log
```

Supply the **actual existing** WSL log filenames (check `ls runtime/logs/`).
Exit 2 means fewer than 20 unambiguous distinct protective order identities,
too few comparable ticks, or target missed. A missing file is an error, not
a zero-duration observation. Pre-HTTP aborts, KIS result-unknown, and TQQQ
are not counted as successful PB1 protective HTTP submissions.

Worker-specific diagnostics (new after this PR):
`runtime/logs/us-stage4-worker-<trade-date>-<session_run_id>.log`.
The file is created mode 0600 by the session runner for authorized WORKER
mode. Do not upload raw credentials or logs publicly; redact account and
credential data before sharing.

## Activation is explicit, not an automatic profile default

A Stage4 session additionally needs the following values within its normal
WSL environment, **not** an ad hoc unaudited child process:

```text
KIS_ENV=practice
US_STAGE123_PAPER_PROFILE=ON
US_RECONCILE_WORKER_MODE=WORKER
US_STAGE4_WRITER_AUTHORIZED=1
US_PROTECTIVE_STOP_LEDGER_MODE=ENFORCE
US_ACCOUNT_RESERVATION_MODE=ENFORCE
US_ATOMIC_CLAIM_OUTBOX_ENABLED=1
US_SHARED_KIS_LIMITER_ENABLED=1
```

The existing permission preflight must pass. Do **not** independently set
`US_STOP_RECOVERY_AUTHORIZED=1` or
`US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED=1` to force stale TP release.
The Stage4 read-only original-date TP inquiry is available even with both
cancel gates OFF; actual cancellation is a separate explicitly authorized
action. Existing account, market, owner, lifecycle, quantity and broker
terminal gates still apply.

## Last blocking evidence as of 2026-10-23

Read-only Supabase checks returned: zero rows in
`us_stage4_reconcile_health`, zero rows in
`us_protective_stop_obligations` and `us_stop_policy_revisions`; historical
US execution claims include six `IN_FLIGHT` and 48 `OPEN` rows.
**Zero stop debt rows is not proof of stop settlement**, and 48 local OPEN
claims are not evidence that 48 KIS orders are still open. These statuses
can change after actual operation. Verify by read-only SQL and original-date
KIS evidence before declaring recovery.

## Blocking categories

- **IMPLEMENTATION BUG**: stage worker crashes, wrong generation, pre-HTTP
  fence bypass, fabricated KIS HTTP marker or state transition contradicting
  repository policy — repair + CI before use.
- **EXPECTED**: no broker-submission when writer stale, historical TP not
  independently terminal, no old debt but no fresh stop signal, or protected
  hard/soft stop withheld by an existing KIS duplicate-SELL fence.
- **POLICY REVIEW**: native callback, implementation and broker evidence match
  the current strategy contract, but the contract's behavior or performance
  target requires operator review. Never conflate this with a code defect.

**No WSL/KIS operational run was performed by the GitHub PR or CI.
No user-visible production completion may be claimed based on mocks.**
