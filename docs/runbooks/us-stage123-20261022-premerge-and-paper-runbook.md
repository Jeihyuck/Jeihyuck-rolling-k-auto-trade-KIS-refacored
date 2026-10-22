# US Stage 1–2–3: pre-merge evidence and practice cutover (2026-10-22)

## 2026-10-22 PAPER default-ON change (supersedes previous default-OFF instructions)

Canonical `scripts/wsl/run-us-am.sh` and `run-us-afternoon.sh` now
source `scripts/wsl/us-stage123-paper-defaults.sh` **before the session**.
For KIS `practice`, `DRY_RUN=0`, `US_PAPER_TRADING_ENABLED=1`,
TRADE mode, non-offline, non-signal-only sessions, the supported
Stage 1/2/3 profile is **ON by default**:

```text
US_STAGE123_PAPER_PROFILE=ON
US_ATOMIC_CLAIM_OUTBOX_ENABLED=1
US_ACCOUNT_RESERVATION_MODE=ENFORCE
US_RECONCILE_WORKER_MODE=RESERVATION_ONLY
US_SHARED_KIS_LIMITER_ENABLED=1
US_EXIT_QUOTE_PREFETCH_ENABLED=1
US_EARLY_HARD_STOP_PILOT_ENABLED=1
US_EARLY_MULTI_HARD_STOP_ENABLED=1
US_EARLY_SOFT_STOP_PILOT_ENABLED=1
```

This is **real KIS PRACTICE order-capable** execution, not dry-run. The
unsupported full-authority `WORKER` is **NOT** turned on; it is still
not implemented. Protection for remaining SELL types continues through
the pre-existing canonical priority merge. Individual explicit OFF
overrides while profile ON fail **before starting trading**, never silently
fall back to an inconsistent mixed mode.

The account coordinator migration `0056` was installed on the connected
Supabase project (2026-10-22) and two new public-schema tables were verified
RLS-enabled, with all anon/authenticated table privileges revoked. This does
not prove the actual WSL broker account/epoch matches that project; the
non-mutating `scripts/us_stage123_paper_preflight.py` checks real WSL
DB access, active epoch and schema on every AM/Afternoon start, and blocks
orders if any prerequisite is absent.

Emergency reversible rollout **without reverting the entire code PR**:
set `US_STAGE123_PAPER_PROFILE=OFF` for BOTH AM/Afternoon scheduler tasks,
and ensure no individual Stage123 feature flag remains explicitly ON in
the inherited environment. This restores the legacy behavior for subsequent
sessions. Never force-release unresolved Claims or reservations.

`US_STAGE123_PAPER_PROFILE=ON` for live KIS, DRY_RUN, offline or signal-only
is rejected. Real KIS live account restrictions are unchanged.

---


## 0. Meaning of "ready"

This document is **not** a declaration that the full dual-writer architecture has
migrated. There are three independent gates:

- **OFFLINE_CODE_PASS** — additive Stage 1 Claim/outbox/account hold, limited
  protected SELL hot path, reservation-only worker, historical fill/cancel,
  cross-epoch concurrent reservation proof, and regressions pass **at the same SHA**.
- **PAPER_READY** — authenticated KIS PRACTICE, preflight checks, valid account
  scope, database migrations, one Windows Task Scheduler, no unresolved
  original-order/reconcile exceptions, and a controlled rollback plan.
- **ARCHITECTURE_COMPLETE** — full protected SELL critical path and *all*
  broker/order/Fill/Claim authoritative reconciliation writes transferred to
  a single fenced process with native restart/late-fill evidence; signal→HTTP
  p95 <= 10s (N>=20 distinct selected protective orders) and whole Tick p95
  <= 60s (N>=20 completed ticks). This is **NOT yet satisfied**.

Keep production trading **OFF** while preparing tonight's PAPER validation.
KIS practice accepts actual simulated orders; these are not offline tests.

## 1. Merge topology (do not apply individually without rebase)

- #245 Stage 1 base dual-agent
- #246 Stage 2 base #245 head branch
- #247 Stage 3 base #246 head branch, had GitHub mergeable=false
- **#248** integrated, based on dual-agent; it contains all three stages'
  critical files including `order_router.py`, `account_authority.py`,
  `reservation_reconcile_worker.py`, `us_exit_engine.py`, and `repos.py`.

If reviewers approve merging *feature-disabled infrastructure ahead of paper*,
merge **#248 only**; don't independently merge #245–247 afterward, since their
changes are already included and may conflict or double-apply. Verify no newer
dual-agent changes conflict with the integration head. A passing test does not
authorize automatic merging or automatic KIS orders.

## 2. Non-trading WSL preflight (safe before tonight)

From the **canonical WSL repo**, after reviewers merge #248 and you run
`git fetch origin && git checkout dual-agent && git pull --ff-only`:

```bash
set -euo pipefail
pwd
git rev-parse --abbrev-ref HEAD
git rev-parse HEAD
git status --short
env KIS_ENV=practice DRY_RUN=1 ALLOW_REAL_ORDER=0 \
    US_ACCOUNT_RESERVATION_MODE=OFF \
    US_ATOMIC_CLAIM_OUTBOX_ENABLED=0 \
    US_EARLY_HARD_STOP_PILOT_ENABLED=0 \
    US_EARLY_MULTI_HARD_STOP_ENABLED=0 \
    US_EARLY_SOFT_STOP_PILOT_ENABLED=0 \
    US_EXIT_QUOTE_PREFETCH_ENABLED=0 \
    US_SHARED_KIS_LIMITER_ENABLED=0 \
    python -m pytest -q tests/us/test_us_20261021_claim_late_fill.py \
    tests/us/test_us_20261021_tqqq_ttl_quantity_contract.py \
    tests/us/test_us_20261021_reconcile_shadow_worker.py \
    tests/us/test_us_20261022_account_authority.py \
    tests/us/test_us_20261022_exit_quote_prefetch.py \
    tests/us/test_us_20261022_shared_kis_limiter.py \
    tests/us/test_us_postgres_integration_real.py
bash scripts/wsl/check-runner-parity.sh
NULLIM_PREFLIGHT_ONLY=1 bash scripts/wsl/run-us-am.sh
NULLIM_PREFLIGHT_ONLY=1 bash scripts/wsl/run-us-afternoon.sh
```

The disposable PostgreSQL tests need `PBCORE_TEST_POSTGRES_URL` pointing
to **a disposable private test database**; NEVER point them at Supabase
production. Without it, PostgreSQL integration tests SKIP: do not count SKIP
as proof. Never run a real session wrapper without PRE-FLIGHT-ONLY or
scheduler coordination.

## 3. Mandatory actual broker evidence before enabling practice orders

Check original KIS practice order/fill/cancel records on the exact broker
account for the 2026-10-20 TQQQ 3-share 0-fill CANCELLED original
`0000038776`. The operating DB still held an UNCERTAIN Claim on last
read-only observation. Do NOT alter, release, or resubmit it based on a
cancel request alone; ensure original-order fill/remaining qty are explicit
and that any late fills are reconciled.

At least one fully disjoint deterministic crash/restart, partial-fill,
cancel-zero-fill, and ACK-DB-failure replay test must pass in the
disposable DB. Check migration 0056 exists and RLS is enabled in an isolated
test DB; do not install it on the operational Supabase instance automatically.

Before running KIS PRACTICE, confirm Windows Task Scheduler is the only
active automatic US scheduler, `dual-agent` at the verified merge SHA,
`KIS_ENV=practice`, the correct non-secret account scope and order permissions,
and no alternate Cron/GitHub Actions order worker. A failed gate means stop,
not enabling more flags.

## 4. Feature rollout sequence — no implied activation

Canonical WSL practice TRADE sessions default Stage123 features ON. Use a practice-only controlled session and
a human-reviewed configuration change. Start with legacy reconciliation and
disabled account enforcement; verify baseline real KIS **practice** submit
and original-order ledger. Do not enable account reservations before
migration 0056 and session-owned reservation-only worker startup/healthy
scope-bound heartbeat exist. Enabling `US_ACCOUNT_RESERVATION_MODE=ENFORCE`
without `US_ATOMIC_CLAIM_OUTBOX_ENABLED=1` AND a healthy
`US_RECONCILE_WORKER_MODE=RESERVATION_ONLY` must fail closed, not fall
back to unreserved broker submissions. Do not set unsupported authoritative
`US_RECONCILE_WORKER_MODE=WORKER`: it is intentionally rejected.

Parallel native KIS quotes require both
`US_EXIT_QUOTE_PREFETCH_ENABLED=1` and
`US_SHARED_KIS_LIMITER_ENABLED=1`, with an isolated KIS client, bounded
deadline and owner-first safeguards. A quote timeout must never invent a
SELL signal or revoke an existing risk state. KEEP critical TQQQ Infinite
ownership independent from US_STANDARD; do not use Risk-Off alone to
condemn a TQQQ trade.

## 5. Runtime sign-off after tonight's practice orders

Collect raw ORIGINAL WSL AM/Afternoon session logs and invoke:

```bash
python scripts/us/check_stage123_runtime_latency.py \
  /path/to/actual-us-am.log /path/to/actual-us-afternoon.log
```

It strictly requires >=20 distinct selected protective SELL attempts and
>=20 completed Tick samples, no failing/timeout Ticks, signal→native HTTP
p95 <=10 s and Tick total p95 <=60 s. This file-only check does **NOT**
prove that the HTTP went to native KIS. Separately reconcile each attempted
order to KIS practice original ACK/fill/cancel and the matching
`us_order_events`, `us_orders`, `us_fills`, Claim, Attempt and account
reservation; never count duplicate events as independent samples.

If native evidence, sample counts, or critical lifecycle reconciliation are
incomplete, mark PAPER_ACCEPTANCE_PENDING. Rollback by disabling opt-in
flags and reverting the rollout via reviewed code change; **do not force
release UNCERTAIN claims**, and preserve raw source logs and state.

## 6. Known structural blocker before declaring full architecture done

`trade_tick_runner.py` still performs canonical order replay, ACK
reconciliation and Fill persistence; `RESERVATION_ONLY` is brokerless and
mutates reservation outcomes only. `WORKER` full authoritative
reconciliation is intentionally unsupported. Hence it is **not correct**
to claim independent complete broker/Claim/Fill writer cutover or guaranteed
protective SELL latency. Finish this structural transition in a separate
reviewed change, with dual-writer exclusion and realistic failure-injection,
before declaring ARCHITECTURE_COMPLETE.
