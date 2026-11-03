# US Stage4 full evidence and release gate — 2026-11-03

**Status: code candidate / operating proof NOT YET VERIFIED.** This runbook is
**not** an instruction to submit, cancel, re-submit, or test a live broker order.
Keep the stacked implementation PRs **Draft** until the exact merged candidate
has both GitHub CI and actual WSL/KIS-practice evidence.

## Scope and sequence

- A #295: preserve original KIS cumulative fill digest before mutating DB save;
  preserve historical claim evidence with lazy balance.
- B #296: final 30-second Stage4 READY recheck after persisting SUBMITTED and
  before the native KIS HTTP boundary.
- C #297: historical vs current-day claim-origin diagnostics, without relaxing
  original fatal close totals.
- D #300: fair stop-debt rotation, cheap DB precheck, original UTC/ET/KST events.
- E #302: separate read-only original-date KIS evidence acquisition; single
  existing Stage4 advisory-lock owner alone may apply DB changes and publish READY.
- F #304: original broker 6 AM + 2 Afternoon orders versus pre-HTTP blocked
  attempts; missing strategy 5W1H remains PENDING_EVIDENCE.
- **Integrated CI review only** #305; never merge this gate PR itself.

## A. Before a WSL session: code / account identity

Use the **canonical WSL repository**, with the exact approved base/merged SHA.
Do not silently merge, reset, or deploy an unreviewed Draft. No source changes
while an active trading session owns the writer.

```bash
cd ~/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored
git branch --show-current
git rev-parse HEAD
git status --short
date -Is
TZ=America/New_York date -Is
powershell.exe -NoProfile -Command 'Get-ScheduledTask -TaskName "PB1 US*" | Select-Object TaskName,State'
```

Confirm `KIS_ENV=practice`, current epoch and account scope, existing Stage4
`WORKER`/atomic outbox/account reservation/stop ledger safety flags as normally
authorized by the scheduler. Never force `US_STOP_RECOVERY_AUTHORIZED=1` or
`US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED=1` to make TMO disappear.

## B. During an already running approved US practice session

Use its actual `session_run_id` and trade date, **not** a guessed run ID:

```bash
PYTHONPATH=. .venv/bin/python scripts/us_stage4_operational_readiness.py \
  --trade-date YYYY-MM-DD --session-run-id ACTUAL_SESSION_RUN_ID
```

It does read-only DB health, active claim counts, local session report and
code pin; it does not submit or cancel an order. A fresh READY heartbeat alone
does not prove performance or reconcile TMO.

## C. After actual US practice runs: Stage4 latency proof

Inspect log names and supply the **actual new candidate** Stage4 worker logs
from AM and Afternoon, keeping the two sessions separate:

```bash
find runtime/logs -type f -iname '*stage4*' | tail -20
PYTHONPATH=. .venv/bin/python scripts/us_stage4_latency_acceptance.py \
  --stage4-only ACTUAL_AM_STAGE4_WORKER_LOG
PYTHONPATH=. .venv/bin/python scripts/us_stage4_latency_acceptance.py \
  --stage4-only ACTUAL_AFTERNOON_STAGE4_WORKER_LOG
```

**PASS only when every required session has at least 20 READY intervals,
p95 <= 20,000 ms, p99 < 30,000 ms, zero >=30,000 ms gaps, zero writer-stale
/ startup-timeout / reconcile-failure events, and no fabricated or reused
generation evidence.** Report p50/p95/p99/max and sample count. This test
does not infer actual KIS ACK or fill latency. If no after-patch logs exist,
the proper result is `NOT_PROVEN`, not a made-up improvement.

## D. TMO 2026-10-30 original-date GET-only proof

Use existing authorized WSL KIS-practice credentials. This command is **read
only**: historical KIS GET and scoped Supabase SELECT, no order or cancellation.

```bash
PYTHONPATH=. .venv/bin/python scripts/us_kis_original_day_evidence.py \
  --from-date 2026-10-30 --to-date 2026-10-30
```

Verify that TMO **SELL one share** is present and exactly linked to the
original order number `0000036019`, source date, account/epoch, owner
`US_STANDARD`, key and attempt. A returned OPEN / 0 filled / 1 outstanding
is **not** a terminal or cancel proof. Do not manually set FILLED, CANCELLED,
EXPIRED or release `IN_FLIGHT` from the DB. If authentic KIS evidence does
prove terminality, let the normal writer reconcile it; then repeat KIS and
scoped DB reads to verify order/claim/ledger consistency. If KIS results
are unreachable, ambiguous, partially paginated, or mismatch, keep the
fence and report **NOT VERIFIED**.

Useful read-only DB check (run through the existing authenticated Supabase
query interface; avoid printing account IDs or raw journal payload):

```sql
SELECT o.trade_date,o.symbol,o.side,o.order_no,o.status,
       o.qty_requested,o.qty_filled,
       c.action_state,c.cumulative_filled_qty,c.remaining_target_qty
FROM public.us_orders o
LEFT JOIN public.us_execution_claims c
 ON c.action_key=o.meta->>'execution_action_key'
AND c.env=o.env AND c.trading_epoch_id=o.trading_epoch_id
AND c.trade_date=o.trade_date
WHERE o.trade_date='2026-10-30' AND o.symbol='TMO'
  AND o.side='SELL' AND o.order_no='0000036019';
```

## E. Historical source and strategy audit

Check exact original `trading_epoch_id`, `account_id`, `action_key`,
`position_lifecycle_id`, `strategy_owner`, broker original order and
KIS cumulative status before settling any stop obligation. The new
read-only reader must not release a claim or make a DB write.

2026-11-02 archived original order/journal/fill evidence was **6 AM + 2
Afternoon US_STANDARD broker fills**, plus **2 separate pre-HTTP aborted
attempts**. Eight broker fills were reconciled by order identity and qty,
but eight 5W1H strategy decision records were MISSING. Thus **broker fill
CONFIRMED does not mean strategy EXPECTED**. For each order, re-evaluate
frozen strategy settings, historical inputs, risk gate, claim, broker
submit/ACK and fill; classify only when evidenced as
`EXPECTED`, `IMPLEMENTATION BUG` or `POLICY REVIEW`.

## F. Failures / release acceptance

- Required GitHub Actions CI on **exact candidate SHA**: no failures;
  skipped native KIS sample gate is **not** live verification.
- Restart/kill-before-HTTP, write crash, partial pagination, stale reader,
  account/epoch rollover and concurrent claim changes: offline tests pass.
  Do **not** artificially kill a running market writer merely for testing.
- WSL Windows scheduler → startup preflight → one Stage4 writer → KIS-practice
  GET/ACK → Supabase → complete close report, with original account scope.
- Existing protective SELL owners, stop thresholds, TQQQ Infinite sleeve
  rules, and 30-second fail-close must remain unchanged.
- If any actual operating evidence is absent, state `NOT_PROVEN` and keep
  the trading/recovery gate protected. Never merge CI-only PR #305.
