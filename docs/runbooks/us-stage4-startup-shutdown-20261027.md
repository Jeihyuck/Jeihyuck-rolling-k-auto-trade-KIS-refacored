# US Stage4 writer lifecycle handshake (2026-10-27)

## Incident evidence (2026-10-26 U.S. AM; practice-only)

- 22:30:21.632 KST — \`trade_session_runner\` spawned the authoritative Stage4 writer.
- 22:30:30.381 KST — KIS practice fills query returned \`status=OK\`, count 0.
- 22:30:34.793 KST — trading Tick's authoritative KIS position reconciliation returned 0 positions.
- 22:30:36.494 KST — Stage4 persisted \`worker_state=READY\` with successful reconcile/replay.
- 22:30:36.613 KST — Tick failed \`stage4_worker_evidence_unavailable\`; no orders.
- 22:30:47 KST — session exited failed, while database Stage4 row remained \`READY\`.

The previous Tick only logged the exception type \`RuntimeError\`. This evidence
is consistent with a startup race; the exact failing health-code was not
recorded and cannot be established retrospectively. Do not label a specific
\`read_worker_health\` assertion as proved by these logs.

## Startup contract

In WORKER mode only, the session now waits **before creating the first
order-capable Tick** for \`read_worker_health\` to prove:

1. KIS practice account/environment, current trading epoch and canonical account.
2. Same expected writer generation UUID, with the worker process still alive.
3. \`worker_state=READY\`, fresh \`evidence_at\` (default maximum age 30 seconds).
4. Accepted authoritative reconcile and journal replay statuses.

The handshake retries only transitional \`NOT_READY\`, \`STALE\`, missing database
health, previous-generation evidence, unverified timestamp, and incomplete
reconcile status. It does not relax or bypass the existing Tick or pre-HTTP
gates. Defaults: \`US_STAGE4_STARTUP_TIMEOUT_SEC=75\` (bounded total wait),
\`US_STAGE4_STARTUP_POLL_SEC=0.5\`. Non-retryable failures, worker process exit,
and timeout produce \`FAILED: stage4_worker_startup_unavailable\` with **zero
order-capable Ticks**.

\`[US_STAGE4][STARTUP_READY]\`, \`[US_STAGE4][STARTUP_FAILED]\`, and the
Tick's \`[US_STAGE4][BLOCK_ALL_ORDERS] ... error_code=...\` record safe
diagnostic codes without printing KIS credentials or raw DB exceptions.

## Shutdown contract

The session first sends SIGTERM to the exact Stage4 worker it spawned. Its
interval loop checks \`_STOP\` at least every 250ms so normal graceful cleanup
can publish \`STOPPED\`. If a call is stuck, the session reaps the process
before an optional forced SIGKILL fallback.

**Only after OS-confirmed exit**, the parent conditionally updates the
existing Stage4 health row to \`STOPPED\`, clears \`evidence_at\`, and uses a
\`WHERE\` predicate requiring the same practice environment, account
fingerprint, trading epoch and writer generation UUID. It cannot overwrite a
newer writer's \`READY\` evidence. Updating writer liveness **does not assert
KIS order, cancellation or fill settlement**. Database-write failure is logged;
the row must not be treated as live without a fresh timestamp.

If the process cannot be reaped, the row is never certified \`STOPPED\` and a
durable session child-unverified fence is requested instead. The existing
Fail-Closed policy remains unchanged.

## Review and release gates

1. Review only \`dual-agent\` code. No changes to PB1, TQQQ Infinite, market
   regime, entry policy, protective stop, or trade sizing.
2. Run \`pytest -q tests/us/test_us_stage4_worker_lifecycle_20261027.py
   tests/us/test_us_stage4_native_pre_http_fencing_20261023.py
   tests/us/test_us_stage4_real_pg_and_handoff_20261023.py\` with no KIS order
   endpoint. The real-Postgres test is skipped unless its fixture URL is set.
3. Verify normal/late READY, stale and wrong-generation health, dead child,
   timeout, forced shutdown and stale-row reconciliation.
4. Before accepting in a practice session, check current \`dual-agent\` HEAD,
   deployed code pin, no active conflicting session, no unresolved KIS orders,
   owner-specific safety rules, and read-only broker evidence.
5. During a future authorized practice AM, verify \`STARTUP_READY\` precedes
   the first \`US_TICK_LOOP\` and that no new order passes the Stage4 gate when
   worker health becomes invalid.
6. Verify \`STOPPED\` or a clearly fenced/failed shutdown, not a misleading
   stale \`READY\`. Do not restart or merge while the U.S. session is running.

There is **no proof of a successful market-session replay** from unit tests or
the presence of a PR alone. Do not merge solely on a compilation/CI pass.
