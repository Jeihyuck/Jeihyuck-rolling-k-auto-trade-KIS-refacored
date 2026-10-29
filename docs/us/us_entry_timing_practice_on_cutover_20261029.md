# US integrated #285 — PRACTICE ENFORCE cutover, pre-merge evidence

## Explicit user authorization and scope (2026-10-29)

The operator requested **actual ON rather than SHADOW** for the US opening
price-aware entry policy before the PR is merged. This is a **practice-account
policy trial**, not production authorization or a claim of improved PnL.

Both canonical scheduled WSL sessions `run-us-am.sh` and
`run-us-afternoon.sh` source
`scripts/wsl/us-entry-timing-paper-defaults.sh` *after* loading `.env`.
For an order-capable, non-offline KIS PRACTICE TRADE session the default flags
are:

```bash
US_ENTRY_OPPORTUNITY_MODE=ENFORCE
US_ENTRY_OPPORTUNITY_APPROVED=1
US_OPENING_OBSERVER_ENABLED=1
US_OPENING_OBSERVER_BAR_FETCH_ENABLED=1
US_WS_COLLECTOR_ENABLED=1
US_OPENING_OBSERVER_INTERVAL_SEC=10
```

Already-defined values are **not overwritten**: an explicit operator
`SHADOW`, `OFF`, `0` or disconnect remains authoritative and is reported
as `[US_ENTRY_TIMING][OVERRIDE]` rather than silently represented as ON.
If `.env` overrides any of these settings, inspect and change the override
before claiming an ENFORCE session. This helper neither changes the order
permission resolver nor touches the trading account, scheduler, database,
TQQQ Infinite/VCP owners, existing position ADD or protective SELL paths.
An observer process is read-only and forcibly disarmed from broker orders.

On a real KIS account, offline run, dry run, signal-only session or non-order
paper session this profile does **not** auto-enable the experimental policy.
The Python `effective_mode` still separately rejects non-practice ENFORCE.

## Implementation and policy separation

- The existing opening 09:30–10:00 ET BUY time block remains unchanged.
- ON applies only to `US_STANDARD` NEW BUY for the supported
  Pullback/Momentum/Breakout entry families; VCP and TQQQ remain isolated.
- The opening observer independently collects **native completed** KIS US
  1-minute OHLCV; a quote snapshot is not an OHLCV bar.
- No opening-bar coverage, missing WS bid/ask, stale/invalid signature, invalid
  price cap or unexpected trend reversal means **WAIT**, not an unconditional
  marketable order. Risk Gate, account Claims and KIS remain authoritative.
- The price/stop/target thresholds in #284 are experimental (POLICY REVIEW).
  Do not infer alpha, PnL improvement or safe production rollout from CI.
- This is a new entry policy ON only for practice; no changes to frozen
  `US_SWING_DEFAULT` exits, protection SELL latency rules, 30-second Stage4
  liveness threshold or cross-owner controls.

## Code checks, prior to merge

```bash
bash -n scripts/wsl/us-entry-timing-paper-defaults.sh \
  scripts/wsl/run-us-am.sh scripts/wsl/run-us-afternoon.sh
python -m pytest -q tests/us/test_us_entry_timing_paper_on_profile_20261029.py \
  tests/us/test_us_price_aware_opening_entry_20261029.py \
  tests/us/test_us_20261029_combined_decision_timing_latency.py
```

All current-HEAD GitHub US/KR/Postgres workflows must complete successfully.
A green *previous* SHA or queued workflow is not proof for the latest change.

## Runtime acceptance (not supplied by GitHub CI)

Before merging or relying on the ON policy for the next US session,
record an operator-run WSL/KIS practice evidence bundle for exactly the
same SHA/account epoch:

1. Actual AM and afternoon environment shows mode=ENFORCE, approved=1,
   observer=1, bars=1, ws=1, with no hidden override. Confirm true practice
   account in both KIS client and original response.
2. Independent observer starts **before/through** the opening BUY block.
   Inspect `runtime/logs/us-opening-observer-*.log`, the verified epoch-scoped
   SQLite bar store and 30/30 completed native 09:30–09:59 bars for each
   intended evaluated symbol. If incomplete, explicitly record WAIT.
3. Verify WS source, fresh `received_at`, bid/ask and max-entry price
   at the actual native BUY HTTP boundary; READY decisions and rejected
   orders must both be attributable to original signed evidence.
4. Preserve risk gates, idempotent Claims, exact KIS order ACK/cumulative fill,
   reconciliation and no duplicate order across restarts. Compare real original
   broker records with Supabase, not synthetic PnL.
5. Require >=20 measured Stage4 READY intervals with no stale gap and
   protected SELL timing, including Tick 180-second order-budget acceptance.
6. Reproduce/resolve the distinct 2026-10-28 US Close
   `FAILED_RECONCILE` incident with genuine original KIS evidence; no
   fabricated balance delta or mass claim reset.
7. Complete operator-level Windows Task Scheduler -> WSL deployed SHA ->
   PREP -> preflight -> AM -> afternoon -> Close -> report/email walkthrough,
   including pinned code SHA, DST and recovery.

**Release judgement:** CODE PASS and OPERATION PASS are separate. If a check
is missing, keep #285 Draft / MERGE HOLD; never represent pending evidence as
production readiness. Do not merge #285 and its overlapping source PRs together.
