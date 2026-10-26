# PR257 — KR reconciliation split and original-account recovery

**Branch:** `fix/kr-20261026-pr251-rebase-broker-truth-latency`.
**Base:** latest `dual-agent`. **Status:** Draft; not deployed.

## Current execution paths — 2026-10-26 P1 review closure

1. **Before PB1 protective SELL:** try the narrow, read-only KIS fresh
   balance + PB1/KR_INFINITE no-pending-intent/claims + strategy owner +
   active account/epoch holdings-parity proof. No dated full KIS order history
   is queried in this normal SELL-first path. If it fails, retain the original
   conservative exact/full pre-SELL reconcile fallback.
2. Evaluate PB1 protective SELL first with its existing submit-time claim,
   idempotency, quantity and broker checks. The -3%/-6% approved **practice
   only** strategy policy is unchanged.
3. **Before any new KR_INFINITE or PB1 BUY:** if the owner exit pass was
   complete and recorded **zero submitted/attempted/blocked/accepted/filled
   SELLs**, collect a fresh KIS post-exit balance and a complete dated
   KIS+DB cumulative quantity/notional/account-scoped exact proof.
4. Only for a confirmed narrow PRE proof (or exact PRE proof), a successful
   POST exact proof, unchanged complete KIS account holdings quantities
   **including KR_INFINITE 122630**, no account reset and zero SELL activity,
   the original redundant FULL historic reconciliation may be skipped.
   It is **not** authorized by the narrow preflight alone.
5. If any proof fails, any SELL is attempted or blocked, the exit pass fails
   or a balance changes, perform FULL post-SELL reconciliation and (for
   attempted SELL) refresh KIS balance. Unknown ACK and pending orders remain
   fenced; no BUY proceeds on unresolved economic evidence.

The exact/full proof branch remains the fallback, not a silently optimistic
authorization. **A P1 review identified that the previous branch could not
skip FULL reconciliation after successful SELL-only preflight; this has been
fixed and is checked by an offline regression which executes the real runner
callback AST, not a separately reimplemented branch.** The additional
zero-SELL pre-check avoids performing exact KIS history followed by FULL
history on attempted/blocked SELL ticks.

**Independent reconciliation worker is hard-disabled.** No manual `--execute`
or approval setting can make it run until a shared Tick/worker single-writer
lease, clock-boundary and crash/restart tests have been implemented. The
separate original-account historical audit is read-only and remains available.

## Operator-only original account evidence audit

```bash
python -m scripts.kr257_historical_kis_evidence_probe --start 2026-10-13 --end-exclusive 2026-10-27
```

Only after loading the correct **original** practice account credentials. If the supplied KIS credentials belong to the 10/25 new account, prior-epoch rows are listed as `ORIGINAL_ACCOUNT_OR_EPOCH_NOT_AUTHORIZED`. No DB updates. KIS BUY 012750 (Oct23 11 shares, only 2 DB fill) and all 14 SELL price-unresolved records are scanned. Broker `tot_ccld_qty`, `tot_ccld_amt` and `avg_prvs` must be mutually consistent before any human-reviewed financial repair.

## Independent reconciliation worker disabled (P1 race guard)

`python -m scripts.kr257_offhours_reconcile_worker --execute` always returns
`BLOCKED` and non-zero status, including before 05:30, after 16:30 and on
weekends. Even the explicit approval environment variable does not enable it.
No KIS request, PostgreSQL connection, economic mutation or order is made.
No scheduler is added or modified.

**Future activation requires** account+trading-epoch single-writer lease shared
by *both* Tick and worker; an off-hours clock recheck at every DB write;
timeout boundary and restart/lease recovery tests. Until then use only the
existing original-account read-only KIS evidence audit above.

## Operational p95 measurement

```bash
python -m scripts.kr257_native_http_latency_report path/to/nullim-kr.log
```

Native KIS HTTP boundary: `[KR_LATENCY][NATIVE_HTTP_START]`.
All attempted ticks including timeouts: `[KR_LATENCY][TICK_END]`.
Requires at least **20 actual sample ticks and 20 actual SELL HTTP events**, otherwise reports `INSUFFICIENT_SAMPLES`; no fake performance PASS. The first 20 samples should be from the same deployed PR SHA, account/epoch and workload.

## Not completed by code alone

- Original-account KIS evidence is unavailable via the current new practice account; never invent historic fills or prices.
- Live KIS SELL submission and measured p95 need actual practice runs after merge. CI and synthetic tests cannot prove them.
- Full authoritative intraday or off-hours worker requires durable single-writer lease SHARED by trading ticks, plus clock-boundary/crash/restart validation. The PR257 CLI is hard-disabled, even when operator-approved. It cannot run concurrently with trading because it cannot run at all.
- Shadow remains for comparison, but -3/-6 paper PB1 SWING order policy itself is active at code level, not shadow. Real-account routing is excluded.

## Rollback

Keep PR in Draft until review. Disable practice policy via `KR_PB1_PAPER_STOP36_ENABLED=0` if a live practice anomaly is observed. The pre/post reconcile optimization is guarded by `KR_PROTECTIVE_SELL_RECONCILE_HANDOFF=0`; that restores original FULL pre-exit reconciliation (with latency cost). Order proof failure never authorizes BUY. No scheduler or source changes are required to roll back a code deployment; revert the PR if needed.
