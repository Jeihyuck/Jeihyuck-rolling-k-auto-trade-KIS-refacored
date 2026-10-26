# PR257 — KR reconciliation split and original-account recovery

**Branch:** `fix/kr-20261026-pr251-rebase-broker-truth-latency`.
**Base:** latest `dual-agent`. **Status:** Draft; not deployed.

## Current execution paths

1. Pre-SELL **exact KIS order + DB + position proof** => offer SELL first.
2. If exact daily history is incomplete but the **KIS fresh holding / no open order / no unresolved claim / owner cycle and portfolio epoch** proof is valid, offer SELL first in **SELL-only mode**. That never authorizes new BUY.
3. After SELL pass: if **zero attempted/blocked SELLs, exit pass OK, second complete KIS order proof, fresh new KIS balance and account reset OFF**, the second proof can skip duplicate full reconciliation and refresh the shared broker balance for PB1 and KR_INFINITE. Every failed check uses the original post-exit reconcile path.
4. If a SELL was attempted, full reconciliation and a fresh post-SELL balance are required before ANY new BUY. If not verified, protected SELL receipts remain durable and BUY is blocked. Unknown/partial ACK stays fenced.

An independent reconciliation worker is **implemented as an off-hours operator-authorized entrypoint only**. It is NOT scheduled, enabled, auto-launched, or allowed to run while market jobs are active. In particular, it does not provide speculative pretrade writer concurrency.

## Operator-only original account evidence audit

```bash
python -m scripts.kr257_historical_kis_evidence_probe --start 2026-10-13 --end-exclusive 2026-10-27
```

Only after loading the correct **original** practice account credentials. If the supplied KIS credentials belong to the 10/25 new account, prior-epoch rows are listed as `ORIGINAL_ACCOUNT_OR_EPOCH_NOT_AUTHORIZED`. No DB updates. KIS BUY 012750 (Oct23 11 shares, only 2 DB fill) and all 14 SELL price-unresolved records are scanned. Broker `tot_ccld_qty`, `tot_ccld_amt` and `avg_prvs` must be mutually consistent before any human-reviewed financial repair.

## Independent off-hours reconciliation entrypoint

```bash
# Must be operator approved; DO NOT run while the market is open
export KR257_OFFHOURS_RECONCILE_APPROVED=1
python -m scripts.kr257_offhours_reconcile_worker --execute
```

Requires practice env + VT KIS endpoint + original matching current account/epoch, off hours (outside weekday 05:30–16:30), and a Postgres per-account/epoch advisory lock. It calls the existing reconciliation writer and makes no trade order. **No Windows task change.** It is deliberately not auto-activated on the user workstation.

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
- Full authoritative intraday worker with concurrent cutover needs durable single-writer lease shared by trading ticks, exhaustive crash and restart validation. This PR intentionally **does not activate concurrent DB writers** to avoid uncertain duplicate economic mutations.
- Shadow remains for comparison, but -3/-6 paper PB1 SWING order policy itself is active at code level, not shadow. Real-account routing is excluded.

## Rollback

Keep PR in Draft until review. Disable practice policy via `KR_PB1_PAPER_STOP36_ENABLED=0` if a live practice anomaly is observed. The pre/post reconcile optimization is guarded by `KR_PROTECTIVE_SELL_RECONCILE_HANDOFF=0`; that restores original FULL pre-exit reconciliation (with latency cost). Order proof failure never authorizes BUY. No scheduler or source changes are required to roll back a code deployment; revert the PR if needed.
