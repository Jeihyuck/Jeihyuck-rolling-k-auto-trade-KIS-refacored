# KR 2026-10-26 new practice-account go-live checklist

> Scope: new Korean **practice** account funded with 100,000,000 KRW.
> This page does NOT authorize real-account trading or execute any orders.

## 0. What the database proved on 2026-10-25

- A trading epoch with reason `NEW_KIS_PRACTICE_ACCOUNT_20261025` is ACTIVE under the new practice account.
- The former practice account has a separate ACTIVE trading epoch and five old OPEN positions (84 shares), all scoped to the **old** account/epoch.
- No active `pb1_pullback_close` portfolio child currently exists under the NEW account. **That is expected**: `_ensure_active_epoch` creates its child on the first economic transaction. No manual portfolio reset or insertion is required.
- Read-only DB evidence does **not** prove the current WSL `.env` selects the new account, or that KIS currently reports 100m cash. Both require the owner's local KIS configuration.

Do NOT delete the former account's epoch, positions, orders, claims, or history.

## 1. Merge timing and deployed code

- Merge this readiness PR into `dual-agent` **before** KR PREP on Mon 2026-10-26 (normally 06:30 KST); this PR intentionally stays Draft until reviewed. The assistant did not merge.
- WSL deploy-preflight auto-syncs `dual-agent` on the first KR session of a day, pins its SHA to `runtime/code-pins/KR-2026-10-26.sha`, and requires a clean local checkout. Do not edit Windows Task Scheduler or suppress the pin.
- A PR merged **after the day's pin** cannot automatically replace the pinned code during AM/PM; don't force a git reset to evade the guard.

## 2. Before market open — local no-order check

Use the existing WSL project checkout and its **new** KIS practice credentials. Do not share the `.env`, CANO, token, app key, or full account number with the PR.

```bash
cd ~/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored
set -a; source .env; set +a
export STRATEGY_ENV=practice KIS_ENV=practice
export NULLIM_TRADE_DATE=2026-10-26
export PAPER_MAX_CAPITAL_KRW=100000000 EXPECTED_PRACTICE_CAPITAL_KRW=100000000
export KIS_HTTP_ENABLED=1 PYTHONPATH=.
python scripts/kr/check_20261026_new_practice_account.py --verify-kis
```

This performs **only** database reads and a KIS balance read. It does not reset state, create an epoch, place a trade, or migrate schema.

Expected:
- `KR_NEW_ACCOUNT_EPOCH_AND_100M_OK`
- `AUTHENTIC_PRACTICE_100M_CASH_NO_HOLDINGS`
- Cash within 1m KRW of 100m; zero holdings before initial trade

If `KR_NEW_ACCOUNT_EPOCH_PROVENANCE_MISMATCH` occurs, WSL likely points to old practice account credentials. Stop and correct `.env` offline; do not create another epoch or blindly override DB keys.

If KIS returns missing token/auth/balance or zero cash, **do not bypass** the readiness gate, fabricate a holding or disable HTTP safety. Correct the new KIS app-key/CANO-product mapping and retry a read-only balance check.

## 3. Runtime gates and positions

- `deploy-preflight.sh` already performs a required ACTIVE epoch lookup. On **2026-10-26**, it additionally demands the *new account*'s Oct25 generation, 100m KR settings, and no foreign active portfolio child. Any mismatch blocks order-capable KR sessions.
- KR WSL wrappers load `.env` again; this PR reasserts `PAPER_MAX_CAPITAL_KRW` and `EXPECTED_PRACTICE_CAPITAL_KRW` as 100m in each KR practice wrapper after the final reload.
- GitHub KR PREP/AM/AFTERNOON/CLOSE budgets are 100m; US budgets, margin/risk sizing and strategy stop policies are unchanged.
- First PB1 portfolio epoch will be created automatically and only for the new account's active trading epoch. Old portfolios remain historical; do not update old scope into the new account.
- At PREP verify `best_k_meta`, `pb1_candidate_pool` and final30 canonical output for trade date 2026-10-26 (do not recycle 10/23 artifacts). Monitor KR rate-limit `EGW00201`.

## 4. Preexisting execution risk

This readiness PR does not fix the Oct23 ~130s duplicate historical reconciliation or certify 60s SELL / 180s full-tick p95. KR PR #251 remains a separate Draft remediation and has not been merged.

Do not claim 'tomorrow all trades will work' based only on account/epoch/capital CI. Verify the first AM session logs for:
- actual checkout SHA and account/epoch gate;
- broker-readonly balance and correct cash budget;
- canonical PREP final30 for 10/26;
- `KIS native HTTP` submits only when strategies authorize them;
- claim/fill/order/position all attributed to the **new** account and epoch;
- Tick timeout, `RECONCILE_LANES` proof failures, and broker circuit breaker evidence.

Never force a BUY/SELL to make the test pass; never bypass active claims or settlement fences.

## 5. Rollback and no-go

If gate or broker auth fails, keep orders blocked and restore previously approved KR practice configuration only after isolating correct account identity. Do not point WSL at the old account as a shortcut. Do not delete/recreate epochs, reset portfolio DB, overwrite secrets in GitHub, or change Windows scheduler times.

Successful CI = readiness **code** verified. Successful local KIS check = **account evidence** verified. Real next-day PREP/order/reconciliation performance = **operational** verification, not asserted in advance.
