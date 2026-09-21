# Unified KR/US Trading Epoch Reset

This runbook replaces the PR137 fresh-database cutover design.

## Contract

- Keep the existing PostgreSQL database.
- Keep all historical rows.
- Do not copy, truncate, or replace the database.
- End the prior account generation and create one new ACTIVE `trading_epoch_id`.
- New KR/US orders, fills, positions, risk lifecycle, and Infinite state belong to the new epoch.
- Legacy rows stay queryable but are excluded from active-epoch trading/PnL reads.
- Strategy policy is unchanged. This is an execution/accounting boundary only.

## Hard stop conditions

Do not create or activate a new trading epoch unless all of the following are true:

1. Windows Task Scheduler trading tasks are disabled.
2. No KR or US trading wrapper is running.
3. The KIS account has no KR holdings, no US holdings, and no pending orders.
4. You intend to resume on the **next trading session**, not later in the same market trade date.

The last rule matters because some legacy client-order identities are deterministic by
trade date. The epoch boundary isolates DB reads/writes, but PR138 intentionally does
not rewrite strategy order-key semantics.

### Same-trade-date restart is intentionally unsupported

This PR does **not** claim that a new epoch is independently tradable later in the same
market trade date. In particular, US `us_orders.client_order_key` and
`us_order_intents.client_order_key` retain their existing global uniqueness contract.
If a deterministic key from the earlier epoch is generated again on the same trade date,
the new code must reject the collision rather than adopt, overwrite, or relabel the old
row into the active epoch.

Therefore:

- cross-epoch contamination is fail-closed;
- same-trade-date key reuse is not made tradable by this PR;
- do not change the UNIQUE constraints or append the epoch to strategy order keys as part
  of this reset change;
- resume automated practice trading on the next market trading date only.

Supporting same-trade-date restart would be a separate policy/schema project because it
changes durable order identity semantics and idempotency constraints.

## 1. Pull merged code

```bash
cd /home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored || exit 1
git fetch origin
git checkout dual-agent
git pull --ff-only origin dual-agent
git status --short
```

Expected: clean working tree on `dual-agent`.

## 2. Load the existing DB/KIS environment

The DB URL does **not** change.

```bash
set -a
source .env
set +a

export STRATEGY_ENV=practice
export KIS_ENV=practice
```

For real trading, use the real environment/account deliberately. Never reuse a
practice confirmation for a real account.

## 3. Broker-flat contract

The operator confirmation is necessary but **not sufficient**. The epoch start command
queries KIS directly before any database migration or epoch mutation and fails closed
unless all broker evidence is authoritative and flat.

Required broker evidence:

- KR balance is authoritative and contains no holdings;
- KR same-day order inquiry is paginated to completion and has no remaining quantity;
- US balance parse status is OK;
- US balance is complete and authoritative;
- practice: NASD, NYSE, and AMEX were all queried successfully;
- real: NASD (KIS real-account US-wide balance contract) was queried successfully;
- US holdings are zero;
- US same-day order inquiry is paginated to completion;
- malformed/quarantined US quantity evidence is rejected;
- no US order has remaining quantity.

If any balance exchange, pagination cursor, quantity, or order-status evidence is
incomplete, the command aborts **before** DB migrations or epoch state changes.

## 4. Start the new epoch

```bash
export TRADING_EPOCH_CONFIRM=YES
export TRADING_EPOCH_BROKER_FLAT_CONFIRMED=YES
export TRADING_EPOCH_REASON="PRACTICE_RESET_20260922"

python scripts/start_new_trading_epoch.py
```

Expected:

```text
"status": "TRADING_EPOCH_STARTED"
"broker_flat_evidence": {"status": "BROKER_FLAT_VERIFIED", ...}
"history_deleted": false
"database_replaced": false
```

The command verifies broker flatness first. Only after that succeeds does it run DB
migrations, end the prior top-level epoch, end prior ACTIVE KR portfolio epochs, close
their DB-only OPEN position rows, and create one new ACTIVE `trading_epoch_id`.

## 5. Verify the active epoch

```bash
python scripts/verify_active_trading_epoch.py
```

Expected:

```text
"status": "ACTIVE_TRADING_EPOCH_OK"
```

If this fails, do not enable trading.

## 6. Run no-order preflight

Use the next trading date.

```bash
NULLIM_TRADE_DATE=2026-09-22 NULLIM_PREFLIGHT_ONLY=1 bash scripts/wsl/run-kr-prep.sh
NULLIM_TRADE_DATE=2026-09-22 NULLIM_PREFLIGHT_ONLY=1 bash scripts/wsl/run-us-prep.sh
```

Canonical WSL preflight now fails closed with
`active_trading_epoch_missing` if the account has no ACTIVE top-level epoch.

## 7. Re-enable scheduler only for the next session

Do not resume same-trade-date order production after an epoch reset.

Windows Task Scheduler remains the sole automatic scheduler owner. WSL cron/systemd
must not independently launch KR/US trading jobs.

## 8. First-session validation

For the first KR and US sessions after reset, verify:

- new order rows carry the current `trading_epoch_id`;
- fills inherit the same epoch;
- positions/snapshots are read only from the active epoch;
- KR Infinite starts from the new epoch state;
- TQQQ Infinite starts from the new epoch state;
- PnL/report DB fallbacks include only active-epoch rows;
- BUY contract -> fill -> position -> SELL policy lineage remains the PR135 contract.

If `POLICY_MISSING`, a cross-epoch order identity, or an epoch-less new trading row
appears, stop order production and investigate before continuing.
