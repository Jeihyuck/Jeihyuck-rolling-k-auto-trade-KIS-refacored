# PR #249 — KR 2026-10-21/22 offline verification, activation, rollback

Scope: `Jeihyuck/Jeihyuck-rolling-k-auto-trade-KIS-refacored`, base `dual-agent`, practice evidence. This is an execution runbook, **not** authorization to submit orders or change production data.

## Disposition: keep Draft until evidence gates pass

- The 2026-10-21 16:00 KR log archive and 2026-10-22 Supabase records prove the recurring errors.
- Tests in `tests/kr/test_kr_active_exit_cycle_20261022.py`, `test_kr_oct21_oct22_cumulative_evidence_replay.py` and `test_kr_entry_exit_contract_postgres.py` cover active-cycle selection, 100x partial-SELL accounting, typed cumulative price/time evidence and PostgreSQL SELL economics.
- These tests do **not** prove correct real KIS broker balance, safe cancellation of partially filled CLOSE orders or real-time performance p95.

## Evidence matrix

| Date | Code/owner | KIS requested/cumulative | Stored discrepancy | Expected offline contract |
| --- | --- | --- | --- | --- |
| 10/21 | 251970 PB1 DAY | BUY 17/17; SELL 17/10 | BUY fills 32; SELL fills 0, OPEN 17 | BUY 17 exactly once; SELL 10 economic shares; remaining order 7 **not automatically resold** |
| 10/21 | 086450 PB1 DAY | BUY 48/48; SELL 48/48 | BUY fills 70; historical CLOSED cycle retry caused unique-violation | final CLOSED/48 economic SELL, no second lifecycle INSERT |
| 10/22 | 053800 KR_STANDARD | today SWING BUY 12/12; SELL 12/12 | SELL tied to October 20 CLOSED DAY cycle | never select old contract; block unresolved historic cycle attribution |
| 10/22 | 253590 PB1 DAY | BUY 79/79; SELL 79/52 | SELL fills 0; OPEN 79 | SELL 52 economic shares, remaining 27 plus actual order state unresolved |
| 10/22 | 144960 PB1 DAY | BUY 81/81, 1,137,680 KRW; SELL 81/81 | BUY stored rounded 14,045 × 81 = 1,137,645 | keep broker notional provenance (35 KRW discrepancy), normal full SELL retained |

These source observations are **not** automatically fixed in Supabase by merging code.

## Before merge: required green proof

1. HEAD matches remote `dual-agent` with mergeability and CI green on exact PR head; source and test diffs reviewed.
2. Postgres transaction proof: attempts/order/fills/positions/claims, concurrent worker and crash/restart, mixed individual & cumulative KIS evidence all correct.
3. Reproduce a CLOSE sequence with fully filled 144960 and partially filled 253590; ACK ≠ fill; no resend after ambiguous acknowledgement.
4. Prior SHA vs PR SHA: same 30 candidates, broker holdings, open orders, injected slow DB and KIS rate limits; measure p50/p95/max for protective SELL to broker submit **≤60s** and full tick **≤180s**. Do not reduce risk gates to meet deadline.
5. Ensure KR_INFINITE 122630 sleeve, KOSPI/KOSDAQ stop caps, DAY/EOD, SWING carry, entry contract SHA and same-day re-entry rules unchanged.
6. Read-only SQL `scripts/kr/audit_kr_oct21_oct22_broker_truth_readonly.sql` produces reconciled exception list.

## Separately authorized data correction (NOT included in PR)

1. Obtain fresh read-only KIS execution/order/cancel records and current balances, scoped by account, trade date, broker ODNO, position-cycle and original entry contract.
2. Snapshot target Supabase epoch's `orders`, `fills`, `positions`, `execution_claims`, `execution_attempts`, `ledger_events`; record count and digest.
3. Generate dry-run per-order delta (buy order-cumulative overcount, SELL partial unapplied, historical wrong-cycle 053800, erroneous fill timestamp) and compare with authoritative KIS.
4. Obtain explicit approval for an isolated data migration/transaction; never mass-delete fill rows or invent replacement executions.
5. Compare post-change broker order cumulative qty/notional with economic-applied watermark, positions, PnL and same-day restrictions; rollback on any mismatch.
6. Separate another explicit permission for any KIS practice test order or deployment/scheduler/settlement cutover.

## Rollback / emergency guard

- Keep existing Draft code unmerged until all checks pass.
- If post-merge position binding cannot be proved, disable new affected-code submissions via the existing durable unresolved-order/owner gates; do **not** change policy or bypass protection for unrelated positions.
- Restore prior code SHA only after reviewing live KIS order state and preserving execution evidence; rollback code must not erase economic activity already committed.
- Monitor `CYCLE_BINDING_CONFLICT`, `KR_BUY_REPLAY_CLOSED_OR_FOREIGN_CYCLE`, `BROKER_FILL_OBSERVED_LEDGER_UNAPPLIED`, tick timeouts and CLOSE run lineage. 


## 2026-10-22 independent blocker-closure commands (no orders, no DB writes)

The two new files `scripts/kr/pr249_db_snapshot_readonly.sql` and
`scripts/kr/pr249_source_backed_repair_plan.py` produce an exact 10-order
reconciliation **review plan**, not an executable mutation.

Run on the **authorized KIS PRACTICE** environment, after confirming the
account and that all environment variables are already provisioned (never paste
or print tokens, account numbers or database URLs into CI logs):

```bash
# Genuine KIS GET-only evidence for original October 21 and 22 orders.
python -m scripts.kr.pr249_original_kis_readonly > /tmp/pr249-original-kis.json

# Use a pre-configured READ-ONLY psql connection (PGHOST/PGUSER/PGDATABASE)
# to the correct portfolio DB. The SQL is SELECT-only, explicitly scoped to
# 10 orders, one portfolio epoch. Do NOT use an admin or superuser role.
psql -XAt -f scripts/kr/pr249_db_snapshot_readonly.sql > /tmp/pr249-db.json

# Computes dry-run quantity/notional and wrong-cycle exceptions.
python -m scripts.kr.pr249_source_backed_repair_plan \
  --kis /tmp/pr249-original-kis.json \
  --db /tmp/pr249-db.json \
  --out /tmp/pr249-repair-review.json
```

If any original KIS page, order, amount, account scope, or DB epoch conflicts,
**STOP**. The comparison utility only checks evidence against the expected
account-scoped snapshot format. A JSON field claiming KIS source provenance
is **not a cryptographic proof**. Retain full evidence and independently
check original account identity, authorization and source log chain before
any production historical adjustment.

For the actual latency gate, collect complete baseline and unchanged-workload
candidate KR tick logs; include failures and missing broker ACK. Use
`scripts/kr/pr249_latency_log_export.py` for each actual log set and then:

```bash
python -m scripts.kr.pr249_latency_acceptance /tmp/pr249-measured-merged.jsonl \
  --expected-after-sha "$(git rev-parse HEAD)"
```

The PR head must match every candidate `after` tick, and there must be at least
20 measured comparable ticks and 10 broker-ACKed protective SELL measurements
per revision, p95 SELL submit <=60 seconds and complete tick <=180 seconds.
No mock/CI-only benchmark can fulfill this gate. Running this against a
working tree without authorized KIS practice and measured logs cannot
establish MERGE READY.

**Current operational evidence:** DB orders are still inconsistent for
251970 BUY/Sell, 086450 BUY, 253590 SELL, 053800 cycle, and 144960 money;
an authenticated original KIS GET was not executed from this repository PR.
No historical fills/orders/positions/claims were updated.
