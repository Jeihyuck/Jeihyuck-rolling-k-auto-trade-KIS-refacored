# US 2026-08-06 Reprocess Validation

This document records the required acceptance checks for the 2026-08-06 US session report/log fixes.

## Scope
- Candidate / intent / submit / ack / fill metric split
- Session vs daily cumulative metric split
- TEMP_ERROR raw vs sequence and endpoint breakdown
- daily_notional_exceeded summary clarity

## Expected values (2026-08-06)
- actual_buy_fill_notional_usd: 29133.105
- AM buy_notional_routed: must not be massively over-counted from repeated candidates
- Afternoon buy_notional_routed: must not be massively over-counted from repeated candidates
- Afternoon session_orders_sent: 0
- Afternoon session_fills_count: 0
- Afternoon daily_cumulative_fills_count: 10
- daily_notional_exceeded summary: must include reason and blocked symbols
- kis_temp_errors.total: must be non-zero when raw TEMP_ERROR logs exist
- order_submit_temp_error_count: separate recovered/unrecovered counts must be present

## Fixture-driven verification command
Use the US report/session fixtures to run verification checks:

```bash
python3 -m pytest -q \
  tests/us/test_us_daily_report.py \
  tests/us/test_us_daily_report_reconcile_counts.py \
  tests/us/test_us_trade_close_workflow_log_contract.py \
  tests/us/test_us_risk_gate.py \
  tests/us/test_us_risk_gate_side_split.py
```

## Full US regression command
```bash
python3 -m pytest -q tests/us
```

## Harness verification command
```bash
python3 -m trader.us.harness.runner --scenario all --offline
```

## Latest run evidence (this change set)
- Full US test suite: 1464 passed, 7 skipped
- US offline harness scenarios: 7/7 passed

Note: Historical runtime report artifacts under reports/us_daily/** are intentionally excluded from the code commit.
