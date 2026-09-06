# US Daily Report — 2026-07-16

> **KIS_ENV: PRACTICE**
> 모의투자 주문이며 실계좌/MTS에는 표시되지 않음

**Session**: CLOSE

## Runtime Metadata

| Field | Value |
|---|---|
| trade_date | 2026-07-16 |
| branch | copilot/refactordual-agent-modularization-gpt54mini |
| commit_sha | 55b5f6a7d0455297130dd39a84b51cecfa8a3c18 |
| workflow | dynamic/copilot-swe-agent/copilot |
| run_id | 34026600948 |
| session | close |
| env | practice |
| KIS_ENV | practice |
| environment_notice | 모의투자 주문이며 실계좌/MTS에는 표시되지 않음 |
| dry_run | True |
| force_now | N/A |

## Trading Summary

| Metric | Value |
|---|---|
| report_status | FAILED_RECONCILE |
| market_state | UNKNOWN |
| defense_regime | NONE |
| risk_on_regime | NONE |
| market_state_reasons | [] |
| exposure_multiplier | 1.0 |
| effective_budget_before_overlay | 0.0 |
| effective_budget_after_overlay | 0.0 |
| 신규매수 허용/차단 | UNKNOWN / blocked=0 |
| 방어 trim | count=0 notional=0.0 |
| 부분익절 | count=0 notional=0.0 |
| trailing_stop_mode | normal |
| account_loss_kill_switch_triggered | False |
| forbidden_hedge_block_count | 0 |
| report_consistency | REPORT_INCONSISTENT_POSITION_VALUE |
| canonical_order_source | router_session_summary |
| canonical_position_source | kis_final_balance |
| orders_submitted_total | 0 |
| session_orders_submitted | 0 |
| daily_orders_submitted_total | 0 |
| orders_ack_total | 0 |
| orders_rejected_total | 0 |
| orders_unresolved_total | 0 |
| buy_notional_total | 0.0 |
| sell_notional_total | 0.0 |
| session_buy_notional | 0.0 |
| session_sell_notional | 0.0 |
| daily_buy_notional_total | 0.0 |
| daily_sell_notional_total | 0.0 |
| daily_fills_total | 0 |
| actual_new_positions | 0 |
| open_position_count | 1 |
| account_equity_krw | 49999999.99999999 |
| account_equity_usd | 0.0 |
| invested_market_value_usd | 0.0 |
| cash_usd | 34482.75862068965 |
| gross_exposure_pct | 0.0 |
| target_exposure_pct | 0.7 |
| max_exposure_pct | 0.85 |
| min_cash_buffer_pct | 0.15 |
| deployment_gap_usd | 24137.931034482754 |
| deployable_cash_usd | 29310.344827586203 |
| allowed_new_buy_usd | 24137.931034482754 |
| capital_deployment_action | NEW_AND_ADD_ALLOWED |
| max_positions | 35 |
| available_new_slots | 35 |
| avg_position_value_usd | 0.0 |
| underdeployed | True |
| full_position | False |
| orders_submitted | 0 |
| orders_ack | 0 |
| broker_fill_confirmed | 0 |
| balance_delta_confirmed | 0 |
| ack_only_unresolved | 0 |
| broker_orderable_cash_blocks | 0 |
| broker_orderable_cash_unknown_blocks | 0 |
| broker_orderable_qty_blocks | 0 |
| broker_position_mismatch | 0 |
| cash_exhausted | False |
| orders_dry_run | 0 |
| orders_blocked | 0 |
| orders_rejected | 0 |
| orders_disabled | 0 |
| orders_signal_only | 0 |
| fills | 0 |
| fill_api_count | 0 |
| balance_confirmed_count | 0 |
| pending_order_count | 0 |
| buy_notional_routed | 0.0 |
| sell_notional_routed | 0.0 |
| total_order_notional_routed | 0.0 |
| ack_reconcile_after_route_status |  |
| ack_pending_reconcile_count | 0 |
| positions | 1 |
| rotation_regime | UNKNOWN |
| market_regime | NEUTRAL |
| capital_scale | 1.0 |
| effective_capital_scale | None |
| effective_max_new_positions | None |
| final30_complete | None |
| final30_trade_ready | None |
| underfilled_final30 | None |
| underfilled_tier | None |
| underfilled_capital_haircut | None |
| sector_cap_enforced | False |
| blocked_entry_reason_counts | {} |
| trade_block_reason | ok |
| cap_violations | [] |

## Watchlist & Score Contract

| Metric | Value |
|---|---|
| watchlist_raw_count | 0 |
| watchlist_unique_count | 0 |
| watchlist_duplicate_count | 0 |
| score_nonzero | 0 |
| score_zero | 0 |
| score_missing | 0 |
| score_nonzero_ratio | 0.0000 |
| score_contract_ok | None |

## Prep Status

| Field | Value |
|---|---|
| prep_status | N/A |
| prep_trade_can_proceed | None |
| first_failed_dirty_code | N/A |
| pinned_contract_status | N/A |
| latest_prep_attempt_status | N/A |
| effective_contract_status_used_by_session | N/A |
| entry_block_source | ok |
| entry_block_root_cause | ok |

## Cluster Exposure & Rotation

**rotation_regime**: UNKNOWN

| Cluster | Market Value | Weight | Unrealized PNL | 1D PNL | Cap | Over Cap |
|---|---:|---:|---:|---:|---:|---|

## Warnings

-ORDER_SOURCE_EMPTY

## Errors

- REPORT_INCONSISTENT_POSITION_VALUE
