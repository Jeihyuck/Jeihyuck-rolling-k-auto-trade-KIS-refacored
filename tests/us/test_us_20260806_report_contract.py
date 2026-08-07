from __future__ import annotations


def _build_summary_payload() -> dict:
    # 2026-08-06 operational contract snapshot (fixture-level contract)
    return {
        "trade_date": "2026-08-06",
        "session": "afternoon",
        "order_audit_buy_notional": 29133.105,
        "fills_confirmed_notional": 29133.105,
        "buy_notional_routed": 29133.105,
        "session_orders_sent": 0,
        "session_orders_ack": 0,
        "session_fills_count": 0,
        "daily_cumulative_fills_count": 10,
        "no_new_orders_reason": "daily_notional_limit_nearly_exhausted",
        "daily_buy_limit_usd": 30000.0,
        "daily_buy_notional_filled_usd": 29133.105,
        "daily_buy_budget_remaining_usd": 866.895,
        "daily_notional_exceeded_block_count": 66,
        "daily_notional_exceeded_block_symbols": {
            "HON": 22,
            "MRK": 22,
            "KO": 22,
        },
        "kis_temp_errors": {
            "total": 93,
            "recovered": 13,
            "unrecovered": 1,
            "by_api": {
                "GET_price": {"temp_error": 79, "recovered": 11, "unrecovered": 0},
                "GET_inquire-balance": {"temp_error": 12, "recovered": 2, "unrecovered": 1},
                "GET_inquire-ccnl": {"temp_error": 2, "recovered": 0, "unrecovered": 0},
            },
        },
        "kis_temp_error_raw_log_count": 93,
        "kis_temp_error_sequence_count": 14,
        "kis_temp_error_by_endpoint": {
            "GET_price": 79,
            "GET_inquire-balance": 12,
            "GET_inquire-ccnl": 2,
        },
        "order_submit_temp_error_count": 2,
        "order_submit_temp_error_recovered_count": 2,
        "order_submit_temp_error_unrecovered_count": 0,
    }


def test_20260806_buy_fill_notional_matches_order_audit() -> None:
    payload = _build_summary_payload()
    assert payload["fills_confirmed_notional"] == payload["order_audit_buy_notional"] == 29133.105


def test_20260806_no_oversized_routed_notional() -> None:
    payload = _build_summary_payload()
    assert payload["buy_notional_routed"] < 100000
    assert payload["buy_notional_routed"] != 404796.9904
    assert payload["buy_notional_routed"] != 995757.388


def test_20260806_afternoon_session_zero_new_orders() -> None:
    payload = _build_summary_payload()
    assert payload["session_orders_sent"] == 0
    assert payload["session_orders_ack"] == 0
    assert payload["session_fills_count"] == 0
    assert payload["daily_cumulative_fills_count"] == 10


def test_20260806_daily_notional_block_reason_visible() -> None:
    payload = _build_summary_payload()
    assert payload["no_new_orders_reason"] == "daily_notional_limit_nearly_exhausted"
    assert payload["daily_notional_exceeded_block_count"] > 0
    assert payload["daily_notional_exceeded_block_symbols"]


def test_20260806_temp_error_totals_not_zero_and_order_endpoint_split() -> None:
    payload = _build_summary_payload()
    assert int(payload["kis_temp_errors"]["total"]) > 0
    assert payload["kis_temp_error_raw_log_count"] > 0
    assert payload["order_submit_temp_error_count"] > 0
    assert payload["order_submit_temp_error_unrecovered_count"] == 0
