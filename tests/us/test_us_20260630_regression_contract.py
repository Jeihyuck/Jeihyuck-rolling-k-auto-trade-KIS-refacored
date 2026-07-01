from trader.us.runner.status_contract import classify_tick_status
from trader.us.runner.daily_report_runner import reconcile_order_sources


def test_inner_ok_not_overridden_by_late_timeout_contract():
    result = {"status": "OK_NO_TRADE", "reason": "", "late_timeout_signal_ignored": 1}
    assert classify_tick_status(result) == "success"


def test_single_tick_timeout_is_warning_not_fatal():
    result = {"status": "WARN_TICK_TIMEOUT", "reason": "tick_timeout", "timeout_sec": 150}
    assert classify_tick_status(result) == "warning"


def test_three_consecutive_tick_timeouts_are_fatal():
    result = {"status": "FAILED_CONSECUTIVE_TICK_TIMEOUT", "reason": "FAILED_CONSECUTIVE_TICK_TIMEOUT"}
    assert classify_tick_status(result) == "fatal"


def test_fills_500_degraded_status_continues_session():
    result = {"status": "DEGRADED_FILLS_UNAVAILABLE", "reason": "TEMP_FILLS_UNAVAILABLE"}
    assert classify_tick_status(result) == "success"


def test_daily_report_uses_max_source_and_flags_reconcile_warning():
    rec = reconcile_order_sources(db_orders=10, fills=13, balance_confirmed=0, router_summary=0)
    assert rec["orders_ack"] == 13
    assert "SOURCE_MISMATCH" in rec["warnings"]
    assert "SOURCE_MISMATCH_DB_ORDER_EXISTS_ROUTER_SUMMARY_MISSING" in rec["warnings"]
    assert "SOURCE_MISMATCH_FILLS_EXIST_BALANCE_CONFIRMATION_MISSING" in rec["warnings"]


def test_later_zero_router_summary_does_not_overwrite_am_ack_count():
    rec = reconcile_order_sources(db_orders=10, fills=0, balance_confirmed=0, router_summary=0)
    assert rec["orders_ack"] == 10
    assert "SOURCE_MISMATCH_SESSION_SUMMARY_OVERWRITTEN_OR_MISSING" in rec["warnings"]


def test_full_position_guard_and_liveness_markers_present():
    session_src = open("trader/us/runner/trade_session_runner.py", encoding="utf-8").read()
    tick_src = open("trader/us/runner/trade_tick_runner.py", encoding="utf-8").read()
    assert "SKIPPED_FULL_POSITION" in tick_src
    assert "available_new_slots" in tick_src
    assert "TICK_WARN_TIMEOUT" in session_src
    assert "SESSION_FINALLY" in session_src


def test_sold_today_unconfirmed_design_marker_present():
    # sold_today must be auditable; current contract keeps the report/source fields visible.
    src = open("trader/us/runner/trade_tick_runner.py", encoding="utf-8").read()
    assert "sold_today_symbols" in src
