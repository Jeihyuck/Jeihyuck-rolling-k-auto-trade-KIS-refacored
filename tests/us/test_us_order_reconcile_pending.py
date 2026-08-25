from trader.us.execution.reconcile import classify_ack_orders_with_final_balance
from trader.us.runner.daily_report_runner import classify_close_order_reconcile_summary
from trader.us.runner.trade_tick_runner import classify_ack_reconcile_gate


def test_close_classifies_reserved_ack_as_open_pending_not_unresolved():
    order = {"symbol": "NVDA", "side": "SELL", "order_no": "N1", "client_order_key": "K1",
             "qty_requested": 1, "pre_order_position_qty": 1, "status": "ACK"}

    class Provider:
        def get_balance(self):
            return {"positions": [{"symbol": "NVDA", "qty": 1, "orderable_qty": 0, "avg_price": 170}]}

    result = classify_ack_orders_with_final_balance(provider=Provider(), trade_date="2026-08-24", orders=[order])
    assert result["status"] == "WARNING_OPEN_ORDER_PENDING"
    assert result["open_order_pending_count"] == 1
    assert result["unresolved_error_count"] == result["pending_order_count"] == 0
    assert result["manual_reconcile_required"] == 0


def test_close_summary_open_pending_is_consistent_warning_exit_zero():
    result = classify_close_order_reconcile_summary(
        ack_total=7, fill_confirmed_total=6, open_order_pending_total=1,
    )
    assert result["status"] == "WARNING_OPEN_ORDER_PENDING"
    assert result["consistent"] is True
    assert result["exit_code"] == result["manual_reconcile_required"] == 0


def test_close_summary_true_unresolved_remains_failure():
    result = classify_close_order_reconcile_summary(
        ack_total=7, fill_confirmed_total=6, unresolved_error_total=1,
    )
    assert result["status"] == "ERROR_RECONCILE_UNRESOLVED"
    assert result["consistent"] is True
    assert result["manual_reconcile_required"] == result["exit_code"] == 1


def test_tick_gate_blocks_new_orders_but_open_pending_is_not_manual_error():
    result = classify_ack_reconcile_gate({
        "status": "WARN", "pending_count": 1, "open_order_pending_count": 1,
        "unresolved_error_count": 0,
        "symbols_by_status": {"open_order_pending": ["NVDA"], "unresolved_error": []},
    })
    assert result["allow_new_orders"] is False
    assert result["reason"] == "open_order_pending"
    assert result["manual_reconcile_required"] == 0
    assert result["last_open_order_pending_symbols"] == ["NVDA"]


def test_tick_gate_true_unresolved_requires_manual_reconcile():
    result = classify_ack_reconcile_gate({
        "pending_count": 1, "open_order_pending_count": 0, "unresolved_error_count": 1,
        "symbols_by_status": {"open_order_pending": [], "unresolved_error": ["NVDA"]},
    })
    assert result["allow_new_orders"] is False
    assert result["reason"] == "unresolved_ack_error"
    assert result["manual_reconcile_required"] == 1
