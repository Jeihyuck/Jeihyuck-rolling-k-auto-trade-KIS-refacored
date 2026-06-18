from trader.us.runner.status_contract import classify_tick_status, is_no_balance_sell_reject
from trader.us.execution.risk_gate import RiskGateBlocked, assert_order_allowed


def _base_intent(**kw):
    d = {
        "symbol": "AAOI",
        "exchange": "NASDAQ",
        "side": "SELL",
        "qty": 1,
        "available_qty": 1,
        "notional_usd": 20.0,
        "client_order_key": "AAOI-20260618-SELL-hard_stop",
    }
    d.update(kw)
    return d


def test_ok_exit_orders_sent_is_success():
    assert classify_tick_status({"status": "OK_EXIT_ORDERS_SENT"}) == "success"


def test_no_balance_exit_rejected_is_warning_not_fatal():
    result = {
        "status": "FAILED_ALL_EXIT_ORDERS_REJECTED",
        "reason": "모의투자 잔고내역이 없습니다",
    }
    assert is_no_balance_sell_reject(result["reason"])
    assert classify_tick_status(result) == "warning"


def test_sell_duplicate_client_order_key_blocked(monkeypatch):
    monkeypatch.setenv("US_ORDER_ACCEPTED_IS_NOT_FILLED", "0")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
    monkeypatch.delenv("US_ORDER_ARMED", raising=False)
    with pytest_raises_risk_duplicate():
        assert_order_allowed(
            _base_intent(),
            existing_order_keys={"AAOI-20260618-SELL-hard_stop"},
            allowed_symbols={"AAOI"},
            current_position_symbols={"AAOI"},
        )


class pytest_raises_risk_duplicate:
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        assert exc_type is RiskGateBlocked
        assert "client_order_key" in str(exc) and "already exists" in str(exc)
        return True
