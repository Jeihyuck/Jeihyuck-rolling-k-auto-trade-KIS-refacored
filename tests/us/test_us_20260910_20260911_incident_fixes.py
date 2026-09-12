from datetime import datetime, timezone

import pytest

from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl
from trader.us.runner.trade_tick_runner import _audit_gate_value, _build_tqqq_ttl_callbacks
from trader.us.execution.order_router import _validate_take_profit_with_fresh_broker_position


NOW = datetime(2026, 9, 11, 15, 0, tzinfo=timezone.utc)


class Repo:
    def __init__(self, order):
        self.order = order
        self.cancelled = []
        self.terminal = []

    def load_expired_open_buy_orders(self, **_kwargs):
        return [self.order]

    def mark_ttl_cancel_requested(self, order, **kwargs):
        self.cancelled.append((order, kwargs))
        order.setdefault("meta", {})["tqqq_ttl_cancel_requested_at"] = kwargs["requested_at"].isoformat()

    def apply_ttl_terminal_observation(self, order, observation):
        self.terminal.append((order, observation))
        order["status"] = observation["status"]
        return {"status": "OK"}


def stale_order():
    return {
        "trade_date": "2026-09-10",
        "client_order_key": "TQQQ_INF_V3:cycle-live:2026-09-10:BUY",
        "order_no": "old-18",
        "symbol": "TQQQ",
        "side": "BUY",
        "qty_requested": 3,
        "status": "OPEN",
        "meta": {},
    }


def test_tqqq_terminal_broker_truth_is_applied_before_cancel():
    repo = Repo(stale_order())
    cancel_calls = []
    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=lambda **kw: cancel_calls.append(kw) or {"status": "ACK"},
        query_order=lambda **_kw: {
            "order_no": "old-18", "symbol": "TQQQ", "side": "BUY",
            "status": "FILLED", "filled_qty": 3,
        },
    )
    assert result["terminal"] == 1
    assert cancel_calls == []
    assert repo.order["status"] == "FILLED"


def test_tqqq_original_order_not_found_isolated_and_requeried():
    repo = Repo(stale_order())
    queries = iter([
        {"status": "OPEN", "order_no": "old-18", "symbol": "TQQQ", "side": "BUY"},
        {"status": "EXPIRED", "order_no": "old-18", "symbol": "TQQQ", "side": "BUY", "filled_qty": 0},
    ])

    def cancel(**_kw):
        raise RuntimeError("모의투자 원주문번호가 존재하지 않습니다")

    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=cancel, query_order=lambda **_kw: next(queries),
    )
    assert result["terminal"] == 1
    assert repo.order["status"] == "EXPIRED"


def test_tqqq_unresolved_cancel_error_keeps_pending_without_exception():
    repo = Repo(stale_order())

    def cancel(**_kw):
        raise RuntimeError("모의투자 원주문번호가 존재하지 않습니다")

    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=cancel, query_order=lambda **_kw: {},
    )
    assert result["pending"] == 1
    assert result["terminal"] == 0
    assert repo.order["status"] == "OPEN"


def test_tqqq_callback_uses_original_trade_date_from_client_key():
    seen = []

    class Provider:
        def get_fills_by_order_no(self, **kwargs):
            seen.append(kwargs)
            return {}

    class Client:
        def cancel_us_order(self, **_kwargs):
            return {"status": "ACK"}

    _cancel, query = _build_tqqq_ttl_callbacks(
        Provider(), Client(), trade_date="2026-09-11", symbol="TQQQ"
    )
    query(
        order_no="old-18",
        client_order_key="TQQQ_INF_V3:cycle-live:2026-09-10:BUY",
        symbol="TQQQ", side="BUY",
    )
    assert seen[0]["trade_date"] == "2026-09-10"


@pytest.mark.parametrize(
    "symbol,avg_price,executable,expected_min",
    [
        ("AAPL", 318.93, 332.4501, 0.04),
        ("MRVL", 230.49, 237.665, 0.03),
    ],
)
def test_take_profit_refresh_uses_fresh_kis_avg_and_allows_valid_tp(symbol, avg_price, executable, expected_min):
    intent = {
        "symbol": symbol,
        "side": "SELL",
        "qty": 2,
        "limit_price": executable,
        "reason": "TAKE_PROFIT_TP1",
        "position_lifecycle_id": f"lc-{symbol}",
        "meta": {
            "reason": "TAKE_PROFIT_TP1",
            "profit_capture_stage": "tp1",
            "tp_threshold_fraction": 0.03,
            "position_lifecycle_id": f"lc-{symbol}",
            "broker_avg_price": avg_price,
            "broker_avg_price_source": "fallback_avg_cost",
            "broker_avg_price_currency": "USD",
            "broker_avg_price_asof": "2026-09-10T14:00:00+00:00",
        },
    }
    broker = {
        "symbol": symbol,
        "qty": 10,
        "orderable_qty": 10,
        "avg_price_usd": avg_price,
        "balance_source": "kis_balance_authoritative",
    }
    result = _validate_take_profit_with_fresh_broker_position(intent, broker, now=NOW)
    assert result["ok"] is True
    assert result["return_rate"] >= expected_min
    assert intent["meta"]["take_profit_guard_source"] == "fresh_kis_balance"
    assert intent["meta"]["broker_avg_price_source"] == "kis_pchs_avg_pric"


def test_take_profit_refresh_reblocks_when_threshold_no_longer_met():
    intent = {
        "symbol": "MRVL", "side": "SELL", "qty": 3,
        "limit_price": 237.665, "reason": "TAKE_PROFIT_TP1",
        "position_lifecycle_id": "lc-mrvl",
        "meta": {"reason": "TAKE_PROFIT_TP1", "tp_threshold_fraction": 0.03, "position_lifecycle_id": "lc-mrvl"},
    }
    broker = {"symbol": "MRVL", "qty": 15, "orderable_qty": 15, "avg_price_usd": 236.50}
    result = _validate_take_profit_with_fresh_broker_position(intent, broker, now=NOW)
    assert result["ok"] is False
    assert result["reason"] == "threshold_not_met_after_refresh"


def test_take_profit_refresh_fails_closed_without_broker_position():
    intent = {
        "symbol": "AAPL", "side": "SELL", "qty": 2,
        "limit_price": 332.4501, "reason": "TAKE_PROFIT_TP1",
        "position_lifecycle_id": "lc-aapl",
        "meta": {"reason": "TAKE_PROFIT_TP1", "tp_threshold_fraction": 0.03, "position_lifecycle_id": "lc-aapl"},
    }
    result = _validate_take_profit_with_fresh_broker_position(intent, {}, now=NOW)
    assert result == {"ok": False, "reason": "fresh_broker_position_unavailable"}


def test_buy_audit_missing_is_na_not_false_zero():
    assert _audit_gate_value({}, "setup_ok", "setup_passed") == "NA"
    assert _audit_gate_value({"risk_ok": False}, "risk_ok", "risk_passed") == 0
    assert _audit_gate_value({"meta": {"sizing_ok": True}}, "sizing_ok", "sizing_passed") == 1
