from decimal import Decimal

import trader.us.db.repos as repos
from trader.us.data_provider import normalize_us_order_status_row
from trader.us.market_state_overlay import build_profit_capture_intents
from trader.us.profit_capture import calc_return_rate
from trader.us.runner.status_contract import classify_tick_status
from trader.us.utils.order_no import canonical_order_no


OVERLAY = {"market_state": "NORMAL", "profit_capture_enabled": True}


def _position(symbol, avg, price, qty=10):
    return {"symbol": symbol, "qty": qty, "avg_price_usd": avg, "current_price_usd": price}


def test_decimal_return_and_loss_tp_block():
    assert calc_return_rate(Decimal("353.690"), Decimal("354.920")) < 0
    assert build_profit_capture_intents([_position("JPM", "354.920", "353.690")], OVERLAY, profit_capture_state={}) == []
    assert build_profit_capture_intents([_position("BAC", "62.410", "62.240")], OVERLAY, profit_capture_state={}) == []


def test_below_three_percent_is_blocked_and_valid_tp1_is_single():
    assert build_profit_capture_intents([_position("V", "369.435", "371.270")], OVERLAY, profit_capture_state={}) == []
    state = {}
    first = build_profit_capture_intents([_position("OK", 100, 103)], OVERLAY, trade_date="2026-08-04", profit_capture_state=state)
    second = build_profit_capture_intents([_position("OK", 100, 103)], OVERLAY, trade_date="2026-08-04", profit_capture_state=state)
    assert len(first) == 1 and second == []
    assert first[0]["meta"]["return_rate_at_decision"] == "0.03"


def test_tp2_requires_confirmed_tp1_fill():
    ack = {"X": {"tp1_pending": True, "tp1_done": False}}
    assert build_profit_capture_intents([_position("X", 100, 106)], OVERLAY, profit_capture_state=ack) == []
    filled = {"X": {"tp1_pending": False, "tp1_done": True}}
    result = build_profit_capture_intents([_position("X", 100, 106)], OVERLAY, profit_capture_state=filled)
    assert result[0]["reason"] == "TAKE_PROFIT_TP2"


def test_pending_reconcile_is_degraded_not_fatal():
    result = {"status": "OK_RECONCILE_ONLY_PENDING", "severity": "DEGRADED", "allow_new_orders": False, "session_should_continue": True}
    assert classify_tick_status(result) == "warning"
    assert all(classify_tick_status(result) != "fatal" for _ in range(10))


def test_order_number_and_open_partial_rows_are_preserved():
    assert canonical_order_no("0000040991") == canonical_order_no("40991") == "40991"
    opened = normalize_us_order_status_row({"odno": "0000040991", "pdno": "JPM", "sll_buy_dvsn_cd": "01", "ft_ord_qty": "2", "ft_ccld_qty": "0", "nccs_qty": "2"})
    assert opened["status"] == "OPEN" and opened["raw_order_no"] == "0000040991"
    assert opened["canonical_order_no"] == "40991" and opened["remaining_qty"] == 2
    partial = normalize_us_order_status_row({"odno": "2", "pdno": "JPM", "ft_ord_qty": "5", "ft_ccld_qty": "2", "nccs_qty": "3"})
    assert partial["status"] == "PARTIALLY_FILLED" and partial["remaining_qty"] == 3


def test_seven_valid_raw_rows_all_normalize():
    rows = [{"odno": f"000{i}", "pdno": "JPM", "ft_ord_qty": "2", "ft_ccld_qty": "0", "nccs_qty": "2"} for i in range(1, 8)]
    normalized = [normalize_us_order_status_row(row) for row in rows]
    assert len(normalized) == 7 and all(row["status"] == "OPEN" for row in normalized)


def test_profit_capture_state_ack_is_pending_until_fill(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    repos.mark_us_profit_capture_stage("2026-08-04", "JPM", "tp1", status="ACK")
    state = repos.load_us_profit_capture_state("2026-08-04", ["JPM"])["JPM"]
    assert state["tp1_pending"] and not state["tp1_done"]
    repos.mark_us_profit_capture_stage("2026-08-04", "JPM", "tp1", status="FILLED")
    state = repos.load_us_profit_capture_state("2026-08-04", ["JPM"])["JPM"]
    assert state["tp1_done"] and not state["tp1_pending"]
